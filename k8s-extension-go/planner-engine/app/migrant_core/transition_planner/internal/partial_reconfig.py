from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...state import GPUState, MigInstance
from ...target_materializer.templates import all_unique_physical_realizations, template_name_list


Slot = tuple[int, int, str]


@dataclass(frozen=True)
class PartialReconfigPlan:
    source_template: str
    target_template: str
    preserve_slots: tuple[Slot, ...]
    delete_slots: tuple[Slot, ...]
    create_slots: tuple[Slot, ...]

    def to_action_fields(self) -> dict[str, Any]:
        return {
            "sourceTemplate": self.source_template,
            "targetTemplate": self.target_template,
            "preserveSlots": [list(slot) for slot in self.preserve_slots],
            "deleteSlots": [list(slot) for slot in self.delete_slots],
            "createSlots": [list(slot) for slot in self.create_slots],
            "deleteSpec": agent_slot_spec(self.delete_slots),
            "createSpec": agent_slot_spec(self.create_slots),
            "preserveSpec": agent_slot_spec(self.preserve_slots),
        }


def build_partial_reconfig_plan(src_gpu: GPUState, tgt_gpu: GPUState) -> PartialReconfigPlan | None:
    """Return a concrete slot patch when target geometry can be reached locally.

    The rule intentionally works on physical slots, not only abstract template
    names. A patch is partial only when at least one existing MIG instance can
    remain in place and every new target slot is fully contained in space freed
    by deleted source slots.
    """

    src_slots = _gpu_slots(src_gpu)
    tgt_slots = _gpu_slots(tgt_gpu)
    preserve = tuple(slot for slot in src_slots if slot in set(tgt_slots))
    if not preserve:
        return None

    delete = tuple(slot for slot in src_slots if slot not in set(preserve))
    create = tuple(slot for slot in tgt_slots if slot not in set(preserve))
    if not delete and not create:
        return None
    if any(not _slot_covered_by_union(slot, delete) for slot in create):
        return None

    return PartialReconfigPlan(
        source_template=src_gpu.template_str(),
        target_template=tgt_gpu.template_str(),
        preserve_slots=preserve,
        delete_slots=delete,
        create_slots=create,
    )


def partial_reconfig_template_targets() -> dict[str, list[str]]:
    """Enumerate abstract template pairs that have at least one partial layout."""

    out: dict[str, list[str]] = {}
    names = template_name_list()
    for src_name in names:
        targets = []
        for tgt_name in names:
            if src_name == tgt_name:
                continue
            if _templates_have_partial_pair(src_name, tgt_name):
                targets.append(tgt_name)
        out[src_name] = targets
    return out


def agent_slot_spec(slots: tuple[Slot, ...] | list[Slot]) -> str:
    return ",".join(f"{start}:{agent_placement_size(start, end, profile)}:{profile}" for start, end, profile in slots)


def agent_placement_size(start: int, end: int, profile: str) -> int:
    """Return the A100 placement size used by nvidia-smi MIG slot commands."""

    if profile == "7g":
        return 8
    if profile == "3g":
        return 4
    return int(end) - int(start)


def _templates_have_partial_pair(src_name: str, tgt_name: str) -> bool:
    for _, src_intervals in all_unique_physical_realizations(src_name):
        src_gpu = _gpu_from_intervals(src_intervals)
        for _, tgt_intervals in all_unique_physical_realizations(tgt_name):
            tgt_gpu = _gpu_from_intervals(tgt_intervals)
            if build_partial_reconfig_plan(src_gpu, tgt_gpu) is not None:
                return True
    return False


def _gpu_from_intervals(intervals: list[Slot]) -> GPUState:
    return GPUState(
        gpu_id=0,
        instances=[
            MigInstance(start=start, end=end, profile=profile)
            for start, end, profile in intervals
            if profile != "void"
        ],
    )


def _gpu_slots(gpu: GPUState) -> tuple[Slot, ...]:
    return tuple(
        sorted(
            (
                (int(inst.start), int(inst.end), str(inst.profile))
                for inst in gpu.instances
                if inst.profile != "void"
            ),
            key=lambda slot: (slot[0], slot[1], slot[2]),
        )
    )


def _slot_covered_by_union(slot: Slot, covering: tuple[Slot, ...]) -> bool:
    start, end, _ = slot
    for slice_idx in range(start, end):
        if not any(cover_start <= slice_idx < cover_end for cover_start, cover_end, _ in covering):
            return False
    return True
