from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any


PROFILE_SIZE = {
    "7g": 7,
    "4g": 4,
    "3g": 3,
    "2g": 2,
    "1g": 1,
    "void": 0,
}


@dataclass
class MigInstance:
    start: int
    end: int
    profile: str
    workload: str | None = None
    batch: int | None = None
    mu: float = 0.0
    preserved: bool = False


@dataclass
class GPUState:
    gpu_id: int
    source: str = "real"
    instances: list[MigInstance] = field(default_factory=list)

    def sort_instances(self) -> None:
        self.instances = sorted(self.instances, key=lambda x: (x.start, x.end, x.profile))

    def template_str(self) -> str:
        self.sort_instances()
        return "+".join(str(PROFILE_SIZE[inst.profile]) for inst in self.instances if inst.profile != "void")


@dataclass
class ClusterState:
    gpus: list[GPUState] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def real_gpus(self) -> list[GPUState]:
        return [g for g in self.gpus if getattr(g, "source", "real") == "real"]


def deepcopy_state(state: ClusterState) -> ClusterState:
    return copy.deepcopy(state)


def gpu_map_by_id(state: ClusterState) -> dict[int, GPUState]:
    return {int(g.gpu_id): g for g in state.real_gpus()}


def assert_valid_cluster_state(state: ClusterState, slice_count: int = 7) -> None:
    for gpu in state.real_gpus():
        gpu.sort_instances()
        cur = 0
        for inst in gpu.instances:
            if inst.start != cur:
                raise ValueError(f"GPU {gpu.gpu_id}: expected start={cur}, got {inst.start}")
            if inst.end <= inst.start:
                raise ValueError(f"GPU {gpu.gpu_id}: bad interval ({inst.start},{inst.end})")
            if inst.profile != "void" and PROFILE_SIZE[inst.profile] != (inst.end - inst.start):
                raise ValueError(f"GPU {gpu.gpu_id}: profile-size mismatch on {inst.profile}")
            cur = inst.end
        if cur != slice_count:
            raise ValueError(f"GPU {gpu.gpu_id}: total covered slices={cur}, expected {slice_count}")


def get_gpu_by_id(state: ClusterState, gpu_id: int) -> GPUState | None:
    for gpu in state.real_gpus():
        if int(gpu.gpu_id) == int(gpu_id):
            return gpu
    return None


def get_inst_by_slot(gpu: GPUState | None, slot: tuple[int, int, str]) -> MigInstance | None:
    if gpu is None:
        return None
    start, end, profile = slot
    for inst in gpu.instances:
        if inst.start == start and inst.end == end and inst.profile == profile:
            return inst
    return None


def copy_inst_payload(dst_inst: MigInstance, src_inst: MigInstance | None) -> None:
    if src_inst is None:
        dst_inst.workload = None
        dst_inst.batch = None
        dst_inst.mu = 0.0
        dst_inst.preserved = False
        return
    dst_inst.workload = src_inst.workload
    dst_inst.batch = src_inst.batch
    dst_inst.mu = float(src_inst.mu)
    dst_inst.preserved = bool(getattr(src_inst, "preserved", False))


def replace_or_append_gpu(state: ClusterState, gpu: GPUState) -> None:
    for idx, old_gpu in enumerate(state.gpus):
        if int(old_gpu.gpu_id) == int(gpu.gpu_id):
            state.gpus[idx] = gpu
            return
    state.gpus.append(gpu)

