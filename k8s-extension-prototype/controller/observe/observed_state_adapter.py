from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

CONTROLLER_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
for root in (CONTROLLER_ROOT, PROJECT_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from migrant_core.state import ClusterState, GPUState, MigInstance, assert_valid_cluster_state


def cluster_state_from_observed_cluster_state(obj: dict[str, Any]) -> ClusterState:
    spec = dict(obj.get("spec", {}))
    observed = dict(spec.get("observedState", {}))
    bindings = {
        str(physical_id): dict(binding)
        for physical_id, binding in dict(observed.get("physicalGpuBindings", {})).items()
    }
    if not bindings:
        bindings = {
            str(binding.get("physicalGpuId")): dict(binding)
            for binding in list(observed.get("physicalGpuBindingList", []))
            if binding.get("physicalGpuId")
        }
    logical_slots_by_physical: dict[str, list[dict[str, Any]]] = {}
    for slot in list(observed.get("logicalMigSlots", [])):
        physical_id = str(slot.get("physicalGpuId") or "")
        if physical_id:
            logical_slots_by_physical.setdefault(physical_id, []).append(dict(slot))
    for physical_id, binding in bindings.items():
        for slot in list(binding.get("logicalMigSlots", [])):
            logical_slots_by_physical.setdefault(physical_id, []).append(dict(slot))

    assignments_by_slot = {}
    for assignment in list(observed.get("podAssignments", [])):
        physical_id = str(assignment.get("physicalGpuId") or "")
        slot = assignment.get("slot") or []
        if physical_id and isinstance(slot, list) and len(slot) >= 3:
            assignments_by_slot[(physical_id, int(slot[0]), int(slot[1]), str(slot[2]))] = dict(assignment)

    physical_ids = sorted(bindings)
    physical_id_map = {idx: physical_id for idx, physical_id in enumerate(physical_ids)}
    gpus = []
    instance_metadata: dict[str, Any] = {}
    for gpu_id, physical_id in physical_id_map.items():
        binding = dict(bindings.get(physical_id, {}))
        raw_slots = sorted(
            logical_slots_by_physical.get(physical_id, []),
            key=lambda row: (int(row.get("slotStart", 0)), int(row.get("slotEnd", 0)), str(row.get("profile", ""))),
        )
        instances = _instances_for_physical_gpu(
            physical_id=physical_id,
            raw_slots=raw_slots,
            assignments_by_slot=assignments_by_slot,
            instance_metadata=instance_metadata,
        )
        gpus.append(GPUState(gpu_id=gpu_id, source="real", instances=instances))

    state = ClusterState(
        gpus=gpus,
        metadata={
            "source": "observed-cluster-state",
            "observedClusterState": dict(obj.get("metadata", {})).get("name"),
            "physical_id_map": physical_id_map,
            "physicalGpuBindings": {
                physical_id: {
                    "nodeName": binding.get("nodeName"),
                    "deviceIndex": binding.get("deviceIndex"),
                    "gpuUuid": binding.get("gpuUuid"),
                    "product": binding.get("product"),
                    "currentMigConfig": binding.get("currentMigConfig") or binding.get("migConfig"),
                    "currentMigConfigState": binding.get("currentMigConfigState") or binding.get("migConfigState"),
                }
                for physical_id, binding in bindings.items()
            },
            "instanceMetadata": instance_metadata,
            "podAssignments": copy.deepcopy(list(observed.get("podAssignments", []))),
        },
    )
    assert_valid_cluster_state(state)
    return state


def _instances_for_physical_gpu(
    physical_id: str,
    raw_slots: list[dict[str, Any]],
    assignments_by_slot: dict[tuple[str, int, int, str], dict[str, Any]],
    instance_metadata: dict[str, Any],
) -> list[MigInstance]:
    if not raw_slots:
        return [MigInstance(start=0, end=7, profile="void")]
    instances = []
    cur = 0
    for slot in raw_slots:
        start = int(slot.get("slotStart", slot.get("slot", [0, 0])[0]))
        end = int(slot.get("slotEnd", slot.get("slot", [0, 0])[1]))
        profile = str(slot.get("profile") or slot.get("slot", [None, None, "void"])[2])
        if start > cur:
            instances.append(MigInstance(start=cur, end=start, profile="void"))
        assignment = assignments_by_slot.get((physical_id, start, end, profile), {})
        inst = MigInstance(
            start=start,
            end=end,
            profile=profile,
            workload=assignment.get("workload"),
            batch=(
                int(assignment["batch"])
                if assignment.get("batch") is not None
                else None
            ),
            mu=float(assignment.get("mu", 0.0) or 0.0),
            preserved=False,
        )
        instances.append(inst)
        key = f"{physical_id}:{start}-{end}:{profile}"
        instance_metadata[key] = {
            "physicalGpuId": physical_id,
            "slot": [start, end, profile],
            "migDeviceUuid": slot.get("migDeviceUuid") or assignment.get("matchedMigDeviceUuid"),
            "gpuUuid": slot.get("gpuUuid"),
            "podName": assignment.get("podName"),
            "namespace": assignment.get("namespace"),
            "endpoint": assignment.get("endpoint"),
            "ready": assignment.get("ready"),
            "acceptingNew": assignment.get("acceptingNew"),
            "inflight": assignment.get("inflight"),
            "queued": assignment.get("queued"),
        }
        cur = end
    if cur < 7:
        instances.append(MigInstance(start=cur, end=7, profile="void"))
    return instances
