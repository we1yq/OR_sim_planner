from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import heapq

from ...physical_ids import get_physical_id
from ...state import gpu_map_by_id
from . import action_builder
from .dag_format import build_phased_action_plan


def bind_physical_lifetimes(actions, plan_items, source_state):
    """Bind acquisition lifetimes, not advance reservations, to physical GPUs."""
    dag = build_phased_action_plan(actions, name="physical-lifetime-input")
    if any(p.get("warning") for p in dag["phases"]):
        raise RuntimeError("cannot bind physical lifetimes of a cyclic DAG")
    nodes = {n["id"]: n for n in dag["nodes"]}
    remaining = {k: len(n["dependsOn"]) for k, n in nodes.items()}
    children = defaultdict(list)
    for key, node in nodes.items():
        for dep in node["dependsOn"]:
            children[dep].append(key)
    active_src = {g: gpu for g, gpu in gpu_map_by_id(source_state).items()
                  if not action_builder._is_available_physical_gpu(gpu)}
    owners = {str(get_physical_id(source_state, g)): str(get_physical_id(source_state, g))
              for g in active_src if get_physical_id(source_state, g) is not None}
    bindings = dict(owners)
    free = list(dict.fromkeys(reversed(action_builder._build_initial_available_pool(source_state, active_src))))
    release_keys = {}
    root_bindings = defaultdict(dict)
    ready = []
    def push(key):
        node = nodes[key]
        acquire = node["action"].get("type") == "allocate_gpu"
        heapq.heappush(ready, (int(acquire), node["index"], key))
    for key, count in remaining.items():
        if count == 0:
            push(key)
    result = []
    while ready:
        _, _, key = heapq.heappop(ready)
        node = nodes[key]
        action = deepcopy(actions[node["index"]])
        old = action.get("physical_gpu_id")
        old = str(old) if old is not None else None
        kind = action.get("type")
        deps = set(action.get("dependsOnActionKeys") or [])
        # Preserve every inferred edge before reordering and changing device IDs.
        deps.update(nodes[d]["action"]["actionKey"] for d in node["dependsOn"])
        if old is not None:
            if kind == "allocate_gpu":
                if old in bindings:
                    raise RuntimeError(f"overlapping planned physical lifetime: {old}")
                if not free:
                    ready_actions = [
                        {
                            "actionKey": nodes[ready_key]["action"].get("actionKey"),
                            "type": nodes[ready_key]["action"].get("type"),
                            "physicalGpuId": nodes[ready_key]["action"].get("physical_gpu_id"),
                        }
                        for _, _, ready_key in sorted(ready)
                    ]
                    raise RuntimeError(
                        "no physical GPU available for ready acquisition: "
                        f"actionKey={action.get('actionKey')!r}, requested={old!r}, "
                        f"bindings={bindings!r}, owners={owners!r}, "
                        f"releaseKeys={release_keys!r}, ready={ready_actions!r}"
                    )
                actual = free.pop(0)
                bindings[old] = actual
                owners[actual] = old
                if actual in release_keys:
                    deps.add(release_keys[actual])
                    action["physicalGpuEffect"] = {
                        **action.get("physicalGpuEffect", {}),
                        "reuseDependencyActionKey": release_keys[actual],
                    }
            else:
                if old not in bindings:
                    raise RuntimeError(f"action outside physical lifetime: {old}, {kind}")
                actual = bindings[old]
            action["physical_gpu_id"] = actual
            if action.get("physicalGpuEffect"):
                action["physicalGpuEffect"]["physicalGpuId"] = actual
            root_bindings[str(action.get("abstractRoot") or "")][old] = actual
            # Explicit cross-device route references follow the currently live binding.
            if action.get("target_physical_gpu_id") is not None:
                dest = str(action["target_physical_gpu_id"])
                action["target_physical_gpu_id"] = bindings.get(dest, dest)
            if kind == "return_gpu":
                if owners.pop(actual, None) != old:
                    raise RuntimeError(f"release does not own physical GPU: {actual}")
                bindings.pop(old)
                release_keys[actual] = action["actionKey"]
                # Prefer released devices over extending the active device pool.
                free.insert(0, actual)
        if deps:
            action["dependsOnActionKeys"] = sorted(deps)
        result.append(action)
        for child in children[key]:
            remaining[child] -= 1
            if remaining[child] == 0:
                push(child)
    if len(result) != len(actions):
        raise RuntimeError("physical lifetime scheduler did not finish")
    for item in plan_items:
        mapping = root_bindings.get(str(item.get("id")), {})
        for field in ("physical_gpu_id", "target_physical_gpu_id"):
            if item.get(field) is not None:
                item[field] = mapping.get(str(item[field]), item[field])
    return result
