from __future__ import annotations

from .state import ClusterState, deepcopy_state


PHYSICAL_ID_POOL = [chr(ord("A") + i) for i in range(26)]


def ensure_state_metadata(state: ClusterState) -> None:
    if not hasattr(state, "metadata") or state.metadata is None:
        state.metadata = {}
    state.metadata.setdefault("physical_id_map", {})
    state.metadata.setdefault("next_physical_idx", 0)
    state.metadata.setdefault("free_physical_gpu_pool", [])
    state.metadata.setdefault("free_physical_gpu_pool_policy", "lifo")


def set_physical_id(state: ClusterState, gpu_id: int, physical_id: str) -> None:
    ensure_state_metadata(state)
    pool = state.metadata["free_physical_gpu_pool"]
    while physical_id in pool:
        pool.remove(physical_id)
    state.metadata["physical_id_map"][int(gpu_id)] = physical_id


def get_physical_id(state: ClusterState, gpu_id: int) -> str | None:
    ensure_state_metadata(state)
    return state.metadata["physical_id_map"].get(int(gpu_id))


def alloc_new_physical_id(state: ClusterState) -> str:
    ensure_state_metadata(state)
    idx = int(state.metadata.get("next_physical_idx", 0))
    if idx >= len(PHYSICAL_ID_POOL):
        raise RuntimeError("Out of physical GPU ids (A-Z)")
    physical_id = PHYSICAL_ID_POOL[idx]
    state.metadata["next_physical_idx"] = idx + 1
    return physical_id


def alloc_from_free_pool_or_new(state: ClusterState) -> str:
    ensure_state_metadata(state)
    pool = state.metadata["free_physical_gpu_pool"]
    policy = state.metadata.get("free_physical_gpu_pool_policy", "lifo")
    if pool:
        if policy != "lifo":
            raise ValueError(f"Unsupported physical GPU free-pool policy: {policy}")
        return pool.pop()
    return alloc_new_physical_id(state)


def release_to_free_pool(state: ClusterState, physical_id: str) -> None:
    ensure_state_metadata(state)
    pool = state.metadata["free_physical_gpu_pool"]
    if physical_id not in pool:
        pool.append(physical_id)


def bootstrap_physical_ids_for_state(state: ClusterState) -> None:
    ensure_state_metadata(state)
    for gpu in sorted(state.real_gpus(), key=lambda x: x.gpu_id):
        if get_physical_id(state, gpu.gpu_id) is None:
            set_physical_id(state, gpu.gpu_id, alloc_new_physical_id(state))


def remove_gpu_if_bound_to_physical_id(
    state: ClusterState,
    gpu_id: int,
    physical_id: str,
    release: bool = True,
) -> None:
    ensure_state_metadata(state)
    current_physical_id = get_physical_id(state, gpu_id)
    if current_physical_id != physical_id:
        return
    state.gpus = [gpu for gpu in state.gpus if int(gpu.gpu_id) != int(gpu_id)]
    state.metadata["physical_id_map"].pop(int(gpu_id), None)
    if release:
        release_to_free_pool(state, physical_id)


def canonicalize_state_for_next_round(executed_state: ClusterState) -> ClusterState:
    out = deepcopy_state(executed_state)
    ensure_state_metadata(out)
    old_gpus = sorted(out.real_gpus(), key=lambda x: x.gpu_id)
    old_physical_id_map = dict(out.metadata.get("physical_id_map", {}))
    new_gpus = []
    new_physical_id_map = {}

    for new_id, gpu in enumerate(old_gpus):
        old_id = int(gpu.gpu_id)
        gpu.gpu_id = new_id
        new_gpus.append(gpu)
        if old_id in old_physical_id_map:
            new_physical_id_map[new_id] = old_physical_id_map[old_id]

    out.gpus = new_gpus
    out.metadata["physical_id_map"] = new_physical_id_map
    out.metadata["display_id_map"] = {
        gpu.gpu_id: idx for idx, gpu in enumerate(sorted(out.real_gpus(), key=lambda x: x.gpu_id))
    }
    return out
