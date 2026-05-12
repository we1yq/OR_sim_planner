from __future__ import annotations

import re
from typing import Any


PROFILE_SIZE = {
    "1g": 1,
    "1g.5gb": 1,
    "2g": 2,
    "2g.10gb": 2,
    "3g": 3,
    "3g.20gb": 3,
    "4g": 4,
    "4g.20gb": 4,
    "7g": 7,
    "7g.40gb": 7,
}

GPU_OPERATOR_PROFILE = {
    "1g": "1g.5gb",
    "2g": "2g.10gb",
    "3g": "3g.20gb",
    "4g": "4g.20gb",
    "7g": "7g.40gb",
}

OR_SIM_CONFIG_TO_PROFILES = {
    "or-sim-empty": [],
    "or-sim-4-3": ["4g", "3g"],
    "or-sim-4-2-1": ["4g", "2g", "1g"],
    "or-sim-4-1-1-1": ["4g", "1g", "1g", "1g"],
    "or-sim-3-2-1": ["3g", "2g", "1g"],
    "or-sim-3-1-1-1": ["3g", "1g", "1g", "1g"],
    "or-sim-2-2-3": ["2g", "2g", "3g"],
    "or-sim-3-2-1-1": ["3g", "2g", "1g", "1g"],
    "or-sim-3-1-1-1-1": ["3g", "1g", "1g", "1g", "1g"],
    "or-sim-2-2-2-1": ["2g", "2g", "2g", "1g"],
    "or-sim-2-2-1-1-1": ["2g", "2g", "1g", "1g", "1g"],
    "or-sim-2-1-1-1-1-1": ["2g", "1g", "1g", "1g", "1g", "1g"],
}


def logical_mig_slots_from_bindings(
    bindings: dict[str, dict[str, Any]],
    observed_state: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    observed_state = observed_state or {}
    out = []
    for physical_id, binding in sorted(bindings.items()):
        rows = logical_mig_slots_for_binding(
            physical_gpu_id=str(physical_id),
            binding=dict(binding),
            mig_config=_observed_mig_config(observed_state, binding),
            mig_config_state=_observed_mig_config_state(observed_state, binding),
        )
        out.extend(rows)
    return out


def logical_mig_slots_for_binding(
    physical_gpu_id: str,
    binding: dict[str, Any],
    mig_config: str | None = None,
    mig_config_state: str | None = None,
) -> list[dict[str, Any]]:
    mig_devices = [
        dict(item)
        for item in list(binding.get("migDevices", []))
        if item.get("migDeviceUuid")
    ]
    if not mig_devices:
        return []
    intervals = _intervals_for_mig_config(str(mig_config or ""), mig_devices)
    if not intervals:
        intervals = _inferred_intervals_from_devices(mig_devices)

    available_devices = sorted(
        mig_devices,
        key=lambda item: (
            int(item.get("migDeviceIndex", 0)),
            _canonical_profile(str(item.get("profile") or "")),
            str(item.get("migDeviceUuid") or ""),
        ),
    )
    rows = []
    used: set[int] = set()
    for start, end, profile in intervals:
        if profile == "void":
            continue
        profile_gpu_operator = _gpu_operator_profile(profile)
        match_idx = None
        for idx, device in enumerate(available_devices):
            if idx in used:
                continue
            if _canonical_profile(str(device.get("profile") or "")) == profile:
                match_idx = idx
                break
        if match_idx is None:
            continue
        used.add(match_idx)
        device = available_devices[match_idx]
        rows.append(
            {
                "physicalGpuId": physical_gpu_id,
                "nodeName": binding.get("nodeName"),
                "deviceIndex": binding.get("deviceIndex"),
                "gpuUuid": binding.get("gpuUuid"),
                "migConfig": mig_config,
                "migConfigState": mig_config_state,
                "slotStart": int(start),
                "slotEnd": int(end),
                "slot": [int(start), int(end), profile],
                "profile": profile,
                "gpuOperatorProfile": profile_gpu_operator,
                "migDeviceIndex": device.get("migDeviceIndex"),
                "migDeviceUuid": device.get("migDeviceUuid"),
                "bindingSource": binding.get("bindingSource"),
                "confidence": _slot_confidence(str(mig_config or "")),
            }
        )
    return rows


def find_logical_slot(
    logical_slots: list[dict[str, Any]],
    expected: dict[str, Any],
) -> dict[str, Any] | None:
    physical_gpu_id = str(expected.get("physicalGpuId") or expected.get("physical_gpu_id") or "")
    slot = expected.get("slot") or []
    if not physical_gpu_id or not isinstance(slot, list) or len(slot) < 3:
        return None
    start, end, profile = int(slot[0]), int(slot[1]), _canonical_profile(str(slot[2]))
    for row in logical_slots:
        if str(row.get("physicalGpuId") or "") != physical_gpu_id:
            continue
        if int(row.get("slotStart", -1)) != start or int(row.get("slotEnd", -1)) != end:
            continue
        if _canonical_profile(str(row.get("profile") or "")) != profile:
            continue
        return dict(row)
    return None


def expected_placement_from_row(row: dict[str, Any]) -> dict[str, Any] | None:
    expected = row.get("expectedPlacement")
    if isinstance(expected, dict):
        return dict(expected)
    physical_gpu_id = row.get("physical_gpu_id") or row.get("physicalGpuId")
    slot = row.get("slot")
    if physical_gpu_id and isinstance(slot, list) and len(slot) >= 3:
        return {
            "physicalGpuId": str(physical_gpu_id),
            "slot": [int(slot[0]), int(slot[1]), _canonical_profile(str(slot[2]))],
            "profile": _canonical_profile(str(slot[2])),
        }
    return None


def parse_mig_uuids_from_nvidia_smi_l(output: str) -> list[str]:
    return re.findall(r"\b(MIG-[A-Za-z0-9-]+)", str(output or ""))


def _intervals_for_mig_config(
    mig_config: str,
    mig_devices: list[dict[str, Any]],
) -> list[tuple[int, int, str]]:
    if not mig_config or mig_config in {"all-disabled", "or-sim-empty"}:
        return []
    profiles = OR_SIM_CONFIG_TO_PROFILES.get(mig_config)
    if profiles is None:
        profiles = _profiles_from_gpu_operator_all_config(mig_config, mig_devices)
    if profiles is None and mig_config.startswith("or-sim-"):
        profiles = [f"{part}g" for part in mig_config.removeprefix("or-sim-").split("-") if part]
    if profiles is None:
        return []
    return _profiles_to_intervals([_canonical_profile(profile) for profile in profiles])


def _profiles_from_gpu_operator_all_config(
    mig_config: str,
    mig_devices: list[dict[str, Any]],
) -> list[str] | None:
    match = re.match(r"^all-(\dg)(?:\.\d+gb)?$", mig_config)
    if not match:
        return None
    profile = _canonical_profile(match.group(1))
    return [profile for _ in mig_devices if _canonical_profile(str(_.get("profile") or "")) == profile]


def _inferred_intervals_from_devices(mig_devices: list[dict[str, Any]]) -> list[tuple[int, int, str]]:
    profiles = [_canonical_profile(str(item.get("profile") or "")) for item in mig_devices]
    profiles = [profile for profile in profiles if profile in {"1g", "2g", "3g", "4g", "7g"}]
    return _profiles_to_intervals(profiles)


def _profiles_to_intervals(profiles: list[str]) -> list[tuple[int, int, str]]:
    intervals = []
    cur = 0
    for profile in profiles:
        size = int(PROFILE_SIZE[profile])
        intervals.append((cur, cur + size, profile))
        cur += size
    if cur < 7:
        intervals.append((cur, 7, "void"))
    return intervals


def _canonical_profile(value: str) -> str:
    match = re.search(r"([12437]g)", value)
    if not match:
        return value
    return match.group(1)


def _gpu_operator_profile(profile: str) -> str:
    return GPU_OPERATOR_PROFILE.get(_canonical_profile(profile), profile)


def _slot_confidence(mig_config: str) -> str:
    if mig_config in OR_SIM_CONFIG_TO_PROFILES or mig_config.startswith("all-"):
        return "logical-slot-from-mig-config-and-nvidia-smi-order"
    if mig_config.startswith("or-sim-"):
        return "logical-slot-from-or-sim-config-name-and-nvidia-smi-order"
    return "logical-slot-inferred-from-nvidia-smi-order"


def _observed_mig_config(observed_state: dict[str, Any], binding: dict[str, Any]) -> str | None:
    node_name = str(binding.get("nodeName") or "")
    for layout in list(observed_state.get("migLayouts", [])):
        if str(layout.get("nodeName") or "") == node_name:
            value = layout.get("migConfig")
            if value is not None:
                return str(value)
    value = binding.get("migConfig") or binding.get("currentMigConfig")
    return str(value) if value is not None else None


def _observed_mig_config_state(observed_state: dict[str, Any], binding: dict[str, Any]) -> str | None:
    node_name = str(binding.get("nodeName") or "")
    for layout in list(observed_state.get("migLayouts", [])):
        if str(layout.get("nodeName") or "") == node_name:
            value = layout.get("migConfigState")
            if value is not None:
                return str(value)
    value = binding.get("migConfigState") or binding.get("currentMigConfigState")
    return str(value) if value is not None else None
