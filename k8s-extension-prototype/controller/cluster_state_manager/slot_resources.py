from __future__ import annotations

import re
from typing import Any


OR_SIM_SLOT_RESOURCE_DOMAIN = "or-sim.io"


def slot_resource_name(
    physical_gpu_id: str,
    slot: list[Any] | tuple[Any, ...],
    domain: str = OR_SIM_SLOT_RESOURCE_DOMAIN,
) -> str:
    if len(slot) != 3:
        raise ValueError("slot must be [start, end, profile]")
    start = int(slot[0])
    end = int(slot[1])
    profile = _resource_token(str(slot[2]))
    physical = _resource_token(str(physical_gpu_id))
    if start < 0 or end <= start:
        raise ValueError(f"invalid slot range: {slot}")
    return f"{domain}/{physical}-s{start}-{end}-{profile}"


def slot_resource_name_from_row(row: dict[str, Any]) -> str | None:
    physical_gpu_id = row.get("physical_gpu_id") or row.get("physicalGpuId")
    slot = row.get("slot")
    if not physical_gpu_id or not isinstance(slot, (list, tuple)):
        return None
    return slot_resource_name(str(physical_gpu_id), slot)


def slot_resource_name_from_logical_slot(logical_slot: dict[str, Any]) -> str:
    return slot_resource_name(
        physical_gpu_id=str(logical_slot.get("physicalGpuId") or ""),
        slot=[
            logical_slot.get("slotStart"),
            logical_slot.get("slotEnd"),
            logical_slot.get("profile"),
        ],
    )


def _resource_token(value: str) -> str:
    token = re.sub(r"[^a-z0-9.-]+", "-", value.lower()).strip(".-")
    token = re.sub(r"-+", "-", token)
    if not token:
        raise ValueError("resource token cannot be empty")
    return token[:63].strip(".-") or "slot"
