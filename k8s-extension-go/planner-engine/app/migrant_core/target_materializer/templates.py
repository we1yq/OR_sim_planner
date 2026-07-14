from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..state import PROFILE_SIZE


PROFILE_ORDER = ["7g", "4g", "3g", "2g", "1g"]
SIZE_TO_PROFILE = {7: "7g", 4: "4g", 3: "3g", 2: "2g", 1: "1g"}


@dataclass(frozen=True)
class PhysicalLayout:
    layout_id: int
    name: str
    profiles: tuple[str, ...]
    intervals: tuple[tuple[int, int, str], ...]
    slots: tuple[tuple[int, int, str], ...]


TEMPLATES = [
    ("7", (1, 0, 0, 0, 0)),
    ("4+3", (0, 1, 1, 0, 0)),
    ("4+2+1", (0, 1, 0, 1, 1)),
    ("4+1+1+1", (0, 1, 0, 0, 3)),
    ("3+3", (0, 0, 2, 0, 0)),
    ("3+2+1", (0, 0, 1, 1, 1)),
    ("3+1+1+1", (0, 0, 1, 0, 3)),
    ("2+2+3", (0, 0, 1, 2, 0)),
    ("3+2+1+1", (0, 0, 1, 1, 2)),
    ("3+1+1+1+1", (0, 0, 1, 0, 4)),
    ("2+2+2+1", (0, 0, 0, 3, 1)),
    ("2+2+1+1+1", (0, 0, 0, 2, 3)),
    ("2+1+1+1+1+1", (0, 0, 0, 1, 5)),
    ("1+1+1+1+1+1+1", (0, 0, 0, 0, 7)),
]


TEMPLATE_K = [
    {
        "7g": vec[0],
        "4g": vec[1],
        "3g": vec[2],
        "2g": vec[3],
        "1g": vec[4],
    }
    for _, vec in TEMPLATES
]


TEMPLATE_NAME_TO_K = {name: dict(TEMPLATE_K[idx]) for idx, (name, _) in enumerate(TEMPLATES)}


ABSTRACT_TO_PHYSICAL = {
    "7": [("7g",)],
    "4+3": [("4g", "3g")],
    "4+2+1": [("4g", "2g", "1g")],
    "4+1+1+1": [("4g", "1g", "1g", "1g")],
    "3+3": [("3g", "unusable", "3g")],
    "3+2+1": [("3g", "unusable", "2g", "1g")],
    "3+1+1+1": [("3g", "unusable", "1g", "1g", "1g")],
    "2+2+3": [("2g", "2g", "3g")],
    "3+2+1+1": [
        ("2g", "1g", "1g", "3g"),
        ("1g", "1g", "2g", "3g"),
    ],
    "3+1+1+1+1": [("1g", "1g", "1g", "1g", "3g")],
    "2+2+2+1": [("2g", "2g", "2g", "1g")],
    "2+2+1+1+1": [
        ("2g", "1g", "1g", "2g", "1g"),
        ("1g", "1g", "2g", "2g", "1g"),
        ("2g", "2g", "1g", "1g", "1g"),
    ],
    "2+1+1+1+1+1": [
        ("2g", "1g", "1g", "1g", "1g", "1g"),
        ("1g", "1g", "2g", "1g", "1g", "1g"),
        ("1g", "1g", "1g", "1g", "2g", "1g"),
    ],
    "1+1+1+1+1+1+1": [("1g", "1g", "1g", "1g", "1g", "1g", "1g")],
}


FRAGMENTATION_AVOIDANCE_REWRITE_CANDIDATES = {
    "3+3": [("4g", "3g")],
    "3+2+1": [
        ("4g", "2g", "1g"),
        ("1g", "1g", "2g", "3g"),
        ("2g", "1g", "1g", "3g"),
    ],
    "3+1+1+1": [
        ("4g", "1g", "1g", "1g"),
        ("1g", "1g", "1g", "1g", "3g"),
    ],
}


def template_name_list() -> list[str]:
    return [name for name, _ in TEMPLATES]


def template_capacity_dict(template_name: str) -> dict[str, int]:
    if template_name not in TEMPLATE_NAME_TO_K:
        raise KeyError(f"Unknown template: {template_name}")
    return dict(TEMPLATE_NAME_TO_K[template_name])


def template_to_parts(template_name: str) -> tuple[int, ...]:
    return tuple(int(x) for x in template_name.split("+"))


def parts_to_profiles(parts: tuple[int, ...]) -> tuple[str, ...]:
    return tuple(SIZE_TO_PROFILE[int(x)] for x in parts)


def physical_profiles_to_string(profiles: tuple[str, ...]) -> str:
    return "+".join(str(PROFILE_SIZE[p]) for p in profiles if p != "unusable")


def physical_profiles_to_intervals(
    profiles: tuple[str, ...],
    slice_count: int = 7,
) -> list[tuple[int, int, str]]:
    out = []
    cur = 0
    for profile in profiles:
        if profile == "unusable":
            out.append((cur, cur + 1, "unusable"))
            cur += 1
            continue
        size = PROFILE_SIZE[profile]
        out.append((cur, cur + size, profile))
        cur += size
    if cur < slice_count:
        out.append((cur, slice_count, "void"))
    return out


def candidate_priority_no_prev(current_template: str, profiles: tuple[str, ...]) -> tuple[int, ...]:
    physical_name = physical_profiles_to_string(profiles)
    if current_template == "3+2+1":
        order = {"1+1+2+3": 0, "2+1+1+3": 1}
        return (order.get(physical_name, 99),)
    if current_template == "3+1+1+1":
        order = {"1+1+1+1+3": 0}
        return (order.get(physical_name, 99),)
    return (99,)


def all_unique_physical_realizations(template_name: str) -> list[tuple[str, list[tuple[int, int, str]]]]:
    if template_name not in ABSTRACT_TO_PHYSICAL:
        raise KeyError(f"Unknown abstract template: {template_name}")
    out = []
    for profiles in ABSTRACT_TO_PHYSICAL[template_name]:
        out.append((physical_profiles_to_string(profiles), physical_profiles_to_intervals(profiles)))
    return out


def physical_layout_key(
    intervals: list[tuple[int, int, str]] | tuple[tuple[int, int, str], ...],
) -> tuple[tuple[int, int, str], ...]:
    return tuple((int(start), int(end), str(profile)) for start, end, profile in intervals)


def current_gpu_physical_layout_key(gpu: Any) -> tuple[tuple[int, int, str], ...]:
    instances = sorted(getattr(gpu, "instances", []), key=lambda inst: (inst.start, inst.end))
    return physical_layout_key(
        [(int(inst.start), int(inst.end), str(inst.profile)) for inst in instances]
    )


def fragment_free_physical_layouts() -> list[PhysicalLayout]:
    records: list[tuple[str, tuple[str, ...]]] = []

    for physical_profiles in ABSTRACT_TO_PHYSICAL.values():
        for profiles in physical_profiles:
            records.append((physical_profiles_to_string(profiles), tuple(profiles)))

    for rewrite_candidates in FRAGMENTATION_AVOIDANCE_REWRITE_CANDIDATES.values():
        for profiles in rewrite_candidates:
            records.append((physical_profiles_to_string(profiles), tuple(profiles)))

    out: list[PhysicalLayout] = []
    seen: set[tuple[tuple[int, int, str], ...]] = set()
    for name, profiles in records:
        if "unusable" in profiles:
            continue
        intervals = physical_layout_key(physical_profiles_to_intervals(profiles))
        if intervals in seen:
            continue
        seen.add(intervals)
        slots = tuple(interval for interval in intervals if interval[2] not in {"void", "unusable"})
        out.append(
            PhysicalLayout(
                layout_id=len(out),
                name=name,
                profiles=profiles,
                intervals=intervals,
                slots=slots,
            )
        )
    return out


def template_summary_dict() -> dict[str, Any]:
    return {
        "profileOrder": list(PROFILE_ORDER),
        "templateCount": len(TEMPLATES),
        "physicalRealizationCount": sum(len(v) for v in ABSTRACT_TO_PHYSICAL.values()),
        "fragmentFreePhysicalLayoutCount": len(fragment_free_physical_layouts()),
        "fragmentationAvoidanceRewriteCandidateCount": sum(
            len(v) for v in FRAGMENTATION_AVOIDANCE_REWRITE_CANDIDATES.values()
        ),
        "templates": [
            {
                "name": name,
                "capacity": template_capacity_dict(name),
                "physicalRealizations": [
                    {"name": physical_name, "intervals": intervals}
                    for physical_name, intervals in all_unique_physical_realizations(name)
                ],
            }
            for name, _ in TEMPLATES
        ],
    }
