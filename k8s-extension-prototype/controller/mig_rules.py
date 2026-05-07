from __future__ import annotations

from pathlib import Path
from typing import Any

from io_utils import load_yaml
from models import (
    GpuMigState,
    MigPhysicalRealization,
    MigProfileRule,
    MigRules,
    MigTemplateRule,
    MigTransitionRewriteCandidate,
)


def load_mig_rules(path: str | Path) -> MigRules:
    return mig_rules_from_yaml(load_yaml(path))


def mig_rules_from_yaml(obj: dict[str, Any]) -> MigRules:
    profiles = {
        str(raw["name"]): MigProfileRule(
            name=str(raw["name"]),
            slices=int(raw["slices"]),
            memory_mb=(int(raw["memoryMb"]) if raw.get("memoryMb") is not None else None),
        )
        for raw in obj.get("profiles", [])
    }

    templates = []
    for raw in obj.get("templates", []):
        templates.append(
            MigTemplateRule(
                name=str(raw["name"]),
                capacity={str(k): int(v) for k, v in dict(raw.get("capacity", {})).items()},
                physical_realizations=[
                    MigPhysicalRealization(profiles=[str(p) for p in item.get("profiles", [])])
                    for item in raw.get("physicalRealizations", [])
                ],
                transition_rewrite_candidates=[
                    MigTransitionRewriteCandidate(
                        profiles=[str(p) for p in item.get("profiles", [])],
                        reason=item.get("reason"),
                    )
                    for item in raw.get("transitionRewriteCandidates", [])
                ],
            )
        )

    rules = MigRules(
        gpu_model=str(obj.get("gpuModel", "")),
        slice_count=int(obj["sliceCount"]),
        profiles=profiles,
        templates=templates,
    )
    validate_mig_rules(rules)
    return rules


def validate_mig_rules(rules: MigRules) -> None:
    errors = []
    if rules.slice_count <= 0:
        errors.append("sliceCount must be positive")
    if not rules.profiles:
        errors.append("at least one profile is required")
    if not rules.templates:
        errors.append("at least one template is required")

    for profile in rules.profiles.values():
        if profile.slices <= 0:
            errors.append(f"profile {profile.name}: slices must be positive")
        if profile.slices > rules.slice_count:
            errors.append(f"profile {profile.name}: uses more slices than the GPU")

    seen_template_names = set()
    for template in rules.templates:
        if template.name in seen_template_names:
            errors.append(f"duplicate template name {template.name}")
        seen_template_names.add(template.name)

        for profile_name, count in template.capacity.items():
            if profile_name not in rules.profiles:
                errors.append(f"template {template.name}: unknown capacity profile {profile_name}")
            if count < 0:
                errors.append(f"template {template.name}: negative capacity for {profile_name}")

        capacity_slices = sum(
            rules.profiles[name].slices * count
            for name, count in template.capacity.items()
            if name in rules.profiles
        )
        if capacity_slices > rules.slice_count:
            errors.append(f"template {template.name}: capacity uses {capacity_slices} slices")
        if not template.physical_realizations:
            errors.append(f"template {template.name}: missing physicalRealizations")

        expected_capacity = _expanded_capacity(template.capacity)
        for idx, realization in enumerate(template.physical_realizations):
            _validate_profile_list(
                errors=errors,
                rules=rules,
                owner=f"template {template.name} physicalRealizations[{idx}]",
                profiles=realization.profiles,
            )
            if _profile_counts(realization.profiles) != expected_capacity:
                errors.append(
                    f"template {template.name} physicalRealizations[{idx}]: "
                    "profiles do not match abstract capacity"
                )

        for idx, candidate in enumerate(template.transition_rewrite_candidates):
            _validate_profile_list(
                errors=errors,
                rules=rules,
                owner=f"template {template.name} transitionRewriteCandidates[{idx}]",
                profiles=candidate.profiles,
            )

    if errors:
        raise ValueError("Invalid MIG rules:\n- " + "\n- ".join(errors))


def mig_rules_summary_dict(rules: MigRules) -> dict[str, Any]:
    physical_count = sum(len(t.physical_realizations) for t in rules.templates)
    rewrite_count = sum(len(t.transition_rewrite_candidates) for t in rules.templates)
    return {
        "kind": "MigRulesSummary",
        "gpuModel": rules.gpu_model,
        "sliceCount": rules.slice_count,
        "profileCount": len(rules.profiles),
        "templateCount": len(rules.templates),
        "physicalRealizationCount": physical_count,
        "transitionRewriteCandidateCount": rewrite_count,
    }


def validate_gpu_state_against_mig_rules(state: GpuMigState, rules: MigRules) -> None:
    errors = []
    for gpu in state.gpus:
        occupied = set()
        for idx, instance in enumerate(gpu.instances):
            owner = f"gpu {gpu.gpu_id} instances[{idx}]"
            if instance.start < 0 or instance.end <= instance.start:
                errors.append(f"{owner}: invalid interval [{instance.start}, {instance.end})")
                continue
            if instance.end > rules.slice_count:
                errors.append(f"{owner}: end {instance.end} exceeds sliceCount {rules.slice_count}")
            if instance.profile not in rules.profiles and instance.profile != "void":
                errors.append(f"{owner}: unknown profile {instance.profile}")
            expected_size = rules.profiles[instance.profile].slices if instance.profile in rules.profiles else instance.size
            if instance.profile != "void" and instance.size != expected_size:
                errors.append(
                    f"{owner}: profile {instance.profile} expects {expected_size} slices, "
                    f"got {instance.size}"
                )
            for slice_idx in range(instance.start, min(instance.end, rules.slice_count)):
                if slice_idx in occupied:
                    errors.append(f"{owner}: overlaps slice {slice_idx}")
                occupied.add(slice_idx)

    if errors:
        raise ValueError("Invalid GPU state for MIG rules:\n- " + "\n- ".join(errors))


def _validate_profile_list(
    errors: list[str],
    rules: MigRules,
    owner: str,
    profiles: list[str],
) -> None:
    total = 0
    for profile in profiles:
        rule = rules.profiles.get(profile)
        if rule is None:
            errors.append(f"{owner}: unknown profile {profile}")
            continue
        total += rule.slices
    if total > rules.slice_count:
        errors.append(f"{owner}: uses {total} slices > {rules.slice_count}")


def _expanded_capacity(capacity: dict[str, int]) -> dict[str, int]:
    return {profile: count for profile, count in capacity.items() if count > 0}


def _profile_counts(profiles: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for profile in profiles:
        counts[profile] = counts.get(profile, 0) + 1
    return counts
