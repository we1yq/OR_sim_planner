from __future__ import annotations

import itertools
import math
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Iterable


PROFILE_SIZE = {"7g": 7, "4g": 4, "3g": 3, "2g": 2, "1g": 1}
PROFILE_ORDER = ("7g", "4g", "3g", "2g", "1g")


@dataclass(frozen=True)
class ServingOption:
    workload: str
    profile: str
    mu: float
    batch: int | None = None
    family: str | None = None
    metrics: dict = field(default_factory=dict)

    @property
    def size(self) -> int:
        return PROFILE_SIZE[self.profile]


@dataclass(frozen=True)
class Slot:
    start: int
    end: int
    profile: str

    @property
    def size(self) -> int:
        return self.end - self.start


@dataclass(frozen=True)
class WorkloadInstance:
    workload: str
    profile: str
    start: int
    end: int
    mu: float
    batch: int | None = None

    @property
    def size(self) -> int:
        return self.end - self.start


@dataclass(frozen=True)
class GPUAllocation:
    gpu_id: int
    instances: tuple[WorkloadInstance, ...]

    @property
    def used_slices(self) -> int:
        return sum(inst.size for inst in self.instances)


@dataclass(frozen=True)
class AllocationResult:
    method: str
    scenario_id: str
    feasible: bool
    gpus: tuple[GPUAllocation, ...]
    runtime_sec: float
    stage_runtime_sec: dict[str, float] = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    @property
    def gpu_count(self) -> int:
        return len(self.gpus)

    @property
    def allocated_slices(self) -> int:
        return sum(gpu.used_slices for gpu in self.gpus)

    @property
    def slice_utilization(self) -> float:
        if not self.gpus:
            return 0.0
        return self.allocated_slices / (7 * len(self.gpus))

    def to_dict(self) -> dict:
        return {
            "method": self.method,
            "scenario_id": self.scenario_id,
            "feasible": self.feasible,
            "runtime_sec": self.runtime_sec,
            "stage_runtime_sec": dict(self.stage_runtime_sec),
            "metadata": dict(self.metadata),
            "metrics": {
                "gpu_count": self.gpu_count,
                "allocated_slices": self.allocated_slices,
                "slice_utilization": self.slice_utilization,
                "fragmentation": 1.0 - self.slice_utilization if self.gpus else 0.0,
            },
            "gpus": [
                {
                    "gpu_id": gpu.gpu_id,
                    "instances": [
                        {
                            "workload": inst.workload,
                            "profile": inst.profile,
                            "start": inst.start,
                            "end": inst.end,
                            "batch": inst.batch,
                            "mu": inst.mu,
                        }
                        for inst in gpu.instances
                    ],
                }
                for gpu in self.gpus
            ],
        }


@dataclass(frozen=True)
class GPUConfig:
    instances: tuple[WorkloadInstance, ...]
    throughput_by_workload: dict[str, float]

    @property
    def workloads(self) -> set[str]:
        return set(self.throughput_by_workload)

    @property
    def used_slices(self) -> int:
        return sum(inst.size for inst in self.instances)

    def as_gpu(self, gpu_id: int) -> GPUAllocation:
        return GPUAllocation(gpu_id=gpu_id, instances=self.instances)


FULL_PHYSICAL_REALIZATIONS: tuple[tuple[str, ...], ...] = (
    ("7g",),
    ("4g", "3g"),
    ("4g", "2g", "1g"),
    ("4g", "1g", "1g", "1g"),
    ("3g", "unusable", "3g"),
    ("3g", "unusable", "2g", "1g"),
    ("3g", "unusable", "1g", "1g", "1g"),
    ("2g", "2g", "3g"),
    ("2g", "1g", "1g", "3g"),
    ("1g", "1g", "2g", "3g"),
    ("1g", "1g", "1g", "1g", "3g"),
    ("2g", "2g", "2g", "1g"),
    ("2g", "1g", "1g", "2g", "1g"),
    ("1g", "1g", "2g", "2g", "1g"),
    ("2g", "2g", "1g", "1g", "1g"),
    ("2g", "1g", "1g", "1g", "1g", "1g"),
    ("1g", "1g", "2g", "1g", "1g", "1g"),
    ("1g", "1g", "1g", "1g", "2g", "1g"),
    ("1g", "1g", "1g", "1g", "1g", "1g", "1g"),
)


def intervals_for_profiles(profiles: Iterable[str]) -> tuple[Slot, ...]:
    cur = 0
    slots: list[Slot] = []
    for profile in profiles:
        if profile == "unusable":
            cur += 1
            continue
        size = PROFILE_SIZE[profile]
        slots.append(Slot(start=cur, end=cur + size, profile=profile))
        cur += size
    return tuple(slots)


def legal_slot_patterns() -> tuple[tuple[Slot, ...], ...]:
    """All non-empty subsets of supported A100 physical realizations.

    The baseline algorithms allocate one GPU at a time. The papers' GPU
    configurations may leave part of a GPU unused, so we enumerate non-empty
    subsets of the full supported physical realizations and deduplicate them by
    occupied intervals. `unusable` positions model A100 MIG placement holes:
    they consume address space but are never returned as allocatable slots.
    """

    patterns: dict[tuple[tuple[int, int, str], ...], tuple[Slot, ...]] = {}
    for profiles in FULL_PHYSICAL_REALIZATIONS:
        slots = intervals_for_profiles(profiles)
        for mask in range(1, 1 << len(slots)):
            subset = tuple(slot for idx, slot in enumerate(slots) if mask & (1 << idx))
            key = tuple((slot.start, slot.end, slot.profile) for slot in subset)
            patterns[key] = subset
    return tuple(
        sorted(
            patterns.values(),
            key=lambda xs: (
                sum(slot.size for slot in xs),
                len(xs),
                tuple((slot.start, slot.profile) for slot in xs),
            ),
        )
    )


LEGAL_SLOT_PATTERNS = legal_slot_patterns()
_GPU_CONFIG_CACHE: dict[tuple[tuple[tuple[str, str, float, int | None], ...], int], list["GPUConfig"]] = {}


def _options_signature(options: Iterable[ServingOption]) -> tuple[tuple[str, str, float, int | None], ...]:
    return tuple(
        sorted(
            (
                str(option.workload),
                str(option.profile),
                round(float(option.mu), 9),
                option.batch,
            )
            for option in options
        )
    )


def enumerate_gpu_configs_cached(
    options: Iterable[ServingOption],
    max_distinct_workloads: int,
    include_patterns: tuple[tuple[Slot, ...], ...] = LEGAL_SLOT_PATTERNS,
) -> list["GPUConfig"]:
    if include_patterns is not LEGAL_SLOT_PATTERNS:
        return enumerate_gpu_configs(options, max_distinct_workloads=max_distinct_workloads, include_patterns=include_patterns)
    key = (_options_signature(options), int(max_distinct_workloads))
    cached = _GPU_CONFIG_CACHE.get(key)
    if cached is None:
        cached = enumerate_gpu_configs(options, max_distinct_workloads=max_distinct_workloads, include_patterns=include_patterns)
        _GPU_CONFIG_CACHE[key] = cached
    return cached


def prune_dominated_options(options: Iterable[ServingOption]) -> list[ServingOption]:
    best: dict[tuple[str, str], ServingOption] = {}
    for option in options:
        key = (option.workload, option.profile)
        prev = best.get(key)
        if prev is None or option.mu > prev.mu:
            best[key] = option
    return sorted(best.values(), key=lambda opt: (opt.workload, PROFILE_SIZE[opt.profile], -opt.mu))


def serving_options_from_dataframe(feasible_option_df) -> list[ServingOption]:
    out = []
    for _, row in feasible_option_df.iterrows():
        metrics = {
            key: value
            for key, value in dict(row).items()
            if key not in {"opt_idx", "w_idx", "workload", "family", "batch", "profile", "mu"}
        }
        out.append(
            ServingOption(
                workload=str(row["workload"]),
                profile=str(row["profile"]),
                mu=float(row["mu"]),
                batch=int(row["batch"]) if not math.isnan(float(row["batch"])) else None,
                family=str(row["family"]) if row.get("family") is not None else None,
                metrics=metrics,
            )
        )
    return out


def enumerate_gpu_configs(
    options: Iterable[ServingOption],
    max_distinct_workloads: int,
    include_patterns: tuple[tuple[Slot, ...], ...] = LEGAL_SLOT_PATTERNS,
) -> list[GPUConfig]:
    """Enumerate candidate one-GPU configurations.

    This follows the configuration-set idea used by Jormungandr and
    MIG-serving: filter infeasible per-instance options first, then enumerate
    valid one-GPU MIG profile placements and workload assignments. Lower
    throughput options with the same workload/profile are removed because the
    utility functions are monotone in throughput.
    """

    pruned_options = prune_dominated_options(options)
    by_profile: dict[str, list[ServingOption]] = defaultdict(list)
    for option in pruned_options:
        by_profile[option.profile].append(option)

    configs: dict[tuple[tuple[int, int, str, str, int | None], ...], GPUConfig] = {}
    for slots in include_patterns:
        choices = [by_profile.get(slot.profile, []) for slot in slots]
        if any(not items for items in choices):
            continue
        for combo in itertools.product(*choices):
            workloads = {option.workload for option in combo}
            if len(workloads) > max_distinct_workloads:
                continue
            instances = tuple(
                WorkloadInstance(
                    workload=option.workload,
                    profile=slot.profile,
                    start=slot.start,
                    end=slot.end,
                    mu=option.mu,
                    batch=option.batch,
                )
                for slot, option in zip(slots, combo, strict=True)
            )
            throughput = Counter()
            for inst in instances:
                throughput[inst.workload] += float(inst.mu)
            key = tuple(
                sorted(
                    (
                        inst.start,
                        inst.end,
                        inst.profile,
                        inst.workload,
                        inst.batch,
                    )
                    for inst in instances
                )
            )
            configs[key] = GPUConfig(instances=instances, throughput_by_workload=dict(throughput))

    return sorted(
        configs.values(),
        key=lambda conf: (
            -conf.used_slices,
            len(conf.workloads),
            tuple(sorted(conf.throughput_by_workload)),
        ),
    )


def covered_throughput(gpus: Iterable[GPUAllocation]) -> dict[str, float]:
    throughput = Counter()
    for gpu in gpus:
        for inst in gpu.instances:
            throughput[inst.workload] += float(inst.mu)
    return dict(throughput)


def all_demands_satisfied(covered: dict[str, float], demand: dict[str, float]) -> bool:
    return all(float(covered.get(workload, 0.0)) + 1e-9 >= float(required) for workload, required in demand.items())


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.elapsed = time.perf_counter() - self.start
        return False
