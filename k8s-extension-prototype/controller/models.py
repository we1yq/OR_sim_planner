from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil
from typing import Any


PROFILE_SIZE = {
    "7g": 7,
    "4g": 4,
    "3g": 3,
    "2g": 2,
    "1g": 1,
    "void": 0,
}


@dataclass(frozen=True)
class WorkloadRequest:
    name: str
    model: str
    family: str | None
    arrival_rate: float
    allowed_batches: list[int] = field(default_factory=list)
    priority: str = "normal"
    slo: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ProfileOption:
    workload: str
    family: str | None
    batch: int
    profile: str
    mu: float
    fit: bool
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def profile_size(self) -> int:
        return PROFILE_SIZE[self.profile]

    def instances_needed(self, arrival_rate: float) -> int:
        if self.mu <= 0:
            return 0
        return int(ceil(arrival_rate / self.mu))


@dataclass(frozen=True)
class MigInstance:
    start: int
    end: int
    profile: str
    workload: str | None = None
    batch: int | None = None

    @property
    def size(self) -> int:
        return self.end - self.start

    @property
    def is_free(self) -> bool:
        return self.profile == "void" or self.workload is None


@dataclass(frozen=True)
class GpuState:
    gpu_id: int
    source: str
    mig_enabled: bool
    instances: list[MigInstance]

    @property
    def free_slices(self) -> int:
        return sum(inst.size for inst in self.instances if inst.is_free)


@dataclass(frozen=True)
class GpuMigState:
    gpus: list[GpuState]

    @property
    def total_free_slices(self) -> int:
        return sum(gpu.free_slices for gpu in self.gpus)


@dataclass(frozen=True)
class TargetDemand:
    workload: str
    batch: int
    profile: str
    profile_size: int
    instances_needed: int
    total_slices_needed: int
    provided_mu: float
    required_arrival_rate: float
    feasible_on_current_mock_capacity: bool


@dataclass(frozen=True)
class ScenarioWorkloadDemand:
    name: str
    source_arrival: float
    target_arrival: float
    workload_ref: str
    profile_catalog_ref: str

    @property
    def delta(self) -> float:
        return self.target_arrival - self.source_arrival


@dataclass(frozen=True)
class PlanningScenario:
    name: str
    source_state_ref: str
    target_state_ref: str
    workloads: list[ScenarioWorkloadDemand]
    policy_ref: str | None = None
    mig_rules_ref: str | None = None
    description: str | None = None
    transition: dict[str, Any] = field(default_factory=dict)

    @property
    def source_arrival(self) -> dict[str, float]:
        return {w.name: w.source_arrival for w in self.workloads}

    @property
    def target_arrival(self) -> dict[str, float]:
        return {w.name: w.target_arrival for w in self.workloads}


@dataclass(frozen=True)
class ClusterTargetPlan:
    scenario: str
    demands: list[TargetDemand]
    total_slices_needed: int
    min_gpus_needed: int
    max_gpus: int | None = None
    templates: list[str] = field(default_factory=list)
    template_capacity: dict[str, int] = field(default_factory=dict)

    @property
    def feasible_under_policy(self) -> bool | None:
        if self.max_gpus is None:
            return None
        return self.min_gpus_needed <= self.max_gpus


@dataclass(frozen=True)
class MigProfileRule:
    name: str
    slices: int
    memory_mb: int | None = None


@dataclass(frozen=True)
class MigPhysicalRealization:
    profiles: list[str]


@dataclass(frozen=True)
class MigTransitionRewriteCandidate:
    profiles: list[str]
    reason: str | None = None


@dataclass(frozen=True)
class MigTemplateRule:
    name: str
    capacity: dict[str, int]
    physical_realizations: list[MigPhysicalRealization]
    transition_rewrite_candidates: list[MigTransitionRewriteCandidate] = field(default_factory=list)


@dataclass(frozen=True)
class MigRules:
    gpu_model: str
    slice_count: int
    profiles: dict[str, MigProfileRule]
    templates: list[MigTemplateRule]
