from __future__ import annotations

from typing import Any

from actuator import should_actuate_dry_run, validate_and_succeed_dry_run_action_plan
from executor_preview import build_gpu_operator_executor_preview
from reconciler import (
    compact_migplan_status_for_k8s,
    evaluate_auto_approval_policy,
    load_profile_catalogs_for_scenario,
    load_scenario_for_migplan_spec,
    run_watch_controller_loop,
    should_reconcile_migplan,
    upsert_cluster_state_configmap,
    upsert_full_plan_configmap,
    upsert_migactionplan,
)


class FakeKubernetesClient:
    def __init__(self) -> None:
        self.configmaps: dict[tuple[str, str], dict[str, Any]] = {}
        self.statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self.autoapprovalpolicies: dict[tuple[str, str], dict[str, Any]] = {}
        self.migplans: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def get_migplan(self, name: str, namespace: str) -> dict[str, Any]:
        raise NotImplementedError

    def list_migplans(self, namespace: str) -> list[dict[str, Any]]:
        return list(self.migplans)

    def patch_migplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        raise NotImplementedError

    def patch_migactionplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.statuses[(namespace, name)] = status

    def get_autoapprovalpolicy(self, name: str, namespace: str) -> dict[str, Any] | None:
        return self.autoapprovalpolicies.get((namespace, name))

    def get_configmap(self, name: str, namespace: str) -> dict[str, Any]:
        return self.configmaps[(namespace, name)]

    def apply_configmap(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.configmaps[(metadata["namespace"], metadata["name"])] = manifest

    def apply_migactionplan(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.configmaps[(metadata["namespace"], metadata["name"])] = manifest

    def watch_migplans(self, namespace: str, timeout_seconds: int) -> Any:
        yield from self.events


def test_should_reconcile_generation_gap() -> None:
    migplan = {"metadata": {"generation": 2}, "status": {"observedGeneration": 1, "phase": "ReachedTarget"}}
    assert should_reconcile_migplan(migplan)


def test_should_skip_observed_generation() -> None:
    migplan = {"metadata": {"generation": 2}, "status": {"observedGeneration": 2, "phase": "ReachedTarget"}}
    assert not should_reconcile_migplan(migplan)


def test_upsert_cluster_state_configmap() -> None:
    client = FakeKubernetesClient()
    state = {"metadata": {"physical_id_map": {"0": "A"}}, "gpus": []}
    upsert_cluster_state_configmap(
        name="target0-state",
        namespace="or-sim",
        state=state,
        owner_migplan="stage0",
        client=client,
    )

    configmap = client.get_configmap("target0-state", "or-sim")
    assert configmap["metadata"]["labels"]["mig.or-sim.io/state-kind"] == "canonical-next-state"
    assert configmap["metadata"]["labels"]["mig.or-sim.io/owner-migplan"] == "stage0"
    assert "physical_id_map" in configmap["data"]["state.yaml"]


def test_upsert_full_plan_configmap() -> None:
    client = FakeKubernetesClient()
    status = {"phase": "ReachedTarget", "actions": [{"type": "clear_gpu"}]}
    upsert_full_plan_configmap(
        name="stage0-full-plan",
        namespace="or-sim",
        status=status,
        owner_migplan="stage0",
        client=client,
    )

    configmap = client.get_configmap("stage0-full-plan", "or-sim")
    assert configmap["metadata"]["labels"]["mig.or-sim.io/state-kind"] == "full-plan-debug"
    assert configmap["metadata"]["labels"]["mig.or-sim.io/owner-migplan"] == "stage0"
    assert "clear_gpu" in configmap["data"]["status.yaml"]


def test_upsert_migactionplan() -> None:
    client = FakeKubernetesClient()
    status = {
        "metrics": {"actionCount": 36, "gpuCount": 6},
        "planningSummary": {
            "actionCountsByType": {"clear_gpu": 9},
            "chosenTemplates": ["4+3"],
        },
        "fullPlanConfigMap": "stage0-full-plan",
        "canonicalNextStateConfigMap": "target0-state",
        "canonicalNextState": {
            "metadata": {"physical_id_map": {"0": "A"}},
            "gpus": [
                {
                    "gpuId": 0,
                    "instances": [
                        {"start": 0, "end": 4, "profile": "4g"},
                        {"start": 4, "end": 7, "profile": "3g"},
                    ],
                }
            ],
        },
    }
    client.autoapprovalpolicies[("or-sim", "default")] = {
        "metadata": {"name": "default"},
        "spec": {
            "enabled": True,
            "dryRunOnly": True,
            "maxActionCount": 40,
            "allowedExecutors": ["nvidia-gpu-operator"],
            "requireFullPlanConfigMap": True,
        },
    }
    upsert_migactionplan(
        name="stage0-action-plan",
        namespace="or-sim",
        owner_migplan="stage0",
        migplan_generation=3,
        auto_approval_policy_name="default",
        status=status,
        client=client,
    )

    obj = client.configmaps[("or-sim", "stage0-action-plan")]
    assert obj["kind"] == "MigActionPlan"
    assert obj["spec"]["executor"] == "nvidia-gpu-operator"
    assert obj["spec"]["phaseGate"] == "PendingApproval"
    assert obj["spec"]["autoApprovalPolicyRef"] == "default"
    assert obj["spec"]["actionCountsByType"] == {"clear_gpu": 9}
    assert obj["spec"]["executorPreview"]["previewOnly"] is True
    assert obj["spec"]["executorPreview"]["gpuTargets"][0]["targetTemplate"] == "4g+3g"
    assert obj["spec"]["executorPreview"]["unresolvedPhysicalGpuIds"] == ["A"]
    assert client.statuses[("or-sim", "stage0-action-plan")]["phase"] == "ApprovedDryRun"


def test_build_gpu_operator_executor_preview_with_node_bindings() -> None:
    status = {
        "canonicalNextState": {
            "metadata": {
                "physical_id_map": {"0": "A"},
                "physicalGpuBindings": {"A": {"nodeName": "gpu-node-0", "deviceIndex": 2}},
            },
            "gpus": [
                {
                    "gpuId": 0,
                    "instances": [
                        {"start": 0, "end": 3, "profile": "3g"},
                        {"start": 3, "end": 6, "profile": "3g"},
                        {"start": 6, "end": 7, "profile": "void"},
                    ],
                }
            ],
        }
    }
    preview = build_gpu_operator_executor_preview(status)
    assert preview["unresolvedPhysicalGpuIds"] == []
    assert preview["gpuTargets"][0]["nodeName"] == "gpu-node-0"
    assert preview["gpuTargets"][0]["deviceIndex"] == 2
    assert preview["gpuTargets"][0]["targetTemplate"] == "3g+3g"
    assert "gpu-node-0" in preview["wouldPatchNodeLabels"]
    assert preview["wouldPatchNodeLabels"]["gpu-node-0"]["nvidia.com/mig.config"].startswith("or-sim-")


def test_evaluate_auto_approval_policy_blocks_large_plan() -> None:
    policy = {
        "metadata": {"name": "default"},
        "spec": {
            "enabled": True,
            "dryRunOnly": True,
            "maxActionCount": 10,
            "allowedExecutors": ["nvidia-gpu-operator"],
            "requireFullPlanConfigMap": True,
        },
    }
    action_plan = {
        "spec": {
            "dryRun": True,
            "executor": "nvidia-gpu-operator",
            "actionCount": 36,
            "fullPlanConfigMap": "stage0-full-plan",
        }
    }
    status = evaluate_auto_approval_policy(policy=policy, action_plan=action_plan)
    assert status["phase"] == "ApprovalBlocked"
    assert "exceeds maxActionCount" in status["reasons"][0]


def test_compact_migplan_status_for_k8s_removes_large_fields() -> None:
    status = {
        "phase": "ReachedTarget",
        "reachedTarget": True,
        "message": "ok",
        "metrics": {"gpuCount": 6},
        "planningTrace": {"milp": {"status": "OPTIMAL"}},
        "observedGeneration": 3,
        "migPlanName": "stage0",
        "canonicalNextStateConfigMap": "target0-state",
        "fullPlanConfigMap": "stage0-full-plan",
        "actionPlanRef": "stage0-action-plan",
        "actions": [{"type": "clear_gpu"}],
        "targetState": {"gpus": []},
        "executedState": {"gpus": []},
        "canonicalNextState": {"gpus": []},
        "milp": {
            "status": "OPTIMAL",
            "gpuCount": 6,
            "chosenTemplates": ["4+3"],
            "KTotal": {"4g": 1},
            "alloc": [{"workload": "llama"}],
        },
    }
    compact = compact_migplan_status_for_k8s(status)
    assert compact["actions"] is None
    assert compact["planningTrace"] is None
    assert compact["targetState"] is None
    assert compact["executedState"] is None
    assert compact["canonicalNextState"] is None
    assert compact["planningSummary"]["pipeline"][1]["status"] == "OPTIMAL"
    assert compact["fullPlanConfigMap"] == "stage0-full-plan"
    assert compact["actionPlanRef"] == "stage0-action-plan"
    assert compact["milp"] == {
        "status": "OPTIMAL",
        "gpuCount": 6,
        "chosenTemplates": ["4+3"],
        "KTotal": {"4g": 1},
        "alloc": None,
    }


def test_watch_loop_skips_observed_initial_object() -> None:
    client = FakeKubernetesClient()
    client.migplans = [
        {
            "metadata": {"name": "stage0", "generation": 1},
            "status": {"observedGeneration": 1, "phase": "ReachedTarget"},
        }
    ]
    summary = run_watch_controller_loop(
        namespace="or-sim",
        max_events=1,
        client=client,
    )
    assert summary["summary"]["skipped"] == ["stage0"]
    assert summary["summary"]["reconciled"] == []


def test_dry_run_actuator_succeeds_approved_plan() -> None:
    client = FakeKubernetesClient()
    client.configmaps[("or-sim", "stage0-full-plan")] = {
        "data": {"status.yaml": "actions:\n- type: clear_gpu\n- type: bind_target_gpu\n"}
    }
    client.configmaps[("or-sim", "target0-state")] = {
        "data": {"state.yaml": "metadata: {}\ngpus: []\n"}
    }
    action_plan = {
        "metadata": {"name": "stage0-action-plan", "generation": 1},
        "spec": {
            "dryRun": True,
            "executor": "nvidia-gpu-operator",
            "fullPlanConfigMap": "stage0-full-plan",
            "canonicalNextStateConfigMap": "target0-state",
            "actionCount": 2,
            "executorPreview": _valid_executor_preview(),
        },
        "status": {"phase": "ApprovedDryRun", "approved": True, "executed": False, "policyRef": "default"},
    }
    assert should_actuate_dry_run(action_plan)
    status = validate_and_succeed_dry_run_action_plan(
        action_plan=action_plan,
        namespace="or-sim",
        client=client,
    )
    assert status["phase"] == "SucceededDryRun"
    assert status["executed"] is True
    assert status["validated"]["actionCount"] == 2
    assert status["validated"]["executorPreview"]["gpuTargetCount"] == 1


def test_dry_run_actuator_blocks_action_count_mismatch() -> None:
    client = FakeKubernetesClient()
    client.configmaps[("or-sim", "stage0-full-plan")] = {
        "data": {"status.yaml": "actions:\n- type: clear_gpu\n"}
    }
    client.configmaps[("or-sim", "target0-state")] = {
        "data": {"state.yaml": "metadata: {}\ngpus: []\n"}
    }
    action_plan = {
        "metadata": {"name": "stage0-action-plan", "generation": 1},
        "spec": {
            "dryRun": True,
            "executor": "nvidia-gpu-operator",
            "fullPlanConfigMap": "stage0-full-plan",
            "canonicalNextStateConfigMap": "target0-state",
            "actionCount": 2,
            "executorPreview": _valid_executor_preview(),
        },
        "status": {"phase": "ApprovedDryRun", "approved": True, "executed": False},
    }
    status = validate_and_succeed_dry_run_action_plan(
        action_plan=action_plan,
        namespace="or-sim",
        client=client,
    )
    assert status["phase"] == "ExecutionBlocked"
    assert "does not match spec.actionCount" in status["reasons"][0]


def test_dry_run_actuator_blocks_missing_executor_preview() -> None:
    client = FakeKubernetesClient()
    client.configmaps[("or-sim", "stage0-full-plan")] = {
        "data": {"status.yaml": "actions:\n- type: clear_gpu\n"}
    }
    client.configmaps[("or-sim", "target0-state")] = {
        "data": {"state.yaml": "metadata: {}\ngpus: []\n"}
    }
    action_plan = {
        "metadata": {"name": "stage0-action-plan", "generation": 1},
        "spec": {
            "dryRun": True,
            "executor": "nvidia-gpu-operator",
            "fullPlanConfigMap": "stage0-full-plan",
            "canonicalNextStateConfigMap": "target0-state",
            "actionCount": 1,
        },
        "status": {"phase": "ApprovedDryRun", "approved": True, "executed": False},
    }
    status = validate_and_succeed_dry_run_action_plan(
        action_plan=action_plan,
        namespace="or-sim",
        client=client,
    )
    assert status["phase"] == "ExecutionBlocked"
    assert "executorPreview is required" in status["reasons"]


def test_load_scenario_from_configmap() -> None:
    client = FakeKubernetesClient()
    client.configmaps[("or-sim", "scenario-stage0")] = {
        "data": {
            "scenario.yaml": """
name: stage0
sourceStateRef: ../gpu-states/simulation-empty-9-a100.yaml
targetStateRef: target0
workloadOrder: [llama]
workloadRefs:
  llama: ../../manifests/examples/workloadrequests/llama.yaml
profileCatalogRefs:
  llama: ../profile-catalogs/llama.yaml
sourceArrival:
  llama: 0
targetArrival:
  llama: 3
"""
        }
    }
    scenario = load_scenario_for_migplan_spec(
        spec={"scenarioConfigMap": "scenario-stage0"},
        namespace="or-sim",
        scenario_root="k8s-extension-prototype/mock/scenarios",
        client=client,
    )
    assert scenario.name == "stage0"
    assert scenario.target_state_ref == "target0"
    assert scenario.workloads[0].name == "llama"
    assert scenario.workloads[0].profile_catalog_ref.endswith("mock/profile-catalogs/llama.yaml")


def test_load_scenario_profile_catalog_configmaps() -> None:
    client = FakeKubernetesClient()
    client.configmaps[("or-sim", "scenario-stage0")] = {
        "data": {
            "scenario.yaml": """
name: stage0
sourceStateRef: ../gpu-states/simulation-empty-9-a100.yaml
targetStateRef: target0
workloadOrder: [llama]
workloadRefs:
  llama: ../../manifests/examples/workloadrequests/llama.yaml
profileCatalogConfigMaps:
  llama: profile-catalog-llama
sourceArrival:
  llama: 0
targetArrival:
  llama: 3
"""
        }
    }
    client.configmaps[("or-sim", "profile-catalog-llama")] = {
        "data": {
            "catalog.yaml": """
options:
- workload: llama
  family: llm
  batch: 1
  profile: 3g
  mu: 0.74
  fit: true
"""
        }
    }

    scenario = load_scenario_for_migplan_spec(
        spec={"scenarioConfigMap": "scenario-stage0"},
        namespace="or-sim",
        scenario_root="k8s-extension-prototype/mock/scenarios",
        client=client,
    )
    assert scenario.workloads[0].profile_catalog_ref is None
    assert scenario.workloads[0].profile_catalog_configmap == "profile-catalog-llama"

    catalogs = load_profile_catalogs_for_scenario(
        scenario=scenario,
        namespace="or-sim",
        client=client,
    )
    assert catalogs is not None
    assert catalogs["llama"][0].profile == "3g"


def _valid_executor_preview() -> dict[str, Any]:
    return {
        "previewOnly": True,
        "executor": "nvidia-gpu-operator",
        "gpuOperatorLabel": "nvidia.com/mig.config",
        "gpuTargets": [{"physicalGpuId": "A", "targetTemplate": "4g+3g"}],
        "wouldPatchNodeLabels": {},
        "unresolvedPhysicalGpuIds": ["A"],
    }


def main() -> int:
    test_should_reconcile_generation_gap()
    test_should_skip_observed_generation()
    test_upsert_cluster_state_configmap()
    test_upsert_full_plan_configmap()
    test_upsert_migactionplan()
    test_build_gpu_operator_executor_preview_with_node_bindings()
    test_evaluate_auto_approval_policy_blocks_large_plan()
    test_compact_migplan_status_for_k8s_removes_large_fields()
    test_watch_loop_skips_observed_initial_object()
    test_dry_run_actuator_succeeds_approved_plan()
    test_dry_run_actuator_blocks_action_count_mismatch()
    test_dry_run_actuator_blocks_missing_executor_preview()
    test_load_scenario_from_configmap()
    test_load_scenario_profile_catalog_configmaps()
    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
