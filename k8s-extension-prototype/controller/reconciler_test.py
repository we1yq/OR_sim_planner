from __future__ import annotations

from typing import Any

import yaml

from actuator import should_actuate_dry_run, validate_and_succeed_dry_run_action_plan
from adapters.observer_adapter import DryRunObservedStateBuilder
from cluster_observer import build_observed_cluster_state_from_k8s_lists, observed_cluster_state_status
from executor_preview import (
    build_abstract_action_preview,
    build_adapter_dry_run_preview,
    build_gpu_operator_executor_preview,
    build_mig_geometry_preview,
    build_observer_preview,
    build_pod_lifecycle_preview,
    build_traffic_and_drain_preview,
)
from k8s_adapter import plan_scenario_as_migplan_status
from models import PlanningScenario, ScenarioWorkloadDemand
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
from simulation_core.state import ClusterState, GPUState, MigInstance


class FakeKubernetesClient:
    def __init__(self) -> None:
        self.configmaps: dict[tuple[str, str], dict[str, Any]] = {}
        self.workloadrouteplans: dict[tuple[str, str], dict[str, Any]] = {}
        self.servinginstancedrains: dict[tuple[str, str], dict[str, Any]] = {}
        self.podlifecycleplans: dict[tuple[str, str], dict[str, Any]] = {}
        self.observedclusterstates: dict[tuple[str, str], dict[str, Any]] = {}
        self.statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self.workloadrouteplan_statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self.servinginstancedrain_statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self.podlifecycleplan_statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self.observedclusterstate_statuses: dict[tuple[str, str], dict[str, Any]] = {}
        self.autoapprovalpolicies: dict[tuple[str, str], dict[str, Any]] = {}
        self.migplans: list[dict[str, Any]] = []
        self.nodes: list[dict[str, Any]] = []
        self.pods: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def get_migplan(self, name: str, namespace: str) -> dict[str, Any]:
        raise NotImplementedError

    def list_migplans(self, namespace: str) -> list[dict[str, Any]]:
        return list(self.migplans)

    def list_nodes(self) -> list[dict[str, Any]]:
        return list(self.nodes)

    def list_pods(self, namespace: str) -> list[dict[str, Any]]:
        return [pod for pod in self.pods if pod.get("metadata", {}).get("namespace") == namespace]

    def patch_migplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        raise NotImplementedError

    def patch_migactionplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.statuses[(namespace, name)] = status

    def patch_workloadrouteplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.workloadrouteplan_statuses[(namespace, name)] = status

    def patch_servinginstancedrain_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.servinginstancedrain_statuses[(namespace, name)] = status

    def patch_podlifecycleplan_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.podlifecycleplan_statuses[(namespace, name)] = status

    def patch_observedclusterstate_status(self, name: str, namespace: str, status: dict[str, Any]) -> None:
        self.observedclusterstate_statuses[(namespace, name)] = status

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

    def apply_workloadrouteplan(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.workloadrouteplans[(metadata["namespace"], metadata["name"])] = manifest

    def apply_servinginstancedrain(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.servinginstancedrains[(metadata["namespace"], metadata["name"])] = manifest

    def apply_podlifecycleplan(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.podlifecycleplans[(metadata["namespace"], metadata["name"])] = manifest

    def apply_observedclusterstate(self, manifest: dict[str, Any]) -> None:
        metadata = manifest["metadata"]
        self.observedclusterstates[(metadata["namespace"], metadata["name"])] = manifest

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
        "actions": [
            {"type": "configure_full_template", "physical_gpu_id": "A", "template": "4g+3g"},
            {
                "type": "place_instance",
                "gpu_id": 0,
                "physical_gpu_id": "A",
                "slot": (0, 4, "4g"),
                "workload": "llama",
                "batch": 1,
            },
        ],
        "planningTrace": {
            "transition": {
                "finalCoarseActions": [
                    {
                        "type": "create_gpu",
                        "gpu_id": 0,
                        "new_physical_gpu_id": "A",
                        "template": "4g+3g",
                        "alloc_policy": "free_pool_lifo",
                    }
                ],
                "finalPlanItems": [
                    {
                        "id": "PLACE_gpu0_0_4_4g",
                        "type": "place_instance",
                        "current_phase": "prepare_target_side",
                        "status": "ready",
                        "gpu_id": 0,
                        "physical_gpu_id": "A",
                        "slot": (0, 4, "4g"),
                        "workload": "llama",
                    }
                ]
            }
        },
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
    assert obj["spec"]["migGeometryPreview"]["geometryActions"][0]["type"] == "configure_full_template"
    assert obj["spec"]["trafficAndDrainPreview"]["planItems"][0]["currentPhase"] == "prepare_target_side"
    assert obj["spec"]["podLifecyclePreview"]["createOrReuse"][0]["workload"] == "llama"
    assert obj["spec"]["abstractActionPreview"]["actions"][0]["type"] == "create_gpu"
    assert obj["spec"]["adapterDryRunPreview"]["adapters"]["pod"]["wouldCreateOrReuse"][0]["workload"] == "llama"
    assert obj["spec"]["observerPreview"]["targetsToObserve"]["physicalGpuIds"] == ["A"]
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
    assert preview["wouldPatchNodeLabels"]["gpu-node-0"]["nvidia.com/mig.config"] == "all-3g.20gb"


def test_build_action_rule_previews() -> None:
    status = {
        "actions": [
            {
                "type": "stop_accepting_new",
                "gpu_id": 1,
                "physical_gpu_id": "B",
                "slot": (0, 3, "3g"),
                "workload": "gpt2",
            },
            {
                "type": "reroute_queued_tasks",
                "gpu_id": 1,
                "physical_gpu_id": "B",
                "slot": (0, 3, "3g"),
                "workload": "gpt2",
                "queued": 2,
                "to": "target-backed[gpu2:3g[0,3)]",
            },
            {
                "type": "mark_draining_instance",
                "gpu_id": 1,
                "physical_gpu_id": "B",
                "slot": (0, 3, "3g"),
                "workload": "gpt2",
                "rounds": 1,
            },
            {
                "type": "remove_instance",
                "gpu_id": 1,
                "physical_gpu_id": "B",
                "slot": (0, 3, "3g"),
                "workload": "gpt2",
                "drained": True,
            },
            {
                "type": "update_batch",
                "gpu_id": 2,
                "physical_gpu_id": "C",
                "slot": (0, 4, "4g"),
                "workload": "llama",
                "old_batch": 1,
                "new_batch": 2,
            },
        ],
        "planningTrace": {
            "transition": {
                "finalCoarseActions": [
                    {
                        "type": "reconfiguration",
                        "gpu_id": 1,
                        "source_physical_gpu_id": "B",
                        "new_physical_gpu_id": "D",
                        "src_template": "3g+3g",
                        "tgt_template": "4g+3g",
                        "mode": "target_first",
                    }
                ],
                "finalPlanItems": [
                    {
                        "id": "RM_gpu1_0_3_3g",
                        "type": "remove_instance",
                        "current_phase": "drain_old",
                        "status": "blocked",
                        "blocked_by": "drain_started",
                        "gpu_id": 1,
                        "physical_gpu_id": "B",
                        "slot": (0, 3, "3g"),
                        "workload": "gpt2",
                        "takeover": "target-backed[gpu2:3g[0,3)]",
                        "queued": 2,
                        "inflight": 1,
                        "drain_remaining": None,
                        "capacity_safe": True,
                    }
                ]
            }
        },
        "canonicalNextState": {"metadata": {"physical_id_map": {}}, "gpus": []},
    }
    traffic = build_traffic_and_drain_preview(status)
    pod = build_pod_lifecycle_preview(status)
    mig = build_mig_geometry_preview(status)
    abstract = build_abstract_action_preview(status)
    adapter = build_adapter_dry_run_preview(status)
    observer = build_observer_preview(status)
    assert traffic["planItems"][0]["blockedBy"] == "drain_started"
    assert traffic["trafficActions"][1]["queued"] == 2
    assert pod["drain"][0]["podAction"] == "drain"
    assert pod["deleteOrRecycle"][0]["podAction"] == "delete-or-recycle"
    assert pod["reloadInPlace"][0]["podAction"] == "reload-in-place"
    assert mig["internalStateActionsExcluded"] == []
    assert abstract["actions"][0]["mode"] == "target_first"
    assert "prepareTargetMigGeometry" in abstract["actions"][0]["gates"]
    assert adapter["adapters"]["router"]["wouldRerouteQueuedTasks"][0]["queued"] == 2
    assert observer["targetsToObserve"]["workloads"] == ["gpt2"]


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
    milp_summary = [
        row for row in compact["planningSummary"]["pipeline"] if row["stage"] == "milp"
    ][0]
    assert milp_summary["status"] == "OPTIMAL"
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
            **_valid_action_previews(),
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
    assert status["validated"]["adapterContracts"]["router"]["wouldStopAcceptingNewCount"] == 1
    assert status["validated"]["adapterContracts"]["pod"]["wouldCreateOrReuseCount"] == 1
    assert status["validated"]["adapterContracts"]["observer"]["requiredObservationsCount"] == 1
    assert status["validated"]["observedStatePreview"]["previewOnly"] is True
    assert status["validated"]["observedStatePreview"]["queuedByWorkloadCount"] == 1
    assert "nvidia GPU node inventory" in status["validated"]["observedStatePreview"]["missingRealClusterInputs"]
    observed_cluster_state_name = status["validated"]["observedClusterStateRef"]
    observed_cluster_state = client.observedclusterstates[("or-sim", observed_cluster_state_name)]
    observed_cluster_state_status = client.observedclusterstate_statuses[
        ("or-sim", observed_cluster_state_name)
    ]
    assert observed_cluster_state["kind"] == "ObservedClusterState"
    assert observed_cluster_state["spec"]["previewOnly"] is True
    assert observed_cluster_state["spec"]["source"] == "dry-run-observer-skeleton"
    assert observed_cluster_state_status["phase"] == "SucceededDryRunPreview"
    assert observed_cluster_state_status["readyForCanonicalization"] is False
    assert status["validated"]["dryRunExecutionLog"]["previewOnly"] is True
    assert status["validated"]["dryRunExecutionLog"]["stepCount"] == 6
    assert "SimulatedCanonicalization" in status["validated"]["dryRunExecutionLog"]["phases"]
    execution_log_configmap_name = status["validated"]["dryRunExecutionLogConfigMap"]
    execution_log_configmap = client.configmaps[("or-sim", execution_log_configmap_name)]
    execution_log = yaml.safe_load(execution_log_configmap["data"]["execution-log.yaml"])
    assert execution_log["kind"] == "DryRunExecutionObservationLog"
    assert execution_log["previewOnly"] is True
    assert execution_log["steps"][-1]["phase"] == "SimulatedCanonicalization"
    assert "post-action state" in execution_log["steps"][-1]["warning"]
    assert status["validated"]["childResources"]["total"] == 3
    assert status["validated"]["childResources"]["succeededDryRunPreview"] == 3
    router_configmap_name = status["validated"]["mockRouterPlanConfigMap"]
    router_configmap = client.configmaps[("or-sim", router_configmap_name)]
    router_plan = yaml.safe_load(router_configmap["data"]["router-plan.yaml"])
    assert router_plan["kind"] == "RouterDryRunPlan"
    assert router_plan["previewOnly"] is True
    assert router_plan["workloadRoutePlans"][0]["kind"] == "WorkloadRoutePlan"
    assert router_plan["workloadRoutePlans"][0]["spec"]["action"] == "StopAcceptingNew"
    assert router_configmap["metadata"]["labels"]["mig.or-sim.io/state-kind"] == "router-dry-run-plan"
    workload_route_plan_name = status["validated"]["workloadRoutePlans"][0]
    workload_route_plan = client.workloadrouteplans[("or-sim", workload_route_plan_name)]
    assert workload_route_plan["kind"] == "WorkloadRoutePlan"
    assert workload_route_plan["metadata"]["labels"]["mig.or-sim.io/owner-action-plan"] == "stage0-action-plan"
    assert workload_route_plan["metadata"]["labels"]["mig.or-sim.io/preview-only"] == "true"
    assert workload_route_plan["spec"]["previewOnly"] is True
    assert workload_route_plan["spec"]["action"] == "StopAcceptingNew"
    assert "target" not in workload_route_plan["spec"]
    workload_route_status = client.workloadrouteplan_statuses[("or-sim", workload_route_plan_name)]
    assert workload_route_status["phase"] == "SucceededDryRunPreview"
    assert workload_route_status["previewOnly"] is True
    assert workload_route_status["ownerActionPlan"] == "stage0-action-plan"
    assert workload_route_status["observedGeneration"] == 1
    assert status["validated"]["childResources"]["items"][0] == {
        "kind": "WorkloadRoutePlan",
        "name": workload_route_plan_name,
        **workload_route_status,
    }
    serving_instance_drain_name = status["validated"]["servingInstanceDrains"][0]
    serving_instance_drain = client.servinginstancedrains[("or-sim", serving_instance_drain_name)]
    assert serving_instance_drain["kind"] == "ServingInstanceDrain"
    assert serving_instance_drain["spec"]["waitForInflightZero"] is True
    serving_instance_drain_status = client.servinginstancedrain_statuses[
        ("or-sim", serving_instance_drain_name)
    ]
    assert serving_instance_drain_status["phase"] == "SucceededDryRunPreview"
    assert serving_instance_drain_status["validatedBy"] == "mig-dry-run-actuator"
    pod_configmap_name = status["validated"]["mockPodLifecyclePlanConfigMap"]
    pod_configmap = client.configmaps[("or-sim", pod_configmap_name)]
    pod_plan = yaml.safe_load(pod_configmap["data"]["pod-lifecycle-plan.yaml"])
    assert pod_plan["kind"] == "PodLifecycleDryRunPlan"
    assert pod_plan["previewOnly"] is True
    assert pod_plan["podLifecyclePlans"][0]["kind"] == "PodLifecyclePlan"
    assert pod_plan["podLifecyclePlans"][0]["spec"]["action"] == "CreateOrReuse"
    assert pod_configmap["metadata"]["labels"]["mig.or-sim.io/state-kind"] == "pod-lifecycle-dry-run-plan"
    pod_lifecycle_plan_name = status["validated"]["podLifecyclePlans"][0]
    pod_lifecycle_plan = client.podlifecycleplans[("or-sim", pod_lifecycle_plan_name)]
    assert pod_lifecycle_plan["kind"] == "PodLifecyclePlan"
    assert pod_lifecycle_plan["metadata"]["labels"]["mig.or-sim.io/owner-action-plan"] == "stage0-action-plan"
    assert pod_lifecycle_plan["spec"]["previewOnly"] is True
    assert pod_lifecycle_plan["spec"]["action"] == "CreateOrReuse"
    pod_lifecycle_plan_status = client.podlifecycleplan_statuses[("or-sim", pod_lifecycle_plan_name)]
    assert pod_lifecycle_plan_status["phase"] == "SucceededDryRunPreview"
    assert pod_lifecycle_plan_status["message"].startswith("PodLifecyclePlan dry-run contract accepted")


def test_dry_run_observed_state_builder() -> None:
    preview = DryRunObservedStateBuilder().build(
        observer_preview={
            "targetsToObserve": {
                "physicalGpuIds": ["A"],
                "workloads": ["llama"],
                "planItemIds": ["RM_gpu0_0_4_4g"],
            },
            "canonicalizationRule": "canonicalize observed state",
        },
        canonical_state={"gpus": [{"gpuId": 0}]},
    )
    observed = preview["observedState"]
    assert observed["migLayouts"][0]["physicalGpuId"] == "A"
    assert observed["podReadiness"][0]["workload"] == "llama"
    assert observed["inflightByInstance"][0]["planItemId"] == "RM_gpu0_0_4_4g"
    assert observed["canonicalNextStateGpuCount"] == 1
    assert "router queued/inflight runtime metrics" in preview["missingRealClusterInputs"]


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
            **_valid_action_previews(),
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
    assert "migGeometryPreview is required" in status["reasons"]


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


def test_load_scenario_with_arrival_snapshot_configmap() -> None:
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
    client.configmaps[("or-sim", "arrival-stage0-epoch1")] = {
        "data": {
            "arrival-snapshot.yaml": """
name: arrival-stage0-epoch1
epoch: 1
source: external-forecast
targetStateRef: target0-epoch1
targetArrival:
  llama: 7
"""
        }
    }

    scenario = load_scenario_for_migplan_spec(
        spec={
            "scenarioConfigMap": "scenario-stage0",
            "arrivalSnapshotConfigMap": "arrival-stage0-epoch1",
        },
        namespace="or-sim",
        scenario_root="k8s-extension-prototype/mock/scenarios",
        client=client,
    )
    assert scenario.target_state_ref == "target0-epoch1"
    assert scenario.workloads[0].target_arrival == 7.0
    assert scenario.transition["arrivalSnapshot"] == {
        "name": "arrival-stage0-epoch1",
        "epoch": 1,
        "source": "external-forecast",
        "previewOnly": True,
    }


def test_current_state_feasible_arrival_snapshot_noop() -> None:
    scenario = PlanningScenario(
        name="noop-epoch",
        source_state_ref="observed-current-state",
        target_state_ref="target-noop",
        workloads=[
            ScenarioWorkloadDemand(
                name="llama",
                source_arrival=2.0,
                target_arrival=3.0,
                workload_ref="unused-workload.yaml",
            )
        ],
    )
    source_state = ClusterState(
        metadata={"gpuModel": "NVIDIA A100 40GB", "physical_id_map": {0: "A"}},
        gpus=[
            GPUState(
                gpu_id=0,
                instances=[
                    MigInstance(0, 4, "4g", workload="llama", batch=1, mu=4.0),
                    MigInstance(4, 7, "void"),
                ],
            )
        ],
    )

    status = plan_scenario_as_migplan_status(
        scenario=scenario,
        source_state_override=source_state,
    )
    assert status["status"]["phase"] == "SucceededNoOp"
    assert status["status"]["metrics"]["actionCount"] == 0
    assert status["status"]["metrics"]["noOp"] is True
    assert status["status"]["milp"]["status"] == "SKIPPED_CURRENT_STATE_FEASIBLE"
    assert status["status"]["currentStateFeasibility"]["recommendedAction"] == "no-op"
    assert status["status"]["planningTrace"]["pipeline"] == "source -> current-state-feasibility -> no-op"


def test_build_observed_cluster_state_from_k8s_lists() -> None:
    manifest = build_observed_cluster_state_from_k8s_lists(
        name="cluster-observed-state",
        namespace="or-sim",
        nodes=[
            {
                "metadata": {"name": "gpu-node-0", "labels": {"nvidia.com/gpu.present": "true"}},
                "status": {
                    "capacity": {"nvidia.com/gpu": "1"},
                    "allocatable": {"nvidia.com/gpu": "1"},
                    "conditions": [{"type": "Ready", "status": "True", "reason": "KubeletReady"}],
                },
            }
        ],
        pods=[
            {
                "metadata": {"namespace": "or-sim", "name": "llama-0"},
                "spec": {"nodeName": "gpu-node-0"},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
        ],
    )
    status = observed_cluster_state_status(manifest)
    assert manifest["kind"] == "ObservedClusterState"
    assert manifest["spec"]["previewOnly"] is False
    assert manifest["spec"]["observedState"]["nodeInventory"][0]["nodeName"] == "gpu-node-0"
    assert manifest["spec"]["observedState"]["podReadiness"][0]["ready"] is True
    assert "MIG device UUID and placement inventory" in manifest["spec"]["missingRealClusterInputs"]
    assert status["phase"] == "NodePodInventoryObserved"
    assert status["readyForCanonicalization"] is False


def test_build_observed_cluster_state_with_mig_profile_inventory() -> None:
    manifest = build_observed_cluster_state_from_k8s_lists(
        name="cluster-observed-state",
        namespace="or-sim",
        nodes=[
            {
                "metadata": {
                    "name": "rtx1",
                    "labels": {
                        "nvidia.com/gpu.present": "true",
                        "nvidia.com/mig.strategy": "mixed",
                        "nvidia.com/mig.config": "all-2g.10gb",
                        "nvidia.com/mig.config.state": "success",
                        "nvidia.com/mig-2g.10gb.count": "3",
                        "nvidia.com/mig-2g.10gb.product": "NVIDIA-A100-PCIE-40GB-MIG-2g.10gb",
                        "nvidia.com/mig-2g.10gb.memory": "9856",
                        "nvidia.com/mig-2g.10gb.multiprocessors": "28",
                        "nvidia.com/mig-2g.10gb.replicas": "1",
                        "nvidia.com/mig-2g.10gb.slices.gi": "2",
                        "nvidia.com/mig-2g.10gb.slices.ci": "2",
                        "nvidia.com/mig-2g.10gb.engines.copy": "2",
                        "nvidia.com/mig-2g.10gb.engines.decoder": "1",
                        "nvidia.com/mig-2g.10gb.engines.encoder": "0",
                    },
                },
                "status": {
                    "capacity": {
                        "nvidia.com/gpu": "3",
                        "nvidia.com/mig-2g.10gb": "3",
                    },
                    "allocatable": {
                        "nvidia.com/gpu": "3",
                        "nvidia.com/mig-2g.10gb": "3",
                    },
                    "conditions": [{"type": "Ready", "status": "True", "reason": "KubeletReady"}],
                },
            }
        ],
        pods=[],
    )
    observed = manifest["spec"]["observedState"]
    layout = observed["migLayouts"][0]
    profile = layout["profiles"][0]
    status = observed_cluster_state_status(manifest)

    assert manifest["metadata"]["labels"]["mig.or-sim.io/observer-kind"] == "kubernetes-mig-node"
    assert manifest["spec"]["source"] == "kubernetes-mig-node-observer"
    assert layout["nodeName"] == "rtx1"
    assert layout["migConfig"] == "all-2g.10gb"
    assert layout["migConfigState"] == "success"
    assert profile["profile"] == "2g.10gb"
    assert profile["labelCount"] == 3
    assert profile["capacity"] == 3
    assert profile["allocatable"] == 3
    assert profile["memoryMiB"] == 9856
    assert profile["multiprocessors"] == 28
    assert profile["slices"] == {"gi": 2, "ci": 2}
    assert profile["engines"]["copy"] == 2
    assert "MIG device UUID and placement inventory" in manifest["spec"]["missingRealClusterInputs"]
    assert status["phase"] == "MigNodeInventoryObserved"
    assert status["readyForCanonicalization"] is False


def _valid_executor_preview() -> dict[str, Any]:
    return {
        "previewOnly": True,
        "executor": "nvidia-gpu-operator",
        "gpuOperatorLabel": "nvidia.com/mig.config",
        "gpuTargets": [{"physicalGpuId": "A", "targetTemplate": "4g+3g"}],
        "wouldPatchNodeLabels": {},
        "unresolvedPhysicalGpuIds": ["A"],
    }


def _valid_action_previews() -> dict[str, Any]:
    return {
        "migGeometryPreview": {
            "previewOnly": True,
            "adapter": "mig-geometry",
            "geometryActions": [],
            "wouldPatchNodeLabels": {},
            "migManagerTargetConfigs": [],
            "unresolvedPhysicalGpuIds": ["A"],
        },
        "trafficAndDrainPreview": {
            "previewOnly": True,
            "adapter": "router-drain",
            "planItems": [],
            "trafficActions": [
                {
                    "type": "stop_accepting_new",
                    "gpu_id": 0,
                    "physical_gpu_id": "A",
                    "slot": (0, 4, "4g"),
                    "workload": "llama",
                },
                {
                    "type": "mark_draining_instance",
                    "gpu_id": 0,
                    "physical_gpu_id": "A",
                    "slot": (0, 4, "4g"),
                    "workload": "llama",
                    "rounds": 1,
                }
            ],
        },
        "podLifecyclePreview": {
            "previewOnly": True,
            "adapter": "pod-lifecycle",
            "createOrReuse": [{"workload": "llama", "podAction": "create-or-reuse"}],
            "drain": [],
            "deleteOrRecycle": [],
            "reloadInPlace": [],
        },
        "abstractActionPreview": {
            "previewOnly": True,
            "actions": [],
        },
        "adapterDryRunPreview": {
            "previewOnly": True,
            "adapters": {},
        },
        "observerPreview": {
            "previewOnly": True,
            "requiredObservations": {"pod": ["podReadiness"]},
            "targetsToObserve": {
                "physicalGpuIds": ["A"],
                "workloads": ["llama"],
                "planItemIds": ["PLACE_gpu0_0_4_4g"],
            },
        },
    }


def main() -> int:
    test_should_reconcile_generation_gap()
    test_should_skip_observed_generation()
    test_upsert_cluster_state_configmap()
    test_upsert_full_plan_configmap()
    test_upsert_migactionplan()
    test_build_gpu_operator_executor_preview_with_node_bindings()
    test_build_action_rule_previews()
    test_evaluate_auto_approval_policy_blocks_large_plan()
    test_compact_migplan_status_for_k8s_removes_large_fields()
    test_watch_loop_skips_observed_initial_object()
    test_dry_run_actuator_succeeds_approved_plan()
    test_dry_run_observed_state_builder()
    test_dry_run_actuator_blocks_action_count_mismatch()
    test_dry_run_actuator_blocks_missing_executor_preview()
    test_load_scenario_from_configmap()
    test_load_scenario_profile_catalog_configmaps()
    test_load_scenario_with_arrival_snapshot_configmap()
    test_current_state_feasible_arrival_snapshot_noop()
    test_build_observed_cluster_state_from_k8s_lists()
    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
