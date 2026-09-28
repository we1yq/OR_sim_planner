"""Unit tests for the safe runner's audit and state-machine boundaries."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("live_runner_20260926.py")
SPEC = importlib.util.spec_from_file_location("live_runner_20260926", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def registry_fixture(*, runtimes: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "status": {
            "health": {"stable": True, "repairRequired": False, "requiredActions": []},
            "queueCounts": {"transitioning": 0},
            "currentAllocation": {"logicalGpus": []},
            "bindings": {
                "gpu-a": {"product": "NVIDIA A100-PCIE-40GB", "migDevices": [], "runtimeBindings": runtimes or []},
                "gpu-b": {"product": "NVIDIA A100-PCIE-40GB", "migDevices": [{"profile": "1g"}], "runtimeBindings": []},
                "gpu-c": {"product": "NVIDIA A100-PCIE-40GB", "migDevices": [], "runtimeBindings": []},
            },
        }
    }


def plan_fixture(*, action_type: str = "allocate_gpu", physical_id: str = "gpu-a") -> dict[str, object]:
    return {
        "metadata": {"name": "plan-test"},
        "spec": {
            "plannerMetadata": {"solverStatus": "OPTIMAL"},
            "targetGpuCount": 1,
            "actionDag": {
                "nodes": [
                    {"id": "a", "type": action_type, "physicalGpuId": physical_id, "dependsOn": []},
                    {"id": "b", "type": "place_instance", "physicalGpuId": physical_id, "dependsOn": ["a"]},
                ]
            },
            "targetAllocationPlan": {"physicalGpuId": physical_id},
        },
        "status": {"phase": "Planned"},
    }


class RunnerAuditTests(unittest.TestCase):
    def test_execution_modes_are_explicitly_mutually_exclusive(self) -> None:
        parser = runner.build_parser()
        args = parser.parse_args(["--preflight-only"])
        self.assertTrue(args.preflight_only)
        self.assertFalse(args.execute)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--preflight-only", "--execute"])

    def test_makespan_mode_has_a_single_explicit_switch_and_five_second_dwell(self) -> None:
        args = runner.build_parser().parse_args(["--execute", "--makespan-mode"])
        self.assertTrue(args.makespan_mode)
        self.assertEqual(args.post_target_dwell_seconds, 5.0)
        self.assertEqual(args.source_control_seconds, 60.0)

        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext(
                "run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60,
                makespan_mode=True, post_target_dwell_seconds=5,
            )
            runner._write_initial_outputs(ctx, SimpleNamespace(), {})
            environment = json.loads((ctx.output_dir / "environment.json").read_text())
            protocol = json.loads((ctx.output_dir / "profile_protocol.json").read_text())
        self.assertEqual(environment["experiment_mode"], "transition_makespan_no_traffic_no_profile")
        self.assertEqual(environment["post_target_dwell_seconds"], 5)
        self.assertEqual(protocol["status"], "skipped_no_profile")

    def test_resume_requires_an_explicit_later_round_and_reuses_run_id(self) -> None:
        parser = runner.build_parser()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "environment.json").write_text(json.dumps({"run_id": "prior-run"}), encoding="utf-8")
            args = parser.parse_args(["--execute", "--makespan-mode", "--start-round", "2", "--resume-run-dir", str(root)])
            ctx = runner.make_run_context(args, root / "unused")
        self.assertEqual(ctx.run_id, "prior-run")
        self.assertEqual(ctx.output_dir, root.resolve())

    def test_preflight_requires_three_a100s_empty_routes_and_healthy_controllers(self) -> None:
        controllers = {name: {"status": {"replicas": 1, "availableReplicas": 1}} for name in ("planner", "executor")}
        report = runner.audit_preflight(registry_fixture(), {"routes": []}, controllers)
        self.assertEqual(report, [])

        bad_registry = registry_fixture(runtimes=[{"model": "resnet50"}])
        report = runner.audit_preflight(bad_registry, {"routes": [{"model": "resnet50"}]}, controllers)
        self.assertIn("R1 registry allocation is not empty", report)
        self.assertIn("R1 router routes are not empty", report)

    def test_only_optimal_plans_with_valid_dags_and_registry_ids_pass(self) -> None:
        report = runner.audit_plan(plan_fixture(), registry_fixture(), source_gpu_count=0)
        self.assertTrue(report["ok"], report)
        self.assertEqual(report["conservativePeakGpuCount"], 1)

        non_optimal = plan_fixture()
        non_optimal["spec"]["plannerMetadata"] = {"solverStatus": "FEASIBLE"}  # type: ignore[index]
        self.assertFalse(runner.audit_plan(non_optimal, registry_fixture())["ok"])

        nested = plan_fixture()
        nested["spec"]["plannerMetadata"] = {  # type: ignore[index]
            "planningTrace": {"milp": {"status": "optimal"}}
        }
        self.assertTrue(runner.audit_plan(nested, registry_fixture(), source_gpu_count=0)["ok"])

        allocate_then_bind = plan_fixture()
        allocate_then_bind["spec"]["actionDag"]["nodes"].insert(  # type: ignore[index]
            1,
            {"id": "bind", "type": "bind_target_gpu", "physicalGpuId": "gpu-a", "dependsOn": ["a"]},
        )
        allocate_then_bind["spec"]["actionDag"]["nodes"][2]["dependsOn"] = ["bind"]  # type: ignore[index]
        bind_report = runner.audit_plan(allocate_then_bind, registry_fixture(), source_gpu_count=0)
        self.assertTrue(bind_report["ok"], bind_report)
        self.assertEqual(bind_report["conservativePeakGpuCount"], 1)

        available_only_in_bindings = registry_fixture()
        available_only_in_bindings["status"]["currentAllocation"] = {  # type: ignore[index]
            "gpus": {"gpu-a": {"state": "active"}}
        }
        available_plan = plan_fixture(physical_id="gpu-c")
        available_report = runner.audit_plan(available_plan, available_only_in_bindings, source_gpu_count=0)
        self.assertTrue(available_report["ok"], available_report)

        empty = plan_fixture()
        empty["spec"]["targetGpuCount"] = 0  # type: ignore[index]
        empty["spec"]["actionDag"]["nodes"] = []  # type: ignore[index]
        empty_report = runner.audit_plan(
            empty,
            registry_fixture(),
            source_gpu_count=0,
            require_nonzero_target=True,
        )
        self.assertTrue(any("nonzero initial demand" in error for error in empty_report["errors"]))

    def test_later_round_source_check_allows_existing_allocation_but_requires_drain(self) -> None:
        controllers = {"executor": {"status": {"replicas": 1, "availableReplicas": 1}}}
        occupied = registry_fixture(runtimes=[{"model": "resnet50_image"}])
        self.assertEqual(
            runner.audit_preflight(occupied, {"routes": []}, controllers, require_empty=False),
            [],
        )
        busy = {"routes": [{"runtimeId": "runtime-a", "endpointInflight": 1, "endpointQueued": 0}]}
        errors = runner.audit_preflight(occupied, busy, controllers, require_empty=False)
        self.assertTrue(any("non-drained" in error for error in errors))

    def test_source_signature_detects_batch_and_layout_drift(self) -> None:
        registry = registry_fixture(runtimes=[{"runtimeId": "r1", "model": "resnet50_image", "batchSize": 4}])
        routes = {"routes": [{"runtimeId": "r1", "model": "resnet50_image", "batchSize": 4, "active": True}]}
        baseline = runner.source_state_signature(registry, routes)
        changed_routes = {"routes": [{"runtimeId": "r1", "model": "resnet50_image", "batchSize": 8, "active": True}]}
        self.assertNotEqual(baseline, runner.source_state_signature(registry, changed_routes))

    def test_forbidden_actions_unknown_ids_cycles_and_peak_are_blocked(self) -> None:
        blocked = runner.audit_plan(plan_fixture(action_type="defer"), registry_fixture())
        self.assertTrue(any("forbidden" in error for error in blocked["errors"]))

        unknown = runner.audit_plan(plan_fixture(physical_id="offline-placeholder"), registry_fixture())
        self.assertTrue(any("absent from registry" in error for error in unknown["errors"]))

        cycle = plan_fixture()
        cycle["spec"]["actionDag"]["nodes"][0]["dependsOn"] = ["b"]  # type: ignore[index]
        cycle_report = runner.audit_plan(cycle, registry_fixture())
        self.assertTrue(any("cycle" in error for error in cycle_report["errors"]))

        peak_plan = plan_fixture()
        peak_plan["spec"]["targetGpuCount"] = 4  # type: ignore[index]
        peak_report = runner.audit_plan(peak_plan, registry_fixture(), source_gpu_count=3)
        self.assertTrue(any("exceeds 3" in error for error in peak_report["errors"]))


class RunnerStateTests(unittest.TestCase):
    def test_state_machine_requires_audit_and_approval_order(self) -> None:
        state = "PREFLIGHT"
        for event, expected in (
            ("snapshot_created", "SNAPSHOT_CREATED"),
            ("plan_planned", "PLAN_PLANNED"),
            ("audit_passed", "AUDITED"),
            ("transition_sender_started", "TRANSITION_SENDER_STARTED"),
            ("approved", "APPROVED"),
            ("terminal", "TERMINAL"),
            ("final_validated", "FINAL_VALIDATED"),
            ("target_steady", "TARGET_STEADY"),
            ("profiled", "PROFILED"),
            ("round_saved", "ROUND_SAVED"),
            ("next_round", "PREFLIGHT"),
        ):
            state = runner.transition_state(state, event)
            self.assertEqual(state, expected)
        with self.assertRaises(ValueError):
            runner.transition_state("PLAN_PLANNED", "approved")

    def test_failure_is_terminal_safe_stop(self) -> None:
        self.assertEqual(runner.transition_state("AUDITED", "failed"), "FAILED")

    def test_capacity_ledger_starts_from_source_and_uses_effective_route_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext("run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60)
            (ctx.output_dir / "environment.json").write_text(
                json.dumps({"clock_alignment": {"status": "synchronized"}}), encoding="utf-8"
            )
            source = [{"runtimeId": "old", "workload": "resnet50_image", "profile": "1g", "batchSize": 1}]
            final = [{"runtimeId": "new", "workload": "resnet50_image", "profile": "1g", "batchSize": 1}]
            lifecycle = [
                {**source[0], "runtime_id": "old", "physical_profile": "1g", "batch": 1,
                 "route_stop_accepting_effective_at": "2026-01-01T00:00:02Z"},
                {**final[0], "runtime_id": "new", "physical_profile": "1g", "batch": 1,
                 "pod_ready_at": "2026-01-01T00:00:02Z", "route_activation_ack_at": "2026-01-01T00:00:03Z"},
            ]
            terminal = {"status": {"transitionExecution": {"timestamps": {"executorStartedAt": "2026-01-01T00:00:01Z"}}}}
            catalog = [{"workload": "resnet50_image", "profile": "1g", "batch": 1, "mu": 25.0}]
            rates = {key: 0.0 for key in runner.WORKLOAD_KEYS}
            rates["resnet50_image"] = 20.0
            rows = runner._capacity_ledger_rows(
                ctx, 2, 7, terminal, source, final, lifecycle, [], catalog, rates, rates
            )
        workload_rows = [row for row in rows if row["workload"] == "resnet50_image"]
        self.assertEqual(workload_rows[0]["event_type"], "source_baseline")
        self.assertEqual(workload_rows[0]["capacity"], 25.0)
        self.assertEqual([row["event_type"] for row in workload_rows[1:]], ["stop_accepting_remove", "route_ready_add"])
        self.assertEqual(workload_rows[-1]["capacity"], 25.0)
        self.assertFalse(any(row["uncertain"] for row in workload_rows))

    def test_runtime_events_use_executor_effect_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext("run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60)
            events = runner._runtime_events_from_actions(ctx, 2, 7, [{
                "action_id": "a", "action_type": "activate_instance_route",
                "runtime_id": "r1", "workload": "gpt2_p64_o64",
                "timing_runtimeReadyAndCUDAVerifiedAt": "2026-01-01T00:00:02Z",
                "timing_routeSyncedAt": "2026-01-01T00:00:03Z",
            }])
        self.assertEqual([event["event_type"] for event in events], ["pod_ready", "route_activation"])
        self.assertEqual(events[-1]["timestamp_source"], "router_upsert_ack")

    def test_round_artifact_writer_uses_stable_formal_schemas(self) -> None:
        runtime_id = "resnet50-image-ampere-gpu0-s0-1-1g"
        terminal = {
            "metadata": {"name": "plan-r1"},
            "spec": {
                "plannerMetadata": {"metrics": {"plannerMakespanSec": 0.5}},
                "actionDag": {"nodes": [{
                    "id": "a", "type": "activate_instance_route",
                    "action": {"type": "activate_instance_route", "workload": "resnet50_image",
                               "physical_gpu_id": "ampere-gpu0", "slot": [0, 1, "1g"]},
                }]},
            },
            "status": {
                "phase": "Executed",
                "actionStatuses": [{
                    "id": "a", "status": "completed", "startedAt": "2026-01-01T00:00:01Z",
                    "finishedAt": "2026-01-01T00:00:03Z",
                    "trace": {"timestamps": {
                        "runtimeReadyAndCUDAVerifiedAt": "2026-01-01T00:00:02Z",
                        "routeSyncedAt": "2026-01-01T00:00:03Z",
                    }},
                }],
                "transitionExecution": {
                    "timestamps": {"executorStartedAt": "2026-01-01T00:00:01Z", "executorFinishedAt": "2026-01-01T00:00:04Z"},
                    "metrics": {"finalValidation": {"ok": True, "finishedAt": "2026-01-01T00:00:04Z"}},
                },
            },
        }
        route = {
            "runtimeId": runtime_id, "workload": "resnet50_image", "model": "resnet50_image",
            "endpoint": "http://runtime", "profile": "1g", "batchSize": 1,
            "active": True, "acceptingNew": True, "draining": False,
        }
        profile_report = {"status": "ok", "samples": [{
            "replicaId": runtime_id, "endpoint": "http://runtime", "sequence": 1,
            "startedAtSeconds": 0.0, "endedAtSeconds": 1.0, "logicalSamples": 25,
            "runtimeInferenceSeconds": 1.0, "runtimeInferenceMs": 1000.0,
            "batchSize": 1, "profile": "1g", "complete": True,
        }]}
        rates = {key: 0.0 for key in runner.WORKLOAD_KEYS}
        rates["resnet50_image"] = 20.0
        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext("run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60)
            (ctx.output_dir / "plans").mkdir()
            (ctx.output_dir / "snapshots").mkdir()
            runner._write_initial_outputs(ctx, SimpleNamespace(), {})
            environment = json.loads((ctx.output_dir / "environment.json").read_text())
            environment["clock_alignment"] = {"status": "synchronized"}
            (ctx.output_dir / "environment.json").write_text(json.dumps(environment), encoding="utf-8")
            runner._record_round_artifacts(
                ctx, 1, 4, terminal, registry_fixture(), [], [route], profile_report,
                {key: 0.0 for key in runner.WORKLOAD_KEYS}, rates,
                [{"workload": "resnet50_image", "profile": "1g", "batch": 1, "mu": 25.0}],
            )
            self.assertTrue((ctx.output_dir / "profile_samples.csv").read_text().count("\n") >= 2)
            self.assertTrue((ctx.output_dir / "runtime_events.jsonl").read_text().strip())


class EnvironmentReadinessTests(unittest.TestCase):
    @staticmethod
    def ready_pod(name: str, node: str, component: str, image_id: str) -> dict[str, object]:
        return {
            "metadata": {
                "name": name,
                "namespace": "or-sim",
                "labels": {
                    "app.kubernetes.io/name": f"migrant-{component}",
                    "app.kubernetes.io/component": component,
                },
            },
            "spec": {"nodeName": node, "containers": [{"name": component, "image": "mirror/image:tag"}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": component, "ready": True, "imageID": image_id}],
            },
        }

    def test_central_registry_tags_are_queried_once_and_worker_readiness_remains_required(self) -> None:
        pods = [
            self.ready_pod("migrant-local-registry-0", "or-sim-control-plane", "local-registry", "docker://registry"),
            self.ready_pod("mig-node-agent-ampere", "ampere", "mig-node-agent", "docker://agent-a"),
            self.ready_pod("mig-node-agent-rtx", "rtx1-worker", "mig-node-agent", "docker://agent-b"),
            self.ready_pod("slot-device-plugin-ampere", "ampere", "slot-device-plugin", "docker://plugin-a"),
            self.ready_pod("slot-device-plugin-rtx", "rtx1-worker", "slot-device-plugin", "docker://plugin-b"),
        ]

        class FakeKube:
            def __init__(self) -> None:
                self.execs: list[list[str]] = []

            def get_json(self, resource: str, name: str | None = None) -> dict[str, object]:
                if resource == "pods":
                    return {"items": pods}
                if resource == "nodes":
                    return {"items": []}
                if resource == "daemonsets":
                    return {"items": []}
                raise AssertionError(resource)

            def run(self, args: list[str]) -> str:
                self.execs.append(args)
                if args[:2] == ["version", "-o"]:
                    return "{}"
                if "migrant-local-registry-0" in args:
                    return json.dumps({"tags": ["go", "llm"]})
                return "yes"

        readiness = {
            "registry": {},
            "observedA100Ids": ["gpu-a", "gpu-b", "gpu-c"],
            "controllers": {
                "transition-executor": {
                    "spec": {"template": {"spec": {"containers": [{"env": [
                        {"name": "VISION_MODEL_RUNTIME_IMAGE", "value": "localhost:10690/migrant-model-runtime:go"},
                        {"name": "GPT2_MODEL_RUNTIME_IMAGE", "value": "localhost:10690/migrant-model-runtime:llm"},
                    ]}]}}}
                }
            },
        }
        with tempfile.TemporaryDirectory() as directory, patch.object(runner, "_local_command", return_value=None):
            root = Path(directory)
            ctx = runner.RunContext("run", root, "or-sim", "http://router", 1, 1, 1)
            runner._json_output(root / "environment.json", {"run_id": "run", "started_at": "now", "traffic_seed": 71, "input_sha256": {}, "solver": {}})
            kube = FakeKube()
            environment = runner._enrich_environment(ctx, kube, readiness)

        image = environment["runtime_image_readiness"]
        self.assertEqual(image["registry_mode"], "centralized")
        self.assertEqual(image["registry_node"], "or-sim-control-plane")
        self.assertEqual(image["registry_query_count"], 1)
        self.assertEqual(image["registry_tags"], ["go", "llm"])
        self.assertTrue(image["worker_components_ready"])
        self.assertTrue(image["worker_mirror_pull_verified"])
        self.assertEqual(runner.environment_readiness_errors(environment), [])
        self.assertEqual(sum("migrant-local-registry-0" in args for args in kube.execs), 1)

    def test_worker_component_failure_is_not_hidden_by_central_registry_tags(self) -> None:
        errors = runner.environment_readiness_errors({
            "observation_errors": [],
            "clock_alignment": {"status": "worker_ntp_verified"},
            "runtime_image_readiness": {
                "ok": True,
                "registry_mode": "centralized",
                "registry_pod": "migrant-local-registry-0",
                "registry_node": "or-sim-control-plane",
                "missing_required_tags": [],
                "worker_components_ready": False,
                "worker_mirror_pull_verified": True,
            },
        })
        self.assertTrue(any("mig-node-agent and device-plugin" in error for error in errors))
        self.assertFalse(any("both GPU-node registries" in error for error in errors))


class KubectlBoundaryTests(unittest.TestCase):
    def test_plan_wait_retries_controller_creation_not_found(self) -> None:
        calls = 0

        class FakeKube:
            def get_json(self, resource: str, name: str) -> dict[str, object]:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise RuntimeError('Error from server (NotFound): migactionplans "plan-test" not found')
                return {"status": {"phase": "Planned"}}

        original_sleep = runner.time.sleep
        runner.time.sleep = lambda _: None
        try:
            observed = runner.wait_for_plan(FakeKube(), "plan-test", timeout=10, poll_seconds=0)
        finally:
            runner.time.sleep = original_sleep
        self.assertEqual(observed["status"]["phase"], "Planned")
        self.assertEqual(calls, 2)

    def test_execution_wait_does_not_return_the_preapproval_planned_phase(self) -> None:
        phases = iter(("Planned", "Executing", "Executed"))

        class FakeKube:
            def get_json(self, resource: str, name: str) -> dict[str, object]:
                return {"status": {"phase": next(phases)}}

        original_sleep = runner.time.sleep
        runner.time.sleep = lambda _: None
        try:
            terminal = runner.wait_for_plan(
                FakeKube(), "plan-test", timeout=10, poll_seconds=0, accept_planned=False
            )
        finally:
            runner.time.sleep = original_sleep
        self.assertEqual(terminal["status"]["phase"], "Executed")

    def test_approval_is_a_merge_patch_and_never_a_delete_or_reset(self) -> None:
        calls: list[tuple[list[str], str | None]] = []

        def command(argv: list[str], *, input_text: str | None = None, timeout: float | None = None) -> SimpleNamespace:
            calls.append((argv, input_text))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        kube = runner.Kubectl("or-sim", command=command)
        kube.approve("plan-test")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0][1], "patch")
        self.assertIn('"phaseGate":"approved"', calls[0][0][-1])
        self.assertNotIn("delete", " ".join(calls[0][0]))

    def test_snapshot_is_manual_and_preserves_all_seven_shapes(self) -> None:
        zero = {key: 0.0 for key in runner.WORKLOAD_KEYS}
        snapshot = runner.build_arrival_snapshot("snap", 1, zero, zero)
        self.assertEqual(snapshot["spec"]["phaseGate"], "manual")  # type: ignore[index]
        self.assertIs(snapshot["spec"]["forceReplan"], True)  # type: ignore[index]
        self.assertEqual(snapshot["spec"]["transitionDemandPolicy"], "min")  # type: ignore[index]
        self.assertEqual(snapshot["spec"]["stage3Variant"], "slicewise")  # type: ignore[index]
        self.assertEqual(tuple(snapshot["spec"]["targetDemand"]), runner.WORKLOAD_KEYS)  # type: ignore[index]
        self.assertEqual(snapshot["spec"]["scenarioPath"], "mock/scenarios/real8gpu.yaml")  # type: ignore[index]
        self.assertEqual(snapshot["spec"]["slo"]["gpt2_p512_o512"]["ttftMs"], 100.0)  # type: ignore[index]
        self.assertEqual(runner.WORKLOAD_CONTRACT["gpt2_p512_o512"]["promptLen"], 512)
        namespaced = runner.build_arrival_snapshot("snap-2", 1, zero, zero, namespace="custom-exp")
        self.assertEqual(namespaced["metadata"]["namespace"], "custom-exp")  # type: ignore[index]
        sw_c = runner.build_arrival_snapshot("snap-3", 1, zero, zero, stage3_variant="sw-c")
        self.assertEqual(sw_c["spec"]["stage3Variant"], "sw-c")  # type: ignore[index]
        generated = runner.snapshot_name_for_run("20260926T092447.387936Z", 1)
        self.assertRegex(generated, r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

    def test_r13_cleanup_is_zero_demand_and_kept_out_of_twelve_round_aggregate(self) -> None:
        registry = {
            "status": {
                "queueCounts": {"active": 0, "available": 3, "transitioning": 0},
                "bindings": {},
            }
        }
        planned = {"metadata": {"name": "plan-r13"}, "status": {"phase": "Planned"}}
        terminal = {
            "metadata": {"name": "plan-r13"},
            "status": {
                "phase": "Executed",
                "actionStatuses": [{"id": "a", "type": "return_gpu", "status": "completed"}],
                "transitionExecution": {"durations": {"makespanSec": 1.25}},
            },
        }

        class FakeKube:
            def __init__(self) -> None:
                self.applied: dict[str, object] | None = None
                self.approved = ""

            def apply(self, value: dict[str, object]) -> None:
                self.applied = value

            def approve(self, name: str) -> None:
                self.approved = name

            def get_json(self, resource: str, name: str | None = None) -> dict[str, object]:
                if resource == "pods":
                    return {"items": []}
                return registry

        demand = [{"round": number, **{key: 1.0 for key in runner.WORKLOAD_KEYS}} for number in range(1, 13)]
        args = SimpleNamespace(
            controller_names=["planner-controller"], placement_nodes=["ampere"],
            watchdog_seconds=10.0, poll_seconds=0.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext("run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60)
            (ctx.output_dir / "snapshots").mkdir()
            (ctx.output_dir / "plans").mkdir()
            kube = FakeKube()
            with (
                patch.object(runner, "preflight", return_value={"ok": True, "errors": [], "routes": {"routes": []}}),
                patch.object(runner, "wait_for_plan", side_effect=[planned, terminal]),
                patch.object(runner, "audit_plan", return_value={"ok": True, "errors": []}),
                patch.object(
                    runner,
                    "wait_independent_final_validation",
                    return_value=({"ok": True, "errors": []}, registry, {"routes": []}),
                ),
            ):
                summary = runner.execute_r13_cleanup(ctx, args, kube, SimpleNamespace(), demand)
            saved = json.loads((ctx.output_dir / "r13_cleanup_summary.json").read_text())

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["round"], 13)
        self.assertFalse(summary["includedInMeasuredTwelveRoundAggregate"])
        self.assertEqual(kube.applied["spec"]["targetDemand"], {key: 0.0 for key in runner.WORKLOAD_KEYS})  # type: ignore[index]
        self.assertEqual(kube.approved, "plan-live-v2-run-r13")
        self.assertEqual(saved["queueCounts"]["available"], 3)

    def test_sw_c_execution_requires_explicit_unsafe_acknowledgement(self) -> None:
        self.assertEqual(runner.main(["--execute", "--stage3-variant", "sw-c"]), 2)

    def test_plan_audit_rejects_ignored_stage3_variant(self) -> None:
        plan = plan_fixture()
        missing = runner.audit_plan(
            plan,
            registry_fixture(),
            expected_stage3_variant="sw-c",
        )
        self.assertFalse(missing["ok"])
        plan["spec"]["plannerMetadata"]["planningTrace"] = {  # type: ignore[index]
            "transition": {"stage3Variant": "sw-c"},
        }
        matched = runner.audit_plan(
            plan,
            registry_fixture(),
            expected_stage3_variant="sw-c",
        )
        self.assertTrue(matched["ok"], matched["errors"])

    def test_independent_validation_waits_for_registry_stability(self) -> None:
        class FakeKube:
            def __init__(self) -> None:
                self.calls = 0

            def get_json(self, resource: str, name: str) -> dict[str, object]:
                self.calls += 1
                return {
                    "status": {
                        "health": {"stable": self.calls >= 2, "repairRequired": False, "requiredActions": []},
                        "queueCounts": {"transitioning": 0},
                        "bindings": {"gpu-a": {}},
                    }
                }

        class FakeRouter:
            def get_json(self, path: str) -> dict[str, object]:
                return {"routes": []}

        plan = {
            "spec": {},
            "status": {"transitionExecution": {"metrics": {"finalValidation": {"ok": True}}}},
        }
        original_sleep = runner.time.sleep
        runner.time.sleep = lambda _: None
        try:
            validation, _, _ = runner.wait_independent_final_validation(
                FakeKube(), FakeRouter(), plan, timeout=1, poll_seconds=0
            )
        finally:
            runner.time.sleep = original_sleep
        self.assertTrue(validation["ok"])

    def test_round_failure_capture_preserves_cluster_observation_errors(self) -> None:
        class FakeKube:
            def get_json(self, resource: str, name: str) -> dict[str, object]:
                return {"status": {"health": {"stable": True}}}

        class BrokenRouter:
            def get_json(self, path: str) -> dict[str, object]:
                raise RuntimeError("router unavailable")

        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext("run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60)
            (ctx.output_dir / "snapshots").mkdir()
            runner._capture_round_failure(
                ctx,
                FakeKube(),
                BrokenRouter(),
                live_round=2,
                trace_round=7,
                phase="R2",
                error=RuntimeError("boom"),
                plan_name="plan-r2",
                terminal={"status": {"phase": "Failed"}},
                profile_report=None,
            )
            evidence = json.loads((ctx.output_dir / "snapshots" / "r02_failure.json").read_text())
        self.assertEqual(evidence["terminal"]["status"]["phase"], "Failed")
        self.assertEqual(evidence["registry"]["status"]["health"]["stable"], True)
        self.assertEqual(evidence["observation_errors"][0]["source"], "routes")

    def test_profile_adapter_exception_is_saved_as_raw_error_report(self) -> None:
        route = {"runtimeId": "r1", "model": "resnet50", "endpoint": "http://runtime", "active": True}
        with patch.object(runner.profile, "run_profile", side_effect=RuntimeError("profile socket failed")):
            report = runner._profile_target([route], 0.01)
        self.assertEqual(report["status"], "error")
        self.assertEqual(report["exception_type"], "RuntimeError")
        self.assertIn("profile socket failed", report["errors"][0])

    def test_profile_target_uses_vision_ten_warmups_and_llm_one_with_shared_call(self) -> None:
        routes = [
            {"runtimeId": "vision-1", "model": "resnet50", "endpoint": "http://vision", "active": True},
            {"runtimeId": "llm-1", "model": "gpt2", "endpoint": "http://llm", "active": True},
        ]
        calls: list[tuple[list[dict[str, object]], dict[str, object]]] = []

        def fake_profile(replicas: list[dict[str, object]], **kwargs: object) -> dict[str, object]:
            calls.append((replicas, kwargs))
            return {"status": "ok", "samples": [], "replicas": []}

        with patch.object(runner.profile, "run_profile", side_effect=fake_profile):
            report = runner._profile_target(routes, 0.01)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(len(calls), 1)
        warmups = {str(row["runtimeId"]): row["warmupRequests"] for row in calls[0][0]}
        self.assertEqual(warmups, {"vision-1": 10, "llm-1": 1})
        self.assertEqual(calls[0][1]["warmup_requests"], 1)

    def test_csv_writer_requires_explicit_schema_and_rejects_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.csv"
            with self.assertRaises(ValueError):
                runner._csv_output(path, [{"a": 1}])
            with self.assertRaises(ValueError):
                runner._csv_output(path, [{"a": 1, "unexpected": 2}], ["a"])

    def test_partial_round_summary_uses_fixed_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "plans").mkdir()
            ctx = runner.RunContext("run", root, "or-sim", "http://router", 1, 1, 1)
            runner._write_initial_outputs(ctx, SimpleNamespace(), {})
            runner._write_partial_round_summary(
                ctx,
                live_round=3,
                trace_round=6,
                phase="R3",
                error=RuntimeError("terminal validation failed"),
            )
            with (root / "round_summary.csv").open(encoding="utf-8", newline="") as stream:
                header = stream.readline().strip().split(",")
                row = stream.readline().strip().split(",")
            self.assertEqual(tuple(header), runner.ROUND_SUMMARY_FIELDS)
            self.assertEqual(len(row), len(runner.ROUND_SUMMARY_FIELDS))
            summary = json.loads((root / "plans" / "r03_round_summary.json").read_text())
            self.assertTrue(summary["partial"])
            self.assertFalse(summary["reached_target"])

    def test_success_and_failure_both_record_required_output_validation(self) -> None:
        for ok, allow_partial in ((True, False), (False, True)):
            with self.subTest(ok=ok), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ctx = runner.RunContext("run", root, "or-sim", "http://router", 1, 1, 1)
                calls: list[bool] = []
                with patch.object(runner, "_finalize_run_outputs"), patch.object(
                    runner.collectors,
                    "validate_required_outputs",
                    side_effect=lambda *_args, **kwargs: calls.append(kwargs["allow_partial"]) or [],
                ):
                    result = runner._validate_and_record_outputs(
                        ctx,
                        {"run_id": "run", "completed_rounds": 12 if ok else 2,
                         "expected_rounds": 12, "ok": ok, "failure": None if ok else "boom"},
                    )
                self.assertEqual(calls, [allow_partial])
                validation = json.loads((root / "output_validation.json").read_text())
                self.assertTrue(validation["ok"])
                self.assertTrue(result["artifact_validation_ok"])


class E1ModeTests(unittest.TestCase):
    def test_e1_switches_rates_per_round_tags_requests_and_runs_r13(self) -> None:
        rates = {key: 0.0 for key in runner.WORKLOAD_KEYS}
        demand = []
        for number in range(1, 13):
            row = {"round": number + 3, **rates}
            row["gpt2_p64_o64"] = 40.0 if number % 2 else 20.0
            demand.append(row)
        sent: list[dict[str, object]] = []

        def fake_transport(_url: str, timeout_s: float = 900.0):
            return lambda request: sent.append(dict(request)) or {"status": "success"}

        terminal = {"status": {"phase": "Executed", "actionStatuses": [], "transitionExecution": {"timestamps": {
            "executorStartedAt": "2026-09-29T00:00:00Z", "executorFinishedAt": "2026-09-29T00:00:01Z"}}}}

        def fake_wait_for_plan(_kube, name, *, timeout, poll_seconds, accept_planned=True):
            self.assertLessEqual(poll_seconds, 0.25)
            return {"metadata": {"name": name}, "status": {"phase": "Planned"}} if accept_planned else terminal

        class FakeKube:
            def apply(self, value): pass
            def approve(self, name): pass
            def get_json(self, resource, name=None):
                return {"items": []} if resource == "pods" else registry_fixture()

        class FakeRouter:
            def get_json(self, path): return {"routes": []}
            def wait_drained(self, timeout): return None

        args = SimpleNamespace(controller_names=["planner-controller"], placement_nodes=[], watchdog_seconds=10.0,
                               poll_seconds=2.0, stage3_variant="sw-c", e1_warmup=True)
        with tempfile.TemporaryDirectory() as directory:
            ctx = runner.RunContext("run", Path(directory), "or-sim-exp", "http://router", 60, 30, 60,
                                    e1_mode=True, dwell_seconds=0.05)
            for sub in ("snapshots", "plans"):
                (ctx.output_dir / sub).mkdir()
            runner._write_initial_outputs(ctx, args, {})
            preflight_calls = []
            with (
                patch.object(runner.traffic, "urllib_transport", fake_transport),
                patch.object(runner, "preflight", side_effect=lambda *a, **k: preflight_calls.append(k) or {"ok": True, "errors": [], "routes": {"routes": []}, "registry": registry_fixture()}),
                patch.object(runner, "wait_for_plan", side_effect=fake_wait_for_plan),
                patch.object(runner, "audit_plan", return_value={"ok": True, "errors": []}),
                patch.object(runner, "wait_independent_final_validation", return_value=({"ok": True, "errors": []}, registry_fixture(), {"routes": []})),
                patch.object(runner, "_record_round_artifacts"),
                patch.object(runner, "_record_plan_artifacts"),
                patch.object(runner, "execute_r13_cleanup", return_value={"ok": True}) as r13,
                patch.object(runner, "e1_warmup", return_value={"steps": []}) as warm,
                patch.object(runner.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="", stderr="")) as audit,
                patch.object(runner, "_validate_and_record_outputs", side_effect=lambda _ctx, result: result),
            ):
                result = runner.execute_e1_experiment(ctx, args, FakeKube(), FakeRouter(), demand, [])
            events = [json.loads(line) for line in (ctx.output_dir / "e1_rate_events.jsonl").read_text().splitlines()]
            windows = (ctx.output_dir / "e1_windows.csv").read_text().splitlines()
            requests = [json.loads(line) for line in (ctx.output_dir / "requests.jsonl").read_text().splitlines()]

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["completed_rounds"], 12)
        r13.assert_called_once()
        warm.assert_called_once()
        self.assertIn("audit_makespan_run.py", str(audit.call_args))
        self.assertEqual(len(windows), 13)  # header + 12 rounds
        self.assertEqual([e["window"] for e in events[:4]], ["transition", "steady", "transition", "steady"])
        self.assertEqual(events[-1]["window"], "stopped")
        self.assertEqual(events[2]["rates"]["gpt2_p64_o64"], 20.0)  # R2 commitment = min(40, 20)
        self.assertTrue(preflight_calls[0]["require_empty"] and not preflight_calls[0]["allow_inflight"])
        self.assertTrue(all(call["allow_inflight"] for call in preflight_calls[1:]))
        tagged = {(r["live_round"], r["phase"]) for r in requests}
        self.assertIn((2, "steady"), tagged)
        self.assertTrue(all(r["workload"] == "gpt2_p64_o64" for r in requests))
        self.assertEqual(len(sent), len(requests))


if __name__ == "__main__":
    unittest.main()
