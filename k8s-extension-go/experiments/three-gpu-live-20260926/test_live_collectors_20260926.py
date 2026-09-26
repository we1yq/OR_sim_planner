"""Fixture-only unit tests for live_collectors_20260926."""

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("live_collectors_20260926.py")
SPEC = importlib.util.spec_from_file_location("live_collectors_20260926", MODULE_PATH)
collectors = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(collectors)


T0 = "2026-09-26T00:00:00Z"
T1 = "2026-09-26T00:00:01Z"
T2 = "2026-09-26T00:00:02Z"
T3 = "2026-09-26T00:00:03Z"


CATALOG = [{"workload": "A", "profile": "1g", "batch": 1, "mu": 10.0}]


def plan_fixture():
    return {
        "metadata": {"name": "plan-1"},
        "spec": {"actions": [{"id": "a1", "type": "place_instance", "category": "create_instance"}]},
        "status": {
            "phase": "Executed",
            "finalValidation": {"valid": True, "finishedAt": T3},
            "transitionExecution": {
                "timestamps": {"executorStartedAt": T0},
                "durationsSeconds": {"total": 3.0},
            },
            "actionStatuses": [{
                "id": "a1",
                "status": "completed",
                "attempt": 2,
                "startedAt": T0,
                "finishedAt": T2,
                "timestamps": {"runtimeDeploymentCreateStartedAt": T0},
                "durationsSeconds": {"runtimeDeploymentCreate": 2.0},
                "attempts": [
                    {"attempt": 1, "status": "failed", "error": "temporary"},
                    {"attempt": 2, "status": "completed"},
                ],
                "errors": ["temporary"],
            }],
        },
    }


class LiveCollectorsTest(unittest.TestCase):
    def test_flatten_preserves_nested_timing_attempts_and_errors(self):
        row = collectors.flatten_mig_action_plan(plan_fixture())[0]
        self.assertEqual(row["plan_id"], "plan-1")
        self.assertEqual(row["action_id"], "a1")
        self.assertEqual(row["attempt"], 2)
        self.assertEqual(row["duration_runtimeDeploymentCreate"], 2.0)
        self.assertEqual(len(row["attempts"]), 2)
        self.assertEqual(row["attempt_errors"][0]["error"], "temporary")
        self.assertEqual(row["errors"], ["temporary"])

    def test_flatten_reads_go_action_dag_nodes_and_global_execution_bounds(self):
        plan = {
            "metadata": {"name": "go-plan"},
            "spec": {"actionDag": {"nodes": [{
                "id": "a0", "dependsOn": [], "action": {"type": "allocate_gpu"},
            }]}},
            "status": {
                "actionStatuses": [{"id": "a0", "type": "allocate_gpu",
                                     "status": "completed", "durationSeconds": 1.5}],
                "transitionExecution": {"timestamps": {
                    "executorStartedAt": T0, "executorFinishedAt": T2,
                }},
            },
        }
        rows = collectors.flatten_mig_action_plan(plan)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["action_type"], "allocate_gpu")
        summary = collectors.summarize_round(rows)
        self.assertEqual(summary["makespan_seconds"], 2.0)

    def test_lifecycle_uses_executor_metrics_and_marks_missing_events(self):
        rows = collectors.build_replica_lifecycle_rows([{
            "workload": "A", "runtimeId": "r1", "profile": "1g", "batch": 1,
            "executorMetrics": {
                "runtimeDeploymentCreateStartedAt": T0,
                "runtimeDeploymentCreatedAt": T1,
                "runtimeReadyAt": T2,
                "routeSyncedAt": T3,
            },
        }])
        row = rows[0]
        self.assertEqual(row["deployment_create_started_at"], T0)
        self.assertEqual(row["pod_ready_at"], T2)
        self.assertIsNone(row["route_stop_accepting_effective_at"])
        self.assertEqual(row["route_stop_accepting_effective_at_reason"], "event_not_observed")
        self.assertEqual(row["create_to_route_seconds"], 3.0)

    def test_action_runtime_identity_matches_planner_runtime_id_and_events_join(self):
        plan = {
            "metadata": {"name": "p"},
            "spec": {"actionDag": {"nodes": [{
                "id": "a", "type": "activate_instance_route",
                "action": {"type": "activate_instance_route", "workload": "gpt2_p64_o64",
                           "physical_gpu_id": "ampere-gpu0", "slot": [0, 1, "1g"]},
            }]}},
            "status": {"actionStatuses": [{
                "id": "a", "status": "completed",
                "trace": {"timestamps": {"runtimeReadyAndCUDAVerifiedAt": T1, "routeSyncedAt": T2}},
            }]},
        }
        action = collectors.flatten_action_plan(plan)[0]
        self.assertEqual(action["runtime_id"], "gpt2-p64-o64-ampere-gpu0-s0-1-1g")
        rows = collectors.build_replica_lifecycle_rows(
            [{"runtimeId": action["runtime_id"], "workload": "gpt2_p64_o64"}],
            [action],
            [{"runtime_id": action["runtime_id"], "event_type": "route_activation", "timestamp": T2}],
        )
        self.assertEqual(rows[0]["route_activation_ack_at"], T2)

    def test_gpu_events_and_intervals_keep_unknown_time_unknown(self):
        events = collectors.build_gpu_events([{
            "action_id": "alloc", "action_type": "allocate_gpu", "physical_gpu_id": "G1",
            "status": "completed", "finished_at": T1,
        }, {
            "action_id": "return", "action_type": "return_gpu", "physical_gpu_id": "G1",
            "status": "completed",
        }], initial_gpu_ids=["G0"])
        self.assertEqual([row["event_type"] for row in events], ["baseline", "acquire", "release"])
        intervals = collectors.gpu_count_intervals(events)
        self.assertEqual(intervals[0]["active_gpu_count"], 1)
        self.assertEqual(intervals[-1]["active_gpu_count"], 2)
        self.assertIsNone(events[-1]["timestamp"])

    def test_capacity_timeline_requires_ready_and_route_and_uses_frozen_mu(self):
        rows = collectors.derive_capacity_timeline(CATALOG, [{
            "workload": "A", "runtime_id": "r1", "profile": "1g", "batch": 1,
            "pod_ready_at": T1, "route_activation_ack_at": T2,
            "route_stop_accepting_effective_at": T3,
        }, {
            "workload": "A", "runtime_id": "r2", "profile": "1g", "batch": 1,
            "pod_ready_at": T1,
        }])
        timed = [row for row in rows if row["timestamp"]]
        self.assertEqual(timed[0]["event_type"], "route_ready_add")
        self.assertEqual(timed[0]["mu"], 10.0)
        self.assertEqual(timed[-1]["event_type"], "stop_accepting_remove")
        missing = [row for row in rows if row["event_type"] == "capacity_add_unproven"]
        self.assertEqual(missing[0]["reason"], "missing_ready_or_route_activation")

    def test_capacity_event_log_can_supply_lifecycle_and_batch_delta(self):
        rows = collectors.derive_capacity_timeline(
            CATALOG,
            runtime_events=[
                {"event_type": "pod_ready", "runtime_id": "r1", "workload": "A",
                 "profile": "1g", "batch": 1, "timestamp": T1},
                {"event_type": "route_active", "runtime_id": "r1", "timestamp": T2},
                {"event_type": "batch_effective", "runtime_id": "r1", "workload": "A",
                 "profile": "1g", "old_batch": 1, "new_batch": 1,
                 "old_mu": 10.0, "new_mu": 12.0, "timestamp": T3},
            ],
        )
        self.assertEqual(rows[0]["event_type"], "route_ready_add")
        self.assertEqual(rows[-1]["event_type"], "batch_effective")
        self.assertEqual(rows[-1]["delta"], 2.0)

    def test_capacity_results_and_round_summary(self):
        results = collectors.compute_capacity_results(
            CATALOG,
            [{"workload": "A", "runtime_id": "r1", "profile": "1g", "batch": 1}],
            [{"workload": "A", "runtime_id": "r1", "sample_count": 20, "inference_seconds": 2.0}],
            {"A": 8.0},
        )
        self.assertEqual(results[0]["D"], 8.0)
        self.assertEqual(results[0]["C_pred"], 10.0)
        self.assertEqual(results[0]["C_measured"], 10.0)
        self.assertEqual(results[0]["C_measured_over_D"], 1.25)
        summary = collectors.summarize_round(
            collectors.flatten_mig_action_plan(plan_fixture()),
            final_validation={"valid": True, "finishedAt": T3},
            gpu_intervals=[{"active_gpu_count": 2}], gpu_capacity=3,
        )
        self.assertEqual(summary["makespan_seconds"], 3.0)
        self.assertEqual(summary["peak_active_gpu_count"], 2)
        self.assertEqual(summary["headroom"], 1)
        self.assertEqual(summary["action_counts"]["by_status"]["completed"], 1)

    def test_capacity_results_reject_incomplete_profile_samples(self):
        results = collectors.compute_capacity_results(
            CATALOG,
            [{"workload": "A", "runtime_id": "r1", "profile": "1g", "batch": 1}],
            [
                {"workload": "A", "runtime_id": "r1", "logicalSamples": 10,
                 "runtimeInferenceSeconds": 1.0, "complete": True},
                {"workload": "A", "runtime_id": "r1", "logicalSamples": 1000,
                 "runtimeInferenceSeconds": 1.0, "complete": False},
            ],
            {"A": 8.0},
        )
        self.assertIsNone(results[0]["C_measured"])
        self.assertFalse(results[0]["complete"])
        self.assertEqual(results[0]["replicas"][0]["discarded_incomplete_samples"], 1)

    def test_standard_library_io_and_schema_validator(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            collectors.write_json(root / "round_summary.json", {
                "finalValidation": {}, "makespan_seconds": 1, "action_counts": {},
            })
            collectors.write_jsonl(root / "actions.jsonl", [{
                "plan_id": "p", "action_id": "a", "action_type": "x", "status": "completed",
            }])
            errors = collectors.validate_required_outputs({
                "round_summary.json": root / "round_summary.json",
                "actions.jsonl": root / "actions.jsonl",
            }, {
                "round_summary.json": ("finalValidation", "makespan_seconds", "action_counts"),
                "actions.jsonl": ("plan_id", "action_id", "action_type", "status"),
            })
            self.assertEqual(errors, [])

    def test_write_csv_requires_stable_fields_and_rejects_unknown_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.csv"
            with self.assertRaisesRegex(ValueError, "explicit fieldnames"):
                collectors.write_csv(path, [{"a": 1}])
            with self.assertRaisesRegex(ValueError, "unknown fields: extra"):
                collectors.write_csv(path, [{"a": 1, "extra": 2}], ["a"])
            collectors.write_csv(path, [{"a": 1}], ["a"])
            self.assertEqual(path.read_text(encoding="utf-8").splitlines(), ["a", "1"])

    def test_validator_checks_every_row_and_round_coverage(self):
        valid_rows = [{"live_round": n, "required": "ok"} for n in range(1, 4)]
        errors = collectors.validate_required_outputs(
            {"rows.jsonl": valid_rows[:1] + [{"live_round": 2}]},
            {"rows.jsonl": ("live_round", "required")},
            expected_rounds=3,
        )
        self.assertTrue(any("row 2 missing fields required" in error for error in errors))
        self.assertTrue(any("missing rounds 3" in error for error in errors))

    def test_validator_allows_explicit_partial_execution_but_reports_empty_evidence_without_it(self):
        schema = {"rows.jsonl": ("live_round", "required")}
        self.assertTrue(collectors.validate_required_outputs({"rows.jsonl": []}, schema))
        self.assertEqual(
            collectors.validate_required_outputs({"rows.jsonl": []}, schema, allow_partial=True), []
        )


if __name__ == "__main__":
    unittest.main()
