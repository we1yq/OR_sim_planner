"""Capacity accounting for route-preserving runtime batch updates."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from migrant_core.state import ClusterState, GPUState, MigInstance
from migrant_core.transition_planner.effect_aware_dag import (
    _effects_for_action,
    _remove_capacity_dependency_edges,
    run,
)


class BatchCapacityEffectsTest(unittest.TestCase):
    def effects(self, action_type, old_mu, new_mu):
        source = {0: GPUState(0, instances=[MigInstance(0, 1, "1g", "A", 1, mu=old_mu)])}
        target = {0: GPUState(0, instances=[MigInstance(0, 1, "1g", "A", 4, mu=new_mu)])}
        return _effects_for_action(
            {"type": action_type, "gpu_id": 0, "slot": (0, 1, "1g"),
             "transitionMode": "batch_change"},
            source, target, {"A": 5.0},
        )

    def test_gain_only_credited_at_route_confirmation(self):
        self.assertNotIn("producesCapacity", self.effects("apply_batch", 5.0, 8.0))
        self.assertNotIn("producesCapacity", self.effects("verify_batch", 5.0, 8.0))
        self.assertEqual(self.effects("activate_instance_route", 5.0, 8.0)["producesCapacity"],
                         [{"workload": "A", "mu": 3.0}])

    def test_decrease_is_gated_before_apply(self):
        effects = self.effects("apply_batch", 8.0, 5.0)
        self.assertEqual(effects["consumesCapacity"], [{"workload": "A", "mu": 3.0}])
        self.assertIn("capacityGate", effects)
        self.assertNotIn("producesCapacity", self.effects("verify_batch", 8.0, 5.0))

    def test_equal_throughput_has_no_capacity_delta(self):
        for action in ("apply_batch", "verify_batch"):
            self.assertEqual(self.effects(action, 5.0, 5.0), {})

    def test_batch_updates_reach_target_without_route_restart(self):
        for old_mu, new_mu in ((5.0, 8.0), (8.0, 5.0)):
            with self.subTest(old_mu=old_mu):
                def state(batch, mu):
                    return ClusterState([GPUState(0, instances=[
                        MigInstance(0, 7, "7g", "A", batch, mu=mu)
                    ])], metadata={"physical_id_map": {0: "GPU-test-0"},
                                   "source": "go-cluster-state-manager-test"})
                result = run(source_state=state(1, old_mu), target_state=state(4, new_mu),
                             src_arrival={"A": old_mu}, tgt_arrival={"A": new_mu})
                self.assertTrue(result["reached_target"])
                actions = result["executed_actions"]
                types = [action["type"] for action in actions]
                self.assertIn("verify_batch", types)
                self.assertNotIn("deactivate_instance_route", types)
                self.assertGreater(types.index("activate_instance_route"), types.index("verify_batch"))
                if new_mu > old_mu:
                    produced = [record["mu"] for action in actions
                                for record in action.get("producesCapacity", [])]
                    self.assertEqual(produced, [new_mu - old_mu])

    def test_sw_c_removes_only_capacity_gate_dependencies(self):
        actions = [
            {"actionKey": "producer", "type": "activate_instance_route"},
            {"actionKey": "resource", "type": "wait_for_resource"},
            {
                "actionKey": "consumer",
                "type": "deactivate_instance_route",
                "dependsOnActionKeys": ["resource", "producer"],
                "capacityGate": {
                    "selectedProducerActionKeys": {"A": ["producer"]},
                },
            },
        ]
        action_keys = [action["actionKey"] for action in actions]
        removed = _remove_capacity_dependency_edges(actions)
        self.assertEqual(removed, {"capacityGate": 1, "temporaryCapacityCleanup": 0})
        self.assertEqual([action["actionKey"] for action in actions], action_keys)
        self.assertEqual(actions[2]["dependsOnActionKeys"], ["resource"])

    def test_sw_c_removes_temporary_capacity_cleanup_ordering(self):
        actions = [
            {"actionKey": "final-activate", "type": "activate_instance_route"},
            {"actionKey": "slot-resource", "type": "place_instance"},
            {
                "actionKey": "temp-cleanup",
                "type": "deactivate_instance_route",
                "cleanupTemporaryCapacity": True,
                "dependsOnActionKeys": ["final-activate", "slot-resource"],
                "temporaryCapacityCleanupDependsOn": ["final-activate"],
            },
        ]
        removed = _remove_capacity_dependency_edges(actions)
        self.assertEqual(removed, {"capacityGate": 0, "temporaryCapacityCleanup": 1})
        self.assertEqual(actions[2]["dependsOnActionKeys"], ["slot-resource"])

    def test_sw_c_keeps_target_and_action_multiset(self):
        def state(gpu_id, physical_id):
            return ClusterState(
                [GPUState(gpu_id, source="real", instances=[
                    MigInstance(0, 7, "7g", "A", 1, mu=5.0),
                ])],
                metadata={
                    "physical_id_map": {gpu_id: physical_id},
                    "source": "go-cluster-state-manager-test",
                    "free_physical_gpu_pool": ["GPU-a", "GPU-b"],
                },
            )

        def kwargs():
            return {
                "source_state": state(0, "GPU-a"),
                "target_state": state(1, "GPU-b"),
                "src_arrival": {"A": 5.0},
                "tgt_arrival": {"A": 5.0},
            }

        sw = run(**kwargs())
        sw_c = run(**kwargs(), stage3_variant="sw-c")
        self.assertTrue(sw["reached_target"] and sw_c["reached_target"])
        self.assertCountEqual(
            [action["actionKey"] for action in sw["executed_actions"]],
            [action["actionKey"] for action in sw_c["executed_actions"]],
        )
        self.assertGreater(sw_c["removed_capacity_dependency_count"], 0)
        self.assertFalse(sw_c["capacity_dependencies_enforced"])


if __name__ == "__main__":
    unittest.main()
