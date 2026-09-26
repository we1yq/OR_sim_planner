"""Capacity accounting for route-preserving runtime batch updates."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from migrant_core.state import ClusterState, GPUState, MigInstance
from migrant_core.transition_planner.effect_aware_dag import _effects_for_action, run


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


if __name__ == "__main__":
    unittest.main()
