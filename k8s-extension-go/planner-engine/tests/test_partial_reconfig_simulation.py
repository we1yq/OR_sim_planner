import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from migrant_core.physical_ids import bootstrap_physical_ids_for_state, ensure_state_metadata, get_physical_id
from migrant_core.state import ClusterState, GPUState, MigInstance
from migrant_core.transition_planner.internal.action_simulator import simulate_transition_actions


class PartialReconfigSimulationTest(unittest.TestCase):
    def test_preserved_slot_activation_before_partial_create_does_not_duplicate_slots(self):
        source = ClusterState(gpus=[GPUState(gpu_id=0, instances=[
            MigInstance(0, 2, "2g", "llama", 1, mu=1.0),
            MigInstance(2, 4, "2g", "llama", 1, mu=1.0),
            MigInstance(4, 7, "3g", "resnet", 4, mu=10.0)])])
        target = ClusterState(gpus=[GPUState(gpu_id=0, instances=[
            MigInstance(0, 1, "1g", "gpt2", 1, mu=0.5),
            MigInstance(1, 2, "1g", "gpt2", 1, mu=0.5),
            MigInstance(2, 3, "1g", "gpt2", 1, mu=0.5),
            MigInstance(3, 4, "1g", "gpt2", 1, mu=0.5),
            MigInstance(4, 7, "3g", "resnet", 16, mu=20.0)])])
        ensure_state_metadata(source)
        bootstrap_physical_ids_for_state(source)
        physical = get_physical_id(source, 0)
        target.metadata = {"physical_id_map": {0: physical}}
        ones = [(i, i + 1, "1g") for i in range(4)]
        actions = [
            {"type": "delete_instance", "gpu_id": 0, "physical_gpu_id": physical, "slot": (0, 2, "2g"), "partial": True},
            {"type": "activate_instance_route", "gpu_id": 0, "physical_gpu_id": physical, "slot": (4, 7, "3g"),
             "partialContextRoot": "PARTIAL_RECONF_gpu0"},
            {"type": "delete_instance", "gpu_id": 0, "physical_gpu_id": physical, "slot": (2, 4, "2g"), "partial": True},
            {"type": "configure_partial_profile", "gpu_id": 0, "physical_gpu_id": physical,
             "deleteSlots": [(0, 2, "2g"), (2, 4, "2g")], "createSlots": ones, "partial": True},
        ]
        executed = simulate_transition_actions(source_state=source, target_state=target,
                                               fine_actions=actions, next_physical_idx=1)
        slots = sorted((i.start, i.end, i.profile) for i in executed.real_gpus()[0].instances)
        self.assertEqual(slots, sorted(ones + [(4, 7, "3g")]))


if __name__ == "__main__":
    unittest.main()
