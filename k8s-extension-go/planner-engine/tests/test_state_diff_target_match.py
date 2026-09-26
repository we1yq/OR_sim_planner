import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from migrant_core.state import ClusterState, GPUState, MigInstance
from migrant_core.transition_planner.internal.state_diff import matches_target_state


class TargetStateMatchTest(unittest.TestCase):
    def state(self, **instance_fields):
        return ClusterState(
            [GPUState(0, instances=[MigInstance(0, 7, "7g", **instance_fields)])]
        )

    def test_omitted_target_runtime_metadata_is_wildcard(self):
        observed = self.state(
            workload="gpt2_p512_o512",
            batch=1,
            runtime_model="gpt2",
            request_class="p512/o512",
            prompt_len=512,
            output_tokens=512,
        )
        target = self.state(workload="gpt2_p512_o512", batch=1)
        self.assertTrue(matches_target_state(observed, target))

    def test_explicit_target_request_class_remains_strict(self):
        observed = self.state(
            workload="gpt2",
            batch=1,
            runtime_model="gpt2",
            request_class="p64/o64",
        )
        target = self.state(
            workload="gpt2",
            batch=1,
            runtime_model="gpt2",
            request_class="p512/o512",
        )
        self.assertFalse(matches_target_state(observed, target))

    def test_geometry_workload_and_batch_are_always_strict(self):
        base = self.state(workload="gpt2_p64_o64", batch=1)
        self.assertFalse(matches_target_state(base, self.state(workload="gpt2_p512_o512", batch=1)))
        self.assertFalse(matches_target_state(base, self.state(workload="gpt2_p64_o64", batch=2)))


if __name__ == "__main__":
    unittest.main()
