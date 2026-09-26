"""Formal planner slot regression; requires a working Gurobi license."""
import os
import sys
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from migrant_core.state import ClusterState, GPUState, MigInstance
from migrant_core.target_materializer.exact_milp_builder import build_target_state_exact_milp, _extract_specific_assignments


class SlotExclusivityTest(unittest.TestCase):
    def test_extraction_rejects_duplicate_selected_slot(self):
        variables={(0,0,0,0):SimpleNamespace(X=1.),(1,0,0,0):SimpleNamespace(X=1.)}
        with self.assertRaisesRegex(RuntimeError,"Multiple demand types assigned"):
            _extract_specific_assignments(variables,[dict(batch=1),dict(batch=2)])

    def test_extraction_ignores_unselected_types(self):
        variables={(0,0,0,0):SimpleNamespace(X=0.),(1,0,0,0):SimpleNamespace(X=1.)}
        self.assertEqual(_extract_specific_assignments(variables,[dict(batch=1),dict(batch=2)]),
                         {(0,0,0):dict(batch=2)})

    def build(self, batches, old_count=1, cold=False, gpu_count=1):
        unique_batches = list(dict.fromkeys(batches))
        df = pd.DataFrame([dict(opt_idx=k,w_idx=0,workload="A",profile="1g",batch=b,mu=float(b))
                           for k,b in enumerate(unique_batches)])
        source = None if cold else ClusterState([GPUState(0,instances=[
            MigInstance(s,s+1,"1g","A" if s<old_count else None,1 if s<old_count else None,
                        mu=1. if s<old_count else 0.) for s in range(7)])],metadata={"physical_id_map": {0: "GPU-test-0"}, "free_physical_gpu_pool": ["GPU-test-1"], "source": "go-cluster-state-manager-test"})
        target = build_target_state_exact_milp(
            dict(gpu_count=gpu_count,x_sol={k:batches.count(b) for k,b in enumerate(unique_batches)},effective_option_df=df),
            prev_state=source,feasible_option_df=df,workload_names=["A"],arrival_rate=[float(sum(batches))])
        occupied=[(g.gpu_id,i) for g in target.real_gpus() for i in g.instances if i.workload is not None]
        self.assertEqual(len(occupied),len(batches))
        self.assertEqual(Counter(i.batch for g,i in occupied),Counter(batches))
        self.assertEqual(len({(g,i.start,i.end) for g,i in occupied}),len(occupied))
        self.assertAlmostEqual(sum(i.mu for g,i in occupied),sum(batches))
        self.assertTrue(target.metadata["build_metrics"]["optimality_proven"])
        if not cold:
            actual=sum(g==0 and i.start<old_count and i.end==i.start+1 and i.profile=="1g"
                       and i.workload=="A" for g,i in occupied)
            self.assertEqual(actual,min(old_count,len(batches)))
            self.assertEqual(target.metadata["build_metrics"]["exact_preserve"],actual)

    def test_two_batches_one_old_slot(self):
        self.build([1,2])

    def test_three_batches_two_old_slots(self):
        self.build([1,2,4],old_count=2)

    def test_single_batch_multiple_instances(self):
        self.build([1,1,1],old_count=2)

    def test_cold_start_multiple_batches(self):
        self.build([1,2,4],cold=True)

    def test_batch_change_still_preserves_workload(self):
        self.build([2])

    def test_multiple_batches_with_new_gpu(self):
        self.build(list(range(1,9)),gpu_count=2)

    def test_native_and_upgrade_types_use_distinct_slots(self):
        df=pd.DataFrame([dict(opt_idx=k,w_idx=0,workload="A",profile=p,batch=1,mu=mu)
                         for k,(p,mu) in enumerate([("3g",3.),("4g",4.)])])
        source=ClusterState([GPUState(0,instances=[MigInstance(0,4,"4g","A",1,mu=4.),
                                                  MigInstance(4,7,"3g")])],metadata={"physical_id_map": {0: "GPU-test-0"}, "free_physical_gpu_pool": ["GPU-test-1"], "source": "go-cluster-state-manager-test"})
        target=build_target_state_exact_milp(dict(gpu_count=1,x_sol={0:1,1:1},effective_option_df=df),
            prev_state=source,feasible_option_df=df,workload_names=["A"],arrival_rate=[7.])
        occupied=[i for g in target.real_gpus() for i in g.instances if i.workload is not None]
        self.assertEqual(sorted(i.profile for i in occupied),["3g","4g"])
        self.assertEqual(sum(i.mu for i in occupied),7.)
        self.assertEqual(target.metadata["build_metrics"]["exact_preserve"],1)


if __name__ == "__main__":
    unittest.main()
