import argparse
from collections import Counter

from run_action_latency_microbench import (
    RUNTIME_HOST_PORT_POOL,
    build_matrix,
    partial_source_spec,
    runtime_host_port,
    slot_resource_name,
)


def args(**overrides):
    base = {
        "suite": "all",
        "profiles": "1g,2g,3g,4g,7g",
        "instance_profiles": "1g,2g,3g,4g,7g",
        "templates": "7,4+3,4+2+1,4+1+1+1,3+3,3+2+1,3+1+1+1,2+2+3,3+2+1+1,3+1+1+1+1,2+2+2+1,2+2+1+1+1,2+1+1+1+1+1,1+1+1+1+1+1+1",
        "workloads": "resnet50,vgg16,vit_base,gpt2_p64_o64,gpt2_p512_o512,llama_p1024_o128,llama_p2048_o64,llama_p4096_o512",
        "skip_partial": False,
        "max_partial_cases": 0,
        "shard_count": 1,
        "shard_index": 0,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_full_matrix_covers_mig_instance_and_route_cases():
    matrix = build_matrix(args())
    counts = Counter(case.action_type for case in matrix)

    assert counts["create_mig"] == 5
    assert counts["delete_mig"] == 5
    assert counts["configure_full_template"] == 14
    assert counts["clear_template"] == 14
    assert counts["partial_reconfig"] > 0
    assert counts["create_instance"] == 8 * 5
    assert counts["delete_instance"] == 8 * 5
    assert counts["route_activate"] == 8 * 5
    assert counts["route_deactivate_drain"] == 8 * 5


def test_partial_source_spec_uses_patch_preserve_and_delete_slots():
    matrix = build_matrix(args(max_partial_cases=1))
    case = next(item for item in matrix if item.action_type == "partial_reconfig")

    source_spec = partial_source_spec(case)

    assert case.delete_spec
    assert case.preserve_spec
    for part in case.delete_spec.split(",") + case.preserve_spec.split(","):
        assert part in source_spec.split(",")


def test_runtime_host_port_matches_planner_pool_shape():
    slot0 = slot_resource_name("ampere-gpu0", 0, 1, "1g")
    slot4 = slot_resource_name("ampere-gpu1", 4, 8, "3g")

    assert runtime_host_port("ampere-gpu0", slot0) == RUNTIME_HOST_PORT_POOL[0]
    assert runtime_host_port("ampere-gpu1", slot4) == RUNTIME_HOST_PORT_POOL[11]
    assert 10684 not in RUNTIME_HOST_PORT_POOL
    assert 10690 not in RUNTIME_HOST_PORT_POOL


def test_shards_partition_matrix_without_overlap():
    full = build_matrix(args())
    shards = [build_matrix(args(shard_count=3, shard_index=i)) for i in range(3)]
    shard_ids = [{case.case_id for case in shard} for shard in shards]

    assert sum(len(shard) for shard in shards) == len(full)
    assert shard_ids[0].isdisjoint(shard_ids[1])
    assert shard_ids[0].isdisjoint(shard_ids[2])
    assert shard_ids[1].isdisjoint(shard_ids[2])
    assert set().union(*shard_ids) == {case.case_id for case in full}
