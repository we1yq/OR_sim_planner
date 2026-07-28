from real_8gpu_k8s_experiment import TrafficDriver, p95_slo_metrics_for_stage, transition_metric_row, vision_batches_for_routes


def test_traffic_uses_min_during_transition_and_target_during_steady() -> None:
    driver = TrafficDriver(
        router="http://unused",
        source={
            "resnet50_image": 0.0,
            "vgg16_image": 0.0,
            "vit_base_image": 0.0,
            "gpt2_p64_o64": 0.8,
            "gpt2_p512_o512": 0.0,
            "llama_p1024_o128": 0.2,
            "llama_p2048_o64": 0.0,
        },
        target={
            "resnet50_image": 100.0,
            "vgg16_image": 0.0,
            "vit_base_image": 0.0,
            "gpt2_p64_o64": 0.3,
            "gpt2_p512_o512": 0.0,
            "llama_p1024_o128": 0.5,
            "llama_p2048_o64": 0.0,
        },
        stage="test",
        request_rows=[],
        route_rows=[],
        sample_interval_s=1.0,
        poll_s=1.0,
        infer_timeout_s=1.0,
    )

    assert driver.phase() == "transition"
    assert driver.effective_rates() == {
        "resnet50_image": 0.0,
        "vgg16_image": 0.0,
        "vit_base_image": 0.0,
        "gpt2_p64_o64": 0.3,
        "gpt2_p512_o512": 0.0,
        "llama_p1024_o128": 0.2,
        "llama_p2048_o64": 0.0,
    }
    driver.enter_steady()
    assert driver.phase() == "steady"
    assert driver.effective_rates() == {
        "resnet50_image": 100.0,
        "vgg16_image": 0.0,
        "vit_base_image": 0.0,
        "gpt2_p64_o64": 0.3,
        "gpt2_p512_o512": 0.0,
        "llama_p1024_o128": 0.5,
        "llama_p2048_o64": 0.0,
    }


def test_transition_metrics_follow_executor_schema() -> None:
    plan = {
        "spec": {
            "actionCount": 2,
            "summary": {"plannerMakespanSec": 0.25, "sourceGpuCount": 1, "targetGpuCount": 2},
        },
        "status": {
            "phase": "Executed",
            "transitionExecution": {
                "durationsSeconds": {"total": 4.5},
                "metrics": {
                    "finalValidation": {"ok": True},
                    "actionSummary": {
                        "totalDagNodes": 8,
                        "reconfigurationNodes": 2,
                        "createdInstanceCount": 3,
                        "deletedInstanceCount": 1,
                        "createdMIGSlotCount": 4,
                        "deletedMIGSlotCount": 2,
                    },
                    "routerSLO": {
                        "startedAt": "start",
                        "finishedAt": "finish",
                        "models": {
                            "llama_p1024_o128": {
                                "requests": 2,
                                "errors": 0,
                                "latencyViolationCount": 1,
                                "latencySLOViolationSeconds": 0.2,
                                "firstViolationAt": "2026-07-10T00:00:01Z",
                                "lastViolationAt": "2026-07-10T00:00:03Z",
                            },
                            "gpt2_p64_o64": {
                                "requests": 3,
                                "errors": 1,
                                "latencyViolationCount": 2,
                                "latencySLOViolationSeconds": 0.4,
                                "firstViolationAt": "2026-07-10T00:00:02Z",
                                "lastViolationAt": "2026-07-10T00:00:05Z",
                            },
                        },
                    },
                },
            },
        },
    }

    row = transition_metric_row(0, "plan-test", plan)
    assert row["transitionMakespanSec"] == 4.5
    assert row["actionCount"] == 6
    assert row["physicalActionCount"] == 6
    assert row["dagNodeCount"] == 2
    assert row["podCreateCount"] == 3
    assert row["podDeleteCount"] == 1
    assert row["migReconfigOpCount"] == 2
    assert row["migPartitionCreateCount"] == 4
    assert row["migPartitionDeleteCount"] == 2
    assert row["sloViolationDurationSec"] == 4.0
    assert row["sloViolationExcessSec"] == 0.6
    assert row["sloViolationCount"] == 3
    assert row["transitionRequestCount"] == 5
    assert row["transitionErrorCount"] == 1
    assert row["finalValidationOk"] is True


def test_p95_slo_duration_uses_transition_buckets() -> None:
    requests = []
    for i, latency in enumerate([10, 20, 30, 40, 120]):
        requests.append({"stage": "epoch-a", "phase": "transition", "model": "resnet50_image", "sentAt": 1000.1 + i * 0.1, "latencyMs": latency})
    for i, latency in enumerate([10, 20, 30, 40, 50]):
        requests.append({"stage": "epoch-a", "phase": "transition", "model": "resnet50_image", "sentAt": 1001.1 + i * 0.1, "latencyMs": latency})
    requests.append({"stage": "epoch-a", "phase": "steady", "model": "resnet50_image", "sentAt": 1002.1, "latencyMs": 500})

    metrics = p95_slo_metrics_for_stage("epoch-a", requests, bucket_seconds=1.0)

    assert metrics["sloViolationDurationSec"] == 1.0
    assert metrics["sloViolationP95BucketSec"] == 1.0


def test_vision_batches_follow_endpoint_capacity_and_batch_size() -> None:
    batches = vision_batches_for_routes(
        1000,
        [
            {"active": True, "acceptingNew": True, "capacity": 600, "batchSize": 32},
            {"active": True, "acceptingNew": True, "capacity": 300, "batchSize": 16},
            {"active": True, "acceptingNew": True, "capacity": 100, "batchSize": 64},
        ],
    )

    assert sum(batches) == 1000
    assert len(batches) == 40
    assert batches[:19] == [32] * 18 + [24]
    assert batches[19:38] == [16] * 18 + [12]
    assert batches[38:] == [64, 36]


def test_slo_request_rate_counts_logical_requests() -> None:
    requests = [
        {
            "stage": "epoch-a",
            "phase": "transition",
            "model": "resnet50_image",
            "sentAt": 1000.1,
            "serviceLatencyMs": 120,
            "logicalRequestCount": 32,
            "ok": True,
            "status": 200,
        },
        {
            "stage": "epoch-a",
            "phase": "transition",
            "model": "resnet50_image",
            "sentAt": 1000.2,
            "serviceLatencyMs": 80,
            "logicalRequestCount": 8,
            "ok": True,
            "status": 200,
        },
    ]

    metrics = p95_slo_metrics_for_stage("epoch-a", requests, bucket_seconds=1.0)

    assert metrics["sloTransitionRequestCount"] == 40
    assert metrics["sloViolationRequestCount"] == 32
    assert metrics["sloViolationRate"] == 0.8


def test_vision_batching_accepts_real_batch_size_one() -> None:
    batches = vision_batches_for_routes(
        1000,
        [{"active": True, "acceptingNew": True, "capacity": 1000, "batchSize": 1}],
    )

    assert sum(batches) == 1000
    assert len(batches) == 1000
    assert set(batches) == {1}


def test_vision_batching_rejects_missing_real_batch_size() -> None:
    try:
        vision_batches_for_routes(
            1000,
            [{"active": True, "acceptingNew": True, "capacity": 1000}],
        )
    except RuntimeError as exc:
        assert "missing a real batch size" in str(exc)
    else:
        raise AssertionError("expected missing real batch size to fail")


def test_vision_batching_rejects_missing_active_route() -> None:
    try:
        vision_batches_for_routes(1000, [])
    except RuntimeError as exc:
        assert "no active route" in str(exc)
    else:
        raise AssertionError("expected missing active route to fail")
