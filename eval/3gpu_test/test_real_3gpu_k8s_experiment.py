from real_3gpu_k8s_experiment import TrafficDriver, p95_slo_metrics_for_stage, transition_metric_row


def test_traffic_uses_min_during_transition_and_target_during_steady() -> None:
    driver = TrafficDriver(
        router="http://unused",
        source={"llama": 0.2, "gpt2": 0.8, "resnet50": 0.0},
        target={"llama": 0.5, "gpt2": 0.3, "resnet50": 100.0},
        stage="test",
        request_rows=[],
        route_rows=[],
        sample_interval_s=1.0,
        poll_s=1.0,
        infer_timeout_s=1.0,
    )

    assert driver.phase() == "transition"
    assert driver.effective_rates() == {"llama": 0.2, "gpt2": 0.3, "resnet50": 0.0}
    driver.enter_steady()
    assert driver.phase() == "steady"
    assert driver.effective_rates() == {"llama": 0.5, "gpt2": 0.3, "resnet50": 100.0}


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
                    "routerSLO": {
                        "startedAt": "start",
                        "finishedAt": "finish",
                        "models": {
                            "llama": {
                                "requests": 2,
                                "errors": 0,
                                "latencyViolationCount": 1,
                                "latencySLOViolationSeconds": 0.2,
                                "firstViolationAt": "2026-07-10T00:00:01Z",
                                "lastViolationAt": "2026-07-10T00:00:03Z",
                            },
                            "gpt2": {
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
    assert row["sloViolationDurationSec"] == 4.0
    assert row["sloViolationExcessSec"] == 0.6
    assert row["sloViolationCount"] == 3
    assert row["transitionRequestCount"] == 5
    assert row["transitionErrorCount"] == 1
    assert row["finalValidationOk"] is True


def test_p95_slo_duration_uses_transition_buckets() -> None:
    requests = []
    for i, latency in enumerate([10, 20, 30, 40, 120]):
        requests.append({"stage": "epoch-a", "phase": "transition", "model": "resnet50", "sentAt": 1000.1 + i * 0.1, "latencyMs": latency})
    for i, latency in enumerate([10, 20, 30, 40, 50]):
        requests.append({"stage": "epoch-a", "phase": "transition", "model": "resnet50", "sentAt": 1001.1 + i * 0.1, "latencyMs": latency})
    requests.append({"stage": "epoch-a", "phase": "steady", "model": "resnet50", "sentAt": 1002.1, "latencyMs": 500})

    metrics = p95_slo_metrics_for_stage("epoch-a", requests, bucket_seconds=1.0)

    assert metrics["sloViolationDurationSec"] == 1.0
    assert metrics["sloViolationP95BucketSec"] == 1.0
