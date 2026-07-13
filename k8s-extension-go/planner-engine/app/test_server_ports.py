from server import RUNTIME_HOST_PORT_POOL, assign_runtime_host_ports


def test_runtime_ports_skip_node_services_and_reuse_across_nodes() -> None:
    runtimes = [
        {
            "node": "ampere",
            "gpu": "ampere-gpu0",
            "slotResource": f"or-sim.io/ampere-gpu0-s{i}-{i + 1}-1g",
            "model": f"model-{i}",
        }
        for i in range(5)
    ]
    runtimes.append(
        {
            "node": "rtx1-worker",
            "gpu": "rtx1-worker-gpu0",
            "slotResource": "or-sim.io/rtx1-worker-gpu0-s0-1-1g",
            "model": "other-node",
        }
    )

    assign_runtime_host_ports(runtimes)

    ampere_ports = [row["hostPort"] for row in runtimes if row["node"] == "ampere"]
    assert ampere_ports == [RUNTIME_HOST_PORT_POOL[i] for i in range(5)]
    assert 10684 not in ampere_ports
    assert 10690 not in ampere_ports
    assert runtimes[-1]["hostPort"] == RUNTIME_HOST_PORT_POOL[0]


def test_runtime_ports_are_unique_across_gpus_on_same_node() -> None:
    runtimes = [
        {
            "node": "ampere",
            "gpu": "ampere-gpu0",
            "slotResource": "or-sim.io/ampere-gpu0-s0-1-1g",
            "model": "gpt2",
        },
        {
            "node": "ampere",
            "gpu": "ampere-gpu1",
            "slotResource": "or-sim.io/ampere-gpu1-s0-1-1g",
            "model": "gpt2",
        },
        {
            "node": "ampere",
            "gpu": "ampere-gpu1",
            "slotResource": "or-sim.io/ampere-gpu1-s4-8-3g",
            "model": "llama",
        },
    ]

    assign_runtime_host_ports(runtimes)

    assert runtimes[0]["hostPort"] == RUNTIME_HOST_PORT_POOL[0]
    assert runtimes[1]["hostPort"] == RUNTIME_HOST_PORT_POOL[7]
    assert runtimes[2]["hostPort"] == RUNTIME_HOST_PORT_POOL[11]
    assert len({row["hostPort"] for row in runtimes}) == len(runtimes)
