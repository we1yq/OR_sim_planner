package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"or-sim/k8s-extension-go/internal/system"
)

func TestExecutionGate(t *testing.T) {
	for _, gate := range []string{"", "auto", "approved"} {
		if !executionGateOpen(gate) {
			t.Fatalf("gate %q should permit execution", gate)
		}
	}
	for _, gate := range []string{"manual", "hold", "blocked"} {
		if executionGateOpen(gate) {
			t.Fatalf("gate %q should hold execution", gate)
		}
	}
}

func TestMissingRouteAlreadyDeactivatedAndDrained(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/routes" {
			t.Fatalf("unexpected router request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"routes":[]}`))
	}))
	defer server.Close()

	action := map[string]any{
		"type":            "deactivate_instance_route",
		"workload":        "resnet50",
		"physical_gpu_id": "ampere-gpu0",
		"slot":            []any{3, 4, "1g"},
	}
	if err := markRouteDrainingForAction(server.URL, action); err != nil {
		t.Fatalf("missing route should already be deactivated: %v", err)
	}
	if err := waitInstanceDrain(server.URL, action, time.Second); err != nil {
		t.Fatalf("missing route should already be drained: %v", err)
	}
}

func TestDrainPropagatesRouterFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	routerURL := server.URL
	server.Close()

	action := map[string]any{
		"type":            "wait_instance_drain",
		"workload":        "resnet50",
		"physical_gpu_id": "ampere-gpu0",
		"slot":            []any{3, 4, "1g"},
	}
	if err := waitInstanceDrain(routerURL, action, time.Second); err == nil {
		t.Fatal("router failure must not be treated as a drained route")
	}
}

func TestDrainUsesEndpointInflightBeforeModelAggregate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/routes" {
			t.Fatalf("unexpected router request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"routes":[{"model":"resnet50","gpu":"ampere-gpu1","slotResource":"or-sim.io/ampere-gpu1-s1-2-1g","endpointInflight":0,"inflight":38,"queued":0}]}`))
	}))
	defer server.Close()

	action := map[string]any{
		"type":            "wait_instance_drain",
		"workload":        "resnet50",
		"physical_gpu_id": "ampere-gpu1",
		"slot":            []any{1, 2, "1g"},
	}
	if err := waitInstanceDrain(server.URL, action, time.Second); err != nil {
		t.Fatalf("endpoint-specific zero inflight should be drained: %v", err)
	}
}

func TestExpectedMIGSlotsUsesDisplayIDAndIncludesUnassignedSlots(t *testing.T) {
	targetState := map[string]any{
		"gpus": []any{map[string]any{
			"gpuId": 2,
			"instances": []any{
				map[string]any{"start": 0, "end": 2, "profile": "2g"},
				map[string]any{"start": 2, "end": 3, "profile": "1g", "workload": "gpt2"},
				map[string]any{"start": 3, "end": 4, "profile": "1g", "workload": "resnet50"},
				map[string]any{"start": 4, "end": 7, "profile": "3g", "workload": "llama"},
			},
		}},
		"metadata": map[string]any{
			"display_id_map":  map[string]any{"2": 0},
			"physical_id_map": map[string]any{"0": "ampere-gpu0"},
		},
	}

	got := expectedMIGSlotsFromTargetState(targetState)
	want := []string{
		"ampere-gpu0|0|2|2g",
		"ampere-gpu0|2|3|1g",
		"ampere-gpu0|3|4|1g",
		"ampere-gpu0|4|8|3g",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d MIG slots, got %d: %#v", len(want), len(got), got)
	}
	for _, key := range want {
		if !got[key] {
			t.Errorf("missing expected MIG slot %q in %#v", key, got)
		}
	}
}

func TestLogicalThreeGSlotMatchesPhysicalResource(t *testing.T) {
	expected := slotRequest{GPUIndex: 0, Start: 4, End: 7, Profile: "3g"}
	resource := "or-sim.io/ampere-gpu0-s4-8-3g"
	if !slotResourceMatches("ampere-gpu0", resource, expected) {
		t.Fatalf("logical 3g slot must match physical resource %q", resource)
	}

	action := map[string]any{
		"workload":        "llama",
		"physical_gpu_id": "ampere-gpu0",
		"slot":            []any{4, 7, "3g"},
	}
	route := map[string]any{
		"model":        "llama",
		"gpu":          "ampere-gpu0",
		"slotResource": resource,
	}
	if !routeMatchesAction(route, action) {
		t.Fatal("3g route must match its logical planner action")
	}
}

func TestValidatePhysicalAcquireLifecycleRejectsDuplicateAcquire(t *testing.T) {
	actions := []actionNode{
		{
			ID:   "a0000_allocate_bridge",
			Type: "allocate_gpu",
			Action: map[string]any{
				"type":            "allocate_gpu",
				"physical_gpu_id": "ampere-gpu0",
			},
		},
		{
			ID:   "a0001_allocate_temp",
			Type: "allocate_gpu",
			Action: map[string]any{
				"type":            "allocate_gpu",
				"physical_gpu_id": "ampere-gpu0",
			},
		},
	}
	if err := validatePhysicalAcquireLifecycle(actions); err == nil {
		t.Fatal("duplicate physical GPU acquire before return_gpu must be rejected")
	}
}

func TestValidatePhysicalAcquireLifecycleAllowsReuseAfterReturn(t *testing.T) {
	actions := []actionNode{
		{
			ID:   "a0000_allocate_temp",
			Type: "allocate_gpu",
			Action: map[string]any{
				"type":            "allocate_gpu",
				"physical_gpu_id": "ampere-gpu0",
			},
		},
		{
			ID:   "a0001_return_temp",
			Type: "return_gpu",
			Action: map[string]any{
				"type":            "return_gpu",
				"physical_gpu_id": "ampere-gpu0",
			},
		},
		{
			ID:   "a0002_allocate_later",
			Type: "allocate_gpu",
			Action: map[string]any{
				"type":            "allocate_gpu",
				"physical_gpu_id": "ampere-gpu0",
			},
		},
	}
	if err := validatePhysicalAcquireLifecycle(actions); err != nil {
		t.Fatalf("reuse after return_gpu should be allowed: %v", err)
	}
}

func TestParseGPUUUIDFromNvidiaSMIL(t *testing.T) {
	out := `GPU 0: NVIDIA A100-PCIE-40GB (UUID: GPU-565d962e-2b15-aaad-cbe0-97c5c4b447ac)
  MIG 1g.5gb      Device  0: (UUID: MIG-a9aaa9b9-3415-5b83-baab-d52b391db3ac)
GPU 1: NVIDIA A100-PCIE-40GB (UUID: GPU-2f76cbca-88aa-4e8e-2fde-65765797dbbb)`

	if got := parseGPUUUIDFromNvidiaSMIL(out, 0); got != "GPU-565d962e-2b15-aaad-cbe0-97c5c4b447ac" {
		t.Fatalf("unexpected GPU0 UUID: %q", got)
	}
	if got := parseGPUUUIDFromNvidiaSMIL(out, 1); got != "GPU-2f76cbca-88aa-4e8e-2fde-65765797dbbb" {
		t.Fatalf("unexpected GPU1 UUID: %q", got)
	}
}

func TestProcessesForUUID(t *testing.T) {
	payload := map[string]any{
		"processesByUUID": map[string]any{
			"GPU-parent": []any{"123 /cuda-spin"},
		},
	}
	if got := processesForUUID(payload, "GPU-parent"); len(got) != 1 {
		t.Fatalf("expected one parent-GPU process, got %#v", got)
	}
	if got := processesForUUID(payload, "MIG-child"); len(got) != 0 {
		t.Fatalf("unexpected child process fallback hit: %#v", got)
	}
}

func TestNodeAgentRegisteredTargetsRequiresRegisteredResources(t *testing.T) {
	targets := []allocatableTarget{{Node: "ampere", Resource: "or-sim.io/mig-new"}}
	body := map[string]any{
		"expectedResources": []any{"or-sim.io/mig-new"},
	}
	ready, missing := nodeAgentRegisteredTargets(body, targets)
	if ready {
		t.Fatal("expectedResources must not be treated as kubelet registration evidence")
	}
	if len(missing) != 1 || missing[0] != "ampere/or-sim.io/mig-new" {
		t.Fatalf("unexpected missing targets: %#v", missing)
	}
}

func TestNodeAgentRegisteredTargetsAcceptsRefreshRegistration(t *testing.T) {
	targets := []allocatableTarget{{Node: "ampere", Resource: "or-sim.io/mig-new"}}
	body := map[string]any{
		"devicePluginRefresh": map[string]any{
			"registeredResources": []any{"or-sim.io/mig-new"},
		},
	}
	ready, missing := nodeAgentRegisteredTargets(body, targets)
	if !ready || len(missing) != 0 {
		t.Fatalf("expected refresh registration to be diagnostic-ready, ready=%v missing=%#v", ready, missing)
	}
}

func TestRuntimeModelSeparatesWorkloadClassFromLoadedModel(t *testing.T) {
	cases := []struct {
		workload string
		want     string
	}{
		{workload: "resnet50_image", want: "resnet50"},
		{workload: "vgg16_image", want: "vgg16"},
		{workload: "vit_base_image", want: "vit_base"},
		{workload: "gpt2_p64_o64", want: "gpt2"},
		{workload: "llama_p1024_o128", want: "llama"},
	}
	for _, tc := range cases {
		rt := system.ModelRuntimeSpec{Model: tc.workload}
		if got := runtimeModel(rt); got != tc.want {
			t.Errorf("runtimeModel(%q)=%q, want %q", tc.workload, got, tc.want)
		}
	}

	rt := system.ModelRuntimeSpec{Model: "vit_base_image", RuntimeModel: "vit_b_16"}
	if got := runtimeModel(rt); got != "vit_b_16" {
		t.Fatalf("explicit runtimeModel must win, got %q", got)
	}
	rt = system.ModelRuntimeSpec{Model: "vit_base_image", RuntimeModel: "vit_base_image"}
	if got := runtimeModel(rt); got != "vit_base" {
		t.Fatalf("explicit request-class runtimeModel must be normalized, got %q", got)
	}
}
