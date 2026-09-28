package main

import (
	"encoding/json"
	"fmt"
	"io"
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
		if r.Method != http.MethodGet || (r.URL.Path != "/routes" && r.URL.Path != "/control/routes") {
			t.Errorf("unexpected router request: %s %s", r.Method, r.URL.Path)
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
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/control/routes":
			_, _ = w.Write([]byte(`{"routes":[{"model":"resnet50","runtimeId":"rt-r50","gpu":"ampere-gpu1","slotResource":"or-sim.io/ampere-gpu1-s1-2-1g"}]}`))
		case r.URL.Path == "/routes" && r.URL.Query().Get("runtimeId") == "rt-r50":
			_, _ = w.Write([]byte(`{"routes":[{"model":"resnet50","runtimeId":"rt-r50","gpu":"ampere-gpu1","slotResource":"or-sim.io/ampere-gpu1-s1-2-1g","endpointInflight":0,"inflight":38,"queued":0}]}`))
		default:
			t.Errorf("unexpected router request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
		}
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

func TestExpectedRuntimeBindingsIncludeBatchSize(t *testing.T) {
	targetPlan := map[string]any{"desiredRuntimes": []any{map[string]any{
		"model": "vit_base", "gpu": "ampere-gpu0", "slotResource": "or-sim.io/ampere-gpu0-s0-1-1g", "batchSize": 32,
	}}}
	got := expectedRuntimeBindingsFromTargetPlan(targetPlan)
	if !got["ampere-gpu0|or-sim.io/ampere-gpu0-s0-1-1g|vit_base|batch=32"] {
		t.Fatalf("batch must be part of the expected runtime identity: %#v", got)
	}
	if got["ampere-gpu0|or-sim.io/ampere-gpu0-s0-1-1g|vit_base|batch=1"] {
		t.Fatalf("unexpected stale-batch match: %#v", got)
	}
}

func TestMatchingRuntimeRoutePrefersRuntimeID(t *testing.T) {
	routes := []any{
		map[string]any{"runtimeId": "old", "model": "vit_base", "gpu": "ampere-gpu0", "slotResource": "slot-a"},
		map[string]any{"runtimeId": "target", "model": "vit_base", "gpu": "ampere-gpu0", "slotResource": "slot-a"},
	}
	route := matchingRuntimeRoute(routes, map[string]any{"runtimeId": "target", "model": "vit_base", "gpu": "ampere-gpu0", "slotResource": "slot-a"})
	if asString(route["runtimeId"]) != "target" {
		t.Fatalf("matched route = %#v, want runtimeId target", route)
	}
}

func TestValidateLiveRuntimeBatchesRejectsStaleRuntimeBatch(t *testing.T) {
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			t.Fatalf("unexpected runtime path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"batchSize":1}`))
	}))
	defer runtime.Close()
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/routes" {
			t.Fatalf("unexpected router path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"routes":[{"runtimeId":"vit-a","model":"vit_base","gpu":"ampere-gpu0","slotResource":"slot-a","endpoint":"` + runtime.URL + `"}]}`))
	}))
	defer router.Close()

	failures := validateLiveRuntimeBatches(router.URL, map[string]any{"desiredRuntimes": []any{map[string]any{
		"runtimeId": "vit-a", "model": "vit_base", "gpu": "ampere-gpu0", "slotResource": "slot-a", "batchSize": 32,
	}}})
	if len(failures) != 1 || intNumber(failures[0]["observedBatchSize"]) != 1 {
		t.Fatalf("stale live batch must fail validation, got %#v", failures)
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

func TestActionResourceClaimsAllowIndependentSlotsOnSameGPU(t *testing.T) {
	first := actionNode{Type: "place_instance", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "slot": []any{0, 1, "1g"}, "workload": "resnet50",
	}}
	second := actionNode{Type: "activate_instance_route", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "slot": []any{1, 2, "1g"}, "workload": "vgg16",
	}}
	running := map[string][]actionResourceClaim{"first": actionResourceClaims(first)}
	if resourceClaimsConflict(actionResourceClaims(second), running) {
		t.Fatal("different slots on one GPU must be allowed to execute concurrently")
	}
}

func TestActionResourceClaimsSerializeSameSlotAcrossActionKinds(t *testing.T) {
	place := actionNode{Type: "place_instance", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "slot": []any{0, 1, "1g"}, "workload": "resnet50",
	}}
	route := actionNode{Type: "activate_instance_route", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "slot": []any{0, 1, "1g"}, "workload": "resnet50",
	}}
	running := map[string][]actionResourceClaim{"place": actionResourceClaims(place)}
	if !resourceClaimsConflict(actionResourceClaims(route), running) {
		t.Fatal("actions on the same slot must remain serialized")
	}
}

func TestActionResourceClaimsGPUExclusiveConflictsWithSlots(t *testing.T) {
	geometry := actionNode{Type: "apply_slots", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0",
	}}
	slot := actionNode{Type: "delete_instance", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "slot": []any{1, 2, "1g"}, "workload": "vgg16",
	}}
	running := map[string][]actionResourceClaim{"geometry": actionResourceClaims(geometry)}
	if !resourceClaimsConflict(actionResourceClaims(slot), running) {
		t.Fatal("GPU geometry changes must exclude all slot work on that GPU")
	}
	if !resourceClaimsConflict(actionResourceClaims(geometry), map[string][]actionResourceClaim{"slot": actionResourceClaims(slot)}) {
		t.Fatal("GPU geometry changes must wait for in-flight slot work")
	}
}

func TestActionResourceClaimsFallbackToGPUExclusiveWithoutSlot(t *testing.T) {
	malformed := actionNode{Type: "wait_instance_drain", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "workload": "resnet50",
	}}
	slot := actionNode{Type: "place_instance", Action: map[string]any{
		"physical_gpu_id": "ampere-gpu0", "slot": []any{1, 2, "1g"}, "workload": "vgg16",
	}}
	if !resourceClaimsConflict(actionResourceClaims(malformed), map[string][]actionResourceClaim{"slot": actionResourceClaims(slot)}) {
		t.Fatal("slot-less instance action must conservatively lock the physical GPU")
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

func TestDeploymentCPUPlacementEnv(t *testing.T) {
	rt := system.ModelRuntimeSpec{Model: "llama", Node: "rtx1-worker", HostPort: 18080, Profile: "4g", GPU: "rtx1-worker-gpu0", BatchSize: 1}
	envValue := func(dep map[string]any, name string) (string, bool) {
		spec := asMap(asMap(asMap(dep["spec"])["template"])["spec"])
		containers, _ := spec["containers"].([]map[string]any)
		for _, c := range containers {
			envs, _ := c["env"].([]map[string]any)
			for _, e := range envs {
				if e["name"] == name {
					return asString(e["value"]), true
				}
			}
		}
		return "", false
	}
	dep := deployment("or-sim", rt, runtimeCPUPlacement{Exclude: "1,33", Set: "4,36"})
	if v, ok := envValue(dep, "OR_SIM_CPU_EXCLUDE"); !ok || v != "1,33" {
		t.Fatalf("OR_SIM_CPU_EXCLUDE = %q, %v; want \"1,33\"", v, ok)
	}
	if v, ok := envValue(dep, "OR_SIM_CPU_SET"); !ok || v != "4,36" {
		t.Fatalf("OR_SIM_CPU_SET = %q, %v; want \"4,36\"", v, ok)
	}
	dep = deployment("or-sim", rt, runtimeCPUPlacement{})
	for _, name := range []string{"OR_SIM_CPU_EXCLUDE", "OR_SIM_CPU_SET"} {
		if v, ok := envValue(dep, name); ok {
			t.Fatalf("%s must be absent without annotations, got %q", name, v)
		}
	}
}

func TestRuntimeCPUPlacementForUsesSlotIndex(t *testing.T) {
	pool := []string{}
	for i := 0; i < 16; i++ {
		pool = append(pool, fmt.Sprintf("c%d", i))
	}
	cfg := nodeCPUConfig{Exclude: "1,33", Pool: pool}
	cases := []struct {
		gpu, slot string
		want      string
	}{
		{"ampere-gpu0", "or-sim.io/ampere-gpu0-s0-4-3g", "c0"},
		{"ampere-gpu0", "or-sim.io/ampere-gpu0-s4-8-3g", "c4"},
		{"ampere-gpu1", "or-sim.io/ampere-gpu1-s2-4-2g", "c10"},
		{"ampere-gpu1", "or-sim.io/ampere-gpu1-s6-7-1g", "c14"},
	}
	for _, tc := range cases {
		rt := system.ModelRuntimeSpec{Model: "llama", Node: "ampere", GPU: tc.gpu, SlotResource: tc.slot}
		got, err := runtimeCPUPlacementFor(cfg, rt)
		if err != nil || got.Set != tc.want || got.Exclude != "1,33" {
			t.Fatalf("%s: placement = %+v, %v; want set %s", tc.slot, got, err, tc.want)
		}
	}
	short := nodeCPUConfig{Pool: []string{"c0"}}
	if _, err := runtimeCPUPlacementFor(short, system.ModelRuntimeSpec{Model: "llama", Node: "ampere", GPU: "ampere-gpu1", SlotResource: "or-sim.io/ampere-gpu1-s0-4-3g"}); err == nil {
		t.Fatal("a pool too short for the slot must be an error")
	}
	if got, err := runtimeCPUPlacementFor(nodeCPUConfig{}, system.ModelRuntimeSpec{Model: "llama"}); err != nil || got.Set != "" {
		t.Fatalf("no pool must yield no set, got %+v, %v", got, err)
	}
}

func routePatchRecorder(t *testing.T, patches *[]map[string]any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/control/routes":
			_, _ = w.Write([]byte(`{"routes":[{"model":"llama_p2048_o64","runtimeModel":"llama","promptLen":2048,"runtimeId":"rt-l","endpoint":"http://l:1","gpu":"ampere-gpu0","slotResource":"or-sim.io/ampere-gpu0-s0-4-3g","batchSize":1}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/control/routes":
			raw, _ := io.ReadAll(r.Body)
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			*patches = append(*patches, body)
			_, _ = w.Write(raw)
		default:
			t.Errorf("unexpected router request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

func TestDrainAndBatchSyncPatchOnlyChangedFields(t *testing.T) {
	var patches []map[string]any
	server := routePatchRecorder(t, &patches)
	defer server.Close()
	action := map[string]any{"workload": "llama_p2048_o64", "physical_gpu_id": "ampere-gpu0", "slot": []any{0, 4, "3g"}}
	if err := markRouteDrainingForAction(server.URL, action); err != nil {
		t.Fatal(err)
	}
	if err := syncRouteBatchSize(server.URL, action, 4); err != nil {
		t.Fatal(err)
	}
	if len(patches) != 2 {
		t.Fatalf("patches = %v, want 2", patches)
	}
	drain, batch := patches[0], patches[1]
	if drain["runtimeId"] != "rt-l" || drain["draining"] != true || drain["acceptingNew"] != false || len(drain) != 5 {
		t.Fatalf("drain patch = %v", drain)
	}
	if batch["runtimeId"] != "rt-l" || batch["batchSize"] != float64(4) || len(batch) != 3 {
		t.Fatalf("batch patch = %v", batch)
	}
	for _, patch := range patches {
		for _, field := range []string{"promptLen", "runtimeModel", "endpoint"} {
			if _, ok := patch[field]; ok {
				t.Fatalf("patch must not resend stored field %s: %v", field, patch)
			}
		}
	}
}

func TestMIGRegistrationAndBindingDoNotBlockOtherSlots(t *testing.T) {
	claim := func(actionType string, slot []any) []actionResourceClaim {
		action := map[string]any{"physical_gpu_id": "ampere-gpu0"}
		if slot != nil {
			action["slot"] = slot
		}
		return actionResourceClaims(actionNode{Type: actionType, Action: action})
	}
	slotAction := claim("apply_batch", []any{4, 8, "3g"})
	slotAction2 := claim("place_instance", []any{0, 4, "4g"})
	for _, actionType := range []string{"register_mig_devices", "refresh_slot_resources", "allocate_gpu", "bind_target_gpu"} {
		c := claim(actionType, nil)
		if resourceClaimsConflict(c, map[string][]actionResourceClaim{"running": slotAction}) {
			t.Fatalf("%s must not block slot actions on the same GPU", actionType)
		}
		if resourceClaimsConflict(slotAction2, map[string][]actionResourceClaim{"running": c}) {
			t.Fatalf("slot action must not wait for %s on the same GPU", actionType)
		}
		if !resourceClaimsConflict(c, map[string][]actionResourceClaim{"running": claim("configure_partial_profile", nil)}) {
			t.Fatalf("%s must wait for MIG geometry changes on the GPU", actionType)
		}
		if !resourceClaimsConflict(c, map[string][]actionResourceClaim{"running": c}) {
			t.Fatalf("two %s on one GPU must not overlap", actionType)
		}
	}
	for _, actionType := range []string{"configure_partial_profile", "configure_full_template", "clear_template", "clear_gpu_binding", "return_gpu"} {
		if !resourceClaimsConflict(claim(actionType, nil), map[string][]actionResourceClaim{"running": slotAction}) {
			t.Fatalf("%s must stay exclusive on its GPU", actionType)
		}
	}
}

func TestWaitingExclusiveActionReservesItsGPU(t *testing.T) {
	reserved := map[string]string{"ampere-gpu0": "a0010_configure"}
	slot := []actionResourceClaim{{physicalID: "ampere-gpu0", kind: resourceSlot, key: "0:4:3g"}}
	other := []actionResourceClaim{{physicalID: "ampere-gpu1", kind: resourceSlot, key: "0:4:3g"}}
	if !claimsReservedGPU(slot, reserved, "a0011_place") {
		t.Fatal("new action on a reserved GPU must wait")
	}
	if claimsReservedGPU(other, reserved, "a0012_place") {
		t.Fatal("reservation must not affect other GPUs")
	}
	if claimsReservedGPU([]actionResourceClaim{{physicalID: "ampere-gpu0", kind: resourceGPUExclusive}}, reserved, "a0010_configure") {
		t.Fatal("the reserving action itself must not be blocked by its reservation")
	}
}

func TestMissingIDs(t *testing.T) {
	got := missingIDs([]string{"a", "b", "", "c"}, []string{"b"})
	if fmt.Sprint(got) != "[a c]" {
		t.Fatalf("missingIDs = %v", got)
	}
}
