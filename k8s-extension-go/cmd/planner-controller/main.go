package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
	"or-sim/k8s-extension-go/internal/system"
)

func main() {
	ns := env("NAMESPACE", "or-sim")
	plannerURL := env("PLANNER_ENGINE_URL", "http://planner-engine:8080")
	client, err := kube.NewInCluster(ns)
	if err != nil {
		log.Fatal(err)
	}
	go loop(client, plannerURL)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "component": "planner-controller", "planner": "final-migrant-planner-only"})
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loop(client *kube.Client, plannerURL string) {
	trigger := make(chan struct{}, 1)
	go watchTrigger(client, kube.NamespacedResource(client.Namespace(), "arrivalsnapshots"), "arrivalsnapshots", trigger)
	go watchTrigger(client, kube.NamespacedResource(client.Namespace(), "physicalgpuregistries"), "physicalgpuregistries", trigger)
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		if err := reconcile(client, plannerURL); err != nil {
			log.Printf("planner reconcile failed: %v", err)
		}
		select {
		case <-trigger:
		case <-tick.C:
		}
	}
}

func watchTrigger(client *kube.Client, apiPath, label string, trigger chan<- struct{}) {
	for {
		rv := resourceVersion(client, apiPath)
		err := client.Watch(context.Background(), apiPath, rv, func(eventType string, _ map[string]any) {
			if eventType != "BOOKMARK" {
				signal(trigger)
			}
		})
		if err != nil && err != context.Canceled {
			log.Printf("planner watch %s ended: %v", label, err)
		}
		time.Sleep(time.Second)
	}
}

func signal(trigger chan<- struct{}) {
	select {
	case trigger <- struct{}{}:
	default:
	}
}

func resourceVersion(client *kube.Client, apiPath string) string {
	var list map[string]any
	if _, err := client.Get(apiPath, &list); err != nil {
		log.Printf("planner list before watch failed for %s: %v", apiPath, err)
		return ""
	}
	return asString(asMap(list["metadata"])["resourceVersion"])
}

func reconcile(client *kube.Client, plannerURL string) error {
	var list map[string]any
	if _, err := client.Get(kube.NamespacedResource(client.Namespace(), "arrivalsnapshots"), &list); err != nil {
		return err
	}
	items := asSlice(list["items"])
	sort.Slice(items, func(i, j int) bool {
		return asString(asMap(asMap(items[i])["metadata"])["creationTimestamp"]) < asString(asMap(asMap(items[j])["metadata"])["creationTimestamp"])
	})
	for _, item := range items {
		snap := asMap(item)
		meta := asMap(snap["metadata"])
		name := asString(meta["name"])
		if name == "" {
			continue
		}
		planName := "plan-" + sanitize(name)
		if status, _ := client.Get(kube.NamespacedResourceName(client.Namespace(), "migactionplans", planName), nil); status == http.StatusOK {
			continue
		}
		if err := createPlan(client, plannerURL, snap, planName); err != nil {
			return err
		}
	}
	return nil
}

func createPlan(client *kube.Client, plannerURL string, snap map[string]any, planName string) error {
	spec := asMap(snap["spec"])
	current, err := loadCurrentAllocation(client)
	if err != nil {
		return err
	}
	planInput := planningInputFromSnapshot(spec)
	current.AllowedNodes = planInput.PlacementNodes
	planned, err := callPlannerEngine(plannerURL, planInput, current)
	if err != nil {
		return err
	}
	runtimes := asSlice(planned["desiredRuntimes"])
	actionDag := asMap(planned["actionDag"])
	actionCount := len(asSlice(actionDag["nodes"]))
	if actionCount == 0 {
		actionCount = len(asSlice(planned["abstractActions"]))
	}
	body := map[string]any{
		"apiVersion": "mig.or-sim.io/v1alpha1",
		"kind":       "MigActionPlan",
		"metadata": map[string]any{
			"name":      planName,
			"namespace": client.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":  "migrant-go",
				"mig.or-sim.io/component": "planner-controller",
			},
		},
		"spec": map[string]any{
			"executor":             "go-transition-executor",
			"phaseGate":            "auto",
			"actionCount":          actionCount,
			"targetGpuCount":       uniqueGPUCountFromMaps(runtimes),
			"plannerMetadata":      planned["metadata"],
			"planningInput":        planInput,
			"currentAllocationRef": "physicalgpuregistries/default",
			"targetAllocationPlan": planned["targetAllocationPlan"],
			"abstractActions":      planned["abstractActions"],
			"actionDag":            actionDag,
			"validationTargets":    planned["validationTargets"],
			"summary": map[string]any{
				"arrivalSnapshotRef": asString(asMap(snap["metadata"])["name"]),
				"targetArrival":      planInput.TargetArrival,
				"desiredRuntimes":    runtimes,
			},
		},
	}
	if err := client.Upsert(kube.NamespacedResourceName(client.Namespace(), "migactionplans", planName), body, nil); err != nil {
		return err
	}
	_, err = client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", planName)+"/status", map[string]any{
		"status": map[string]any{"phase": "Planned", "message": "planned by planner-engine with original Gurobi MILP, target materialization, and effect-aware DAG"},
	}, nil)
	return err
}

func callPlannerEngine(plannerURL string, input system.PlanningInput, current system.CurrentAllocation) (map[string]any, error) {
	body := map[string]any{
		"planningInput":     input,
		"currentAllocation": current,
		"scenarioPath":      "mock/scenarios/stage0.yaml",
	}
	raw, _ := json.Marshal(body)
	resp, err := http.Post(strings.TrimRight(plannerURL, "/")+"/plan", "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("planner-engine returned %d: %v", resp.StatusCode, payload)
	}
	return payload, nil
}

func planningInputFromSnapshot(spec map[string]any) system.PlanningInput {
	return system.PlanningInput{
		Source:                asString(spec["source"]),
		Mode:                  asString(spec["mode"]),
		Epoch:                 asString(spec["epoch"]),
		WindowSeconds:         intNumber(spec["windowSeconds"]),
		Unit:                  asString(spec["unit"]),
		ObservedAt:            asString(spec["observedAt"]),
		TriggerReason:         asString(spec["triggerReason"]),
		TargetArrival:         numberMap(spec["targetArrival"]),
		RegisteredSLOMs:       numberMap(spec["registeredSLOMs"]),
		RequestCount:          int64Map(spec["requestCount"]),
		ProfileCatalogRef:     asString(spec["profileCatalogRef"]),
		CalibrationOverlayRef: asString(spec["calibrationOverlayRef"]),
		CurrentAllocationRef:  asString(spec["currentAllocationRef"]),
		PlacementNodes:        stringList(asSlice(asMap(spec["placement"])["nodes"])),
	}
}

func loadCurrentAllocation(client *kube.Client) (system.CurrentAllocation, error) {
	var registry map[string]any
	status, err := client.Get(kube.NamespacedResourceName(client.Namespace(), "physicalgpuregistries", "default"), &registry)
	if err != nil {
		return system.CurrentAllocation{}, err
	}
	if status != http.StatusOK {
		return system.CurrentAllocation{GPUs: map[string]system.CurrentGPU{}}, nil
	}
	registryStatus := asMap(registry["status"])
	canonical := asMap(registryStatus["currentAllocation"])
	bindings := asMap(registryStatus["bindings"])
	gpus := map[string]system.CurrentGPU{}
	for id, raw := range bindings {
		item := asMap(raw)
		gpu := system.CurrentGPU{
			ID:             id,
			Node:           asString(item["node"]),
			GPUIndex:       intNumber(item["gpuIndex"]),
			State:          asString(item["state"]),
			Cleanliness:    asString(item["cleanliness"]),
			RequiredAction: asString(item["requiredAction"]),
			Labels: map[string]string{
				"migConfig":      asString(item["migConfig"]),
				"migConfigState": asString(item["migConfigState"]),
			},
		}
		for _, rawDevice := range asSlice(item["migDevices"]) {
			device := asMap(rawDevice)
			gpu.MIGDevices = append(gpu.MIGDevices, system.ObservedMIGSlot{
				Start:   intNumber(device["start"]),
				End:     intNumber(device["end"]),
				Profile: asString(device["profile"]),
				UUID:    asString(device["uuid"]),
			})
		}
		for _, rawBinding := range asSlice(item["runtimeBindings"]) {
			binding := asMap(rawBinding)
			gpu.RuntimeBindings = append(gpu.RuntimeBindings, system.RuntimeBinding{
				Model:           asString(binding["model"]),
				BatchSize:       intNumber(binding["batchSize"]),
				Pod:             asString(binding["pod"]),
				Phase:           asString(binding["phase"]),
				SlotResource:    asString(binding["slotResource"]),
				DeviceResource:  asString(binding["deviceResource"]),
				ExpectedMIGUUID: asString(binding["expectedMigUuid"]),
			})
		}
		gpus[id] = gpu
	}
	current := system.CurrentAllocation{GPUs: gpus}
	if len(canonical) > 0 {
		current.LogicalGPUs = mapSlice(asSlice(canonical["logicalGpus"]))
		current.Metadata = asMap(canonical["metadata"])
		current.FreePhysicalGPUPool = stringList(asSlice(canonical["freePhysicalGpuPool"]))
		if canonicalGPUs := asMap(canonical["gpus"]); len(canonicalGPUs) > 0 {
			current.GPUs = currentGPUsFromCanonical(canonicalGPUs, gpus)
		}
	}
	return current, nil
}

func currentGPUsFromCanonical(canonical map[string]any, fallback map[string]system.CurrentGPU) map[string]system.CurrentGPU {
	out := map[string]system.CurrentGPU{}
	for id, raw := range canonical {
		item := asMap(raw)
		gpu, ok := fallback[id]
		if !ok {
			gpu = system.CurrentGPU{
				ID:          id,
				Node:        asString(item["node"]),
				GPUIndex:    intNumber(item["gpuIndex"]),
				State:       asString(item["state"]),
				Cleanliness: asString(item["cleanliness"]),
			}
		}
		out[id] = gpu
	}
	return out
}

func loadCalibrationOverlay(client *kube.Client) map[string]float64 {
	var registry map[string]any
	status, err := client.Get(kube.NamespacedResourceName(client.Namespace(), "physicalgpuregistries", "default"), &registry)
	if err != nil || status != http.StatusOK {
		return nil
	}
	overlay := asMap(asMap(registry["status"])["profileCalibrationOverlay"])
	out := map[string]float64{}
	for _, raw := range asSlice(overlay["observations"]) {
		row := asMap(raw)
		model := asString(row["model"])
		if model == "" || asString(row["confidence"]) == "none" {
			continue
		}
		latencyMs := asFloat(row["runtime.avgLatencyMs"])
		if latencyMs == 0 {
			latencyMs = asFloat(row["avgLatencyMs"])
		}
		if latencyMs > 0 {
			out[model] = latencyMs
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func runtimesAsMaps(runtimes []system.ModelRuntimeSpec) []map[string]any {
	out := []map[string]any{}
	for _, rt := range runtimes {
		out = append(out, map[string]any{
			"model": rt.Model, "batchSize": rt.BatchSize, "node": rt.Node,
			"hostPort": rt.HostPort, "profile": rt.Profile, "gpu": rt.GPU,
			"slotResource": rt.SlotResource,
		})
	}
	return out
}

func uniqueGPUCount(runtimes []system.ModelRuntimeSpec) int {
	seen := map[string]bool{}
	for _, rt := range runtimes {
		if rt.GPU != "" {
			seen[rt.Node+"/"+rt.GPU] = true
		}
	}
	return len(seen)
}

func uniqueGPUCountFromMaps(runtimes []any) int {
	seen := map[string]bool{}
	for _, raw := range runtimes {
		rt := asMap(raw)
		gpu := asString(rt["gpu"])
		if gpu == "" {
			continue
		}
		seen[asString(rt["node"])+"/"+gpu] = true
	}
	return len(seen)
}

func stringList(values []any) []string {
	out := []string{}
	for _, raw := range values {
		if value := asString(raw); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func mapSlice(values []any) []map[string]any {
	out := []map[string]any{}
	for _, raw := range values {
		out = append(out, asMap(raw))
	}
	return out
}

func sanitize(s string) string { return strings.ReplaceAll(s, "_", "-") }

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func stringSet(values []any) map[string]bool {
	out := map[string]bool{}
	for _, raw := range values {
		if value := asString(raw); value != "" {
			out[value] = true
		}
	}
	return out
}

func numberMap(v any) map[string]float64 {
	out := map[string]float64{}
	for k, raw := range asMap(v) {
		if n := asFloat(raw); n != 0 {
			out[k] = n
		}
	}
	return out
}

func int64Map(v any) map[string]int64 {
	out := map[string]int64{}
	for k, raw := range asMap(v) {
		switch x := raw.(type) {
		case float64:
			out[k] = int64(x)
		case int:
			out[k] = int64(x)
		case int64:
			out[k] = x
		case json.Number:
			n, _ := x.Int64()
			out[k] = n
		}
	}
	return out
}

func intNumber(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case json.Number:
		n, _ := x.Int64()
		return int(n)
	default:
		return 0
	}
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		n, _ := x.Float64()
		return n
	default:
		return 0
	}
}
