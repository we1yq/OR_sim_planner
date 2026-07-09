package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
	"or-sim/k8s-extension-go/internal/system"
)

type state struct {
	epoch        int
	repairEpoch  int
	lastArrival  map[string]float64
	lastSnapshot time.Time
}

var registeredSLOMs = map[string]float64{
	"llama":    1500,
	"gpt2":     1000,
	"resnet50": 500,
}

func main() {
	ns := env("NAMESPACE", "or-sim")
	router := env("ROUTER_URL", "http://runtime-router:8080")
	client, err := kube.NewInCluster(ns)
	if err != nil {
		log.Fatal(err)
	}
	st := &state{lastArrival: map[string]float64{}}
	go loop(client, router, st)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "component": "epoch-controller", "epoch": st.epoch})
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loop(client *kube.Client, router string, st *state) {
	trigger := make(chan struct{}, 1)
	go watchTrigger(client, kube.NamespacedResourceName(client.Namespace(), "physicalgpuregistries", "default"), "physicalgpuregistry/default", trigger)
	go watchTrigger(client, configMaps(client.Namespace()), "configmaps", trigger)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		if err := reconcile(client, router, st); err != nil {
			log.Printf("epoch reconcile failed: %v", err)
		}
		select {
		case <-trigger:
		case <-tick.C:
		}
	}
}

func reconcile(client *kube.Client, router string, st *state) error {
	if created, err := reconcileRepair(client, st); err != nil {
		return err
	} else if created {
		return nil
	}
	if created, err := reconcileScheduledTrace(client); err != nil {
		return err
	} else if created {
		return nil
	}
	resp, err := http.Get(router + "/metrics/demand")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var demand system.DemandResponse
	if err := json.NewDecoder(resp.Body).Decode(&demand); err != nil {
		return err
	}
	current := map[string]float64{}
	requests := map[string]int64{}
	for _, model := range demand.Models {
		if model.Requests > 0 {
			current[model.Model] = model.ArrivalRate
			requests[model.Model] = model.Requests
		}
	}
	if len(current) == 0 {
		return nil
	}
	open, reason := shouldOpen(st, current, demand.Models)
	if !open {
		return nil
	}
	sourceArrival := copyFloatMap(st.lastArrival)
	planner := env("PLANNER", env("PLANNER_SELECTOR", "ours"))
	st.epoch++
	st.lastSnapshot = time.Now()
	st.lastArrival = copyFloatMap(current)
	name := "runtime-epoch-" + strconv.Itoa(st.epoch)
	spec := map[string]any{
		"source":          "runtime-router",
		"mode":            "observed",
		"planner":         planner,
		"epoch":           strconv.Itoa(st.epoch),
		"windowSeconds":   int(demand.WindowSeconds),
		"unit":            "requests_per_second",
		"observedAt":      time.Now().Format(time.RFC3339Nano),
		"triggerReason":   reason,
		"registeredSLOMs": registeredSLOMs,
		"sourceArrival":   sourceArrival,
		"targetArrival":   current,
		"slo":             sloFromArrival(current, sourceArrival, registeredSLOMs),
		"requestCount":    requests,
	}
	return createArrivalSnapshot(client, name, "epoch-controller", spec)
}

func createArrivalSnapshot(client *kube.Client, name, component string, spec map[string]any) error {
	body := map[string]any{
		"apiVersion": "mig.or-sim.io/v1alpha1",
		"kind":       "ArrivalSnapshot",
		"metadata": map[string]any{
			"name":      name,
			"namespace": client.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":  "migrant-go",
				"mig.or-sim.io/component": component,
			},
		},
		"spec": spec,
	}
	if err := client.Upsert(kube.NamespacedResourceName(client.Namespace(), "arrivalsnapshots", name), body, nil); err != nil {
		return err
	}
	_, err := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "arrivalsnapshots", name)+"/status", map[string]any{
		"status": map[string]any{"phase": "Ready", "message": "created by " + component},
	}, nil)
	return err
}

func reconcileScheduledTrace(client *kube.Client) (bool, error) {
	schedule, ok, err := loadTraceSchedule(client)
	if err != nil || !ok {
		return false, err
	}
	if hasActiveMigActionPlan(client) {
		return false, nil
	}
	stages := asSlice(schedule["stages"])
	sort.Slice(stages, func(i, j int) bool {
		left := intNumber(asMap(stages[i])["offsetSeconds"])
		right := intNumber(asMap(stages[j])["offsetSeconds"])
		if left != right {
			return left < right
		}
		return asString(asMap(stages[i])["epoch"]) < asString(asMap(stages[j])["epoch"])
	})
	now := time.Now()
	startedAt := scheduleStartedAt(schedule, now)
	prevTarget := firstNonEmptyMap(
		asMap(schedule["sourceArrival"]),
		asMap(schedule["currentDemand"]),
		asMap(schedule["currentArrival"]),
		demandRateMapFromSLO(asMap(schedule["slo"]), "sourceArrival", "currentDemandRate", "currentDemand", "sourceDemandRate", "sourceDemand"),
	)
	for stageIdx, rawStage := range stages {
		stage := asMap(rawStage)
		epoch := firstNonEmpty(asString(stage["epoch"]), asString(stage["name"]))
		if epoch == "" {
			continue
		}
		targetArrival := firstNonEmptyMap(
			asMap(stage["targetArrival"]),
			asMap(stage["targetDemand"]),
			demandRateMapFromSLO(asMap(stage["slo"]), "targetArrival", "demandRate", "targetDemandRate", "targetDemand"),
		)
		sourceArrival := firstNonEmptyMap(
			asMap(stage["sourceArrival"]),
			asMap(stage["currentDemand"]),
			asMap(stage["currentArrival"]),
			asMap(stage["sourceDemand"]),
			demandRateMapFromSLO(asMap(stage["slo"]), "sourceArrival", "currentDemandRate", "currentDemand", "sourceDemandRate", "sourceDemand"),
			prevTarget,
		)
		offset := scheduledStageOffset(schedule, stage, stageIdx)
		if now.Before(startedAt.Add(offset)) {
			break
		}
		name := "scheduled-" + sanitize(epoch)
		if status, _ := client.Get(kube.NamespacedResourceName(client.Namespace(), "arrivalsnapshots", name), nil); status == http.StatusOK {
			if len(targetArrival) > 0 {
				prevTarget = targetArrival
			}
			continue
		}
		slo := firstNonEmptyMap(asMap(stage["slo"]), asMap(schedule["slo"]))
		if len(slo) == 0 {
			slo = sloFromArrivalAny(targetArrival, sourceArrival, firstNonEmptyMap(asMap(stage["registeredSLOMs"]), asMap(schedule["registeredSLOMs"]), registeredSLOMs))
		}
		spec := map[string]any{
			"source":               firstNonEmpty(asString(schedule["source"]), "trace"),
			"mode":                 firstNonEmpty(asString(schedule["mode"]), "scheduled"),
			"planner":              firstNonEmpty(asString(stage["planner"]), asString(stage["planningMethod"]), asString(stage["targetPlanner"]), asString(schedule["planner"]), asString(schedule["planningMethod"]), asString(schedule["targetPlanner"]), env("PLANNER", env("PLANNER_SELECTOR", "ours"))),
			"epoch":                epoch,
			"windowSeconds":        firstPositive(intNumber(stage["windowSeconds"]), intNumber(schedule["windowSeconds"]), 60),
			"unit":                 firstNonEmpty(asString(schedule["unit"]), "requests_per_second"),
			"observedAt":           now.Format(time.RFC3339Nano),
			"triggerReason":        "scheduled_trace_window",
			"scheduleOffsetSeconds": offset.Seconds(),
			"profileCatalogRef":    firstNonEmpty(asString(stage["profileCatalogRef"]), asString(schedule["profileCatalogRef"]), "default"),
			"currentAllocationRef": firstNonEmpty(asString(stage["currentAllocationRef"]), asString(schedule["currentAllocationRef"]), "physicalgpuregistry/default"),
			"registeredSLOMs":      firstNonEmptyMap(asMap(stage["registeredSLOMs"]), asMap(schedule["registeredSLOMs"]), registeredSLOMs),
			"sourceArrival":        sourceArrival,
			"targetArrival":        targetArrival,
			"slo":                  slo,
			"notes": []string{
				"scheduled/forecast epoch created from trace-derived request-rate vector",
				"the predictor is external to this system; this ConfigMap supplies its output for experiments",
			},
		}
		if placement := asMap(stage["placement"]); len(placement) > 0 {
			spec["placement"] = placement
		} else if placement := asMap(schedule["placement"]); len(placement) > 0 {
			spec["placement"] = placement
		}
		if err := createArrivalSnapshot(client, name, "scheduled-trace", spec); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func loadTraceSchedule(client *kube.Client) (map[string]any, bool, error) {
	var cm map[string]any
	status, err := client.Get(configMapPath(client.Namespace(), "arrival-trace-schedule"), &cm)
	if err != nil {
		if status == http.StatusNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	raw := strings.TrimSpace(asString(asMap(cm["data"])["schedule.json"]))
	if raw == "" {
		return nil, false, nil
	}
	var schedule map[string]any
	if err := json.Unmarshal([]byte(raw), &schedule); err != nil {
		return nil, false, err
	}
	if asString(schedule["startAt"]) == "" && asString(schedule["createdAt"]) == "" {
		if createdAt := asString(asMap(cm["metadata"])["creationTimestamp"]); createdAt != "" {
			schedule["createdAt"] = createdAt
		}
	}
	return schedule, true, nil
}

func configMaps(ns string) string {
	return "/api/v1/namespaces/" + ns + "/configmaps"
}

func configMapPath(ns, name string) string {
	return "/api/v1/namespaces/" + ns + "/configmaps/" + name
}

func hasActiveMigActionPlan(client *kube.Client) bool {
	var list map[string]any
	if _, err := client.Get(kube.NamespacedResource(client.Namespace(), "migactionplans"), &list); err != nil {
		return false
	}
	for _, raw := range asSlice(list["items"]) {
		phase := asString(asMap(asMap(raw)["status"])["phase"])
		if phase != "Executed" && phase != "Failed" {
			return true
		}
	}
	return false
}

func scheduleStartedAt(schedule map[string]any, fallback time.Time) time.Time {
	raw := asString(schedule["startAt"])
	if raw == "" {
		raw = asString(schedule["createdAt"])
	}
	if raw == "" {
		return fallback
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, raw)
	}
	if err != nil {
		return fallback
	}
	return parsed
}

func scheduledStageOffset(schedule, stage map[string]any, stageIdx int) time.Duration {
	if stageDuration := asFloat(firstNonZero(stage["stageDurationSeconds"], schedule["stageDurationSeconds"])); stageDuration > 0 {
		return secondsDuration(float64(stageIdx) * stageDuration)
	}
	offsetSeconds := asFloat(stage["offsetSeconds"])
	compression := asFloat(schedule["timeCompression"])
	if compression <= 0 {
		compression = 1
	}
	return secondsDuration(offsetSeconds / compression)
}

func firstNonZero(values ...any) any {
	for _, value := range values {
		if asFloat(value) != 0 {
			return value
		}
	}
	return nil
}

func secondsDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func reconcileRepair(client *kube.Client, st *state) (bool, error) {
	var registry map[string]any
	status, err := client.Get(kube.NamespacedResourceName(client.Namespace(), "physicalgpuregistries", "default"), &registry)
	if err != nil || status != http.StatusOK {
		return false, err
	}
	health := asMap(asMap(registry["status"])["health"])
	if !asBool(health["repairRequired"]) {
		return false, nil
	}
	required := asSlice(health["requiredActions"])
	if len(required) == 0 {
		return false, nil
	}
	if hasActiveRepairPlan(client) {
		return false, nil
	}
	st.repairEpoch++
	name := "repair-epoch-" + strconv.Itoa(st.repairEpoch) + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	actions := repairActions(required)
	if len(actions) == 0 {
		return false, nil
	}
	body := map[string]any{
		"apiVersion": "mig.or-sim.io/v1alpha1",
		"kind":       "MigActionPlan",
		"metadata": map[string]any{
			"name":      name,
			"namespace": client.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":  "migrant-go",
				"mig.or-sim.io/component": "reconciler",
				"mig.or-sim.io/plan-type": "repair",
			},
		},
		"spec": map[string]any{
			"executor":             "go-transition-executor",
			"phaseGate":            "auto",
			"planType":             "repair",
			"actionCount":          len(actions),
			"targetGpuCount":       intNumber(asMap(asMap(registry["status"])["queueCounts"])["active"]),
			"currentAllocationRef": "physicalgpuregistries/default",
			"plannerMetadata": map[string]any{
				"planner": "state-repair-reconciler",
				"reason":  "PhysicalGpuRegistry health requires repair",
			},
			"abstractActions": []any{},
			"actionDag": map[string]any{
				"format": "migrant.action-dag/v1",
				"name":   name,
				"nodes":  actions,
			},
			"validationTargets": map[string]any{
				"registryHealth": health,
			},
			"summary": map[string]any{
				"planType":        "repair",
				"requiredActions": required,
				"desiredRuntimes": []any{},
			},
		},
	}
	if err := client.Upsert(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name), body, nil); err != nil {
		return false, err
	}
	_, err = client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
		"status": map[string]any{"phase": "Planned", "message": "state repair plan created by reconciler"},
	}, nil)
	return true, err
}

func repairActions(required []any) []map[string]any {
	nodes := []map[string]any{}
	index := 0
	for _, raw := range required {
		req := asMap(raw)
		if asString(req["type"]) != "clear_template_before_available" {
			continue
		}
		physicalID := asString(req["physicalGpuId"])
		if physicalID == "" {
			continue
		}
		clearID := "repair-clear-template-" + sanitize(physicalID)
		returnID := "repair-return-gpu-" + sanitize(physicalID)
		common := map[string]any{
			"physical_gpu_id": physicalID,
			"physicalGpuId":   physicalID,
			"gpu":             physicalID,
			"node":            req["node"],
			"gpuIndex":        req["gpuIndex"],
			"reason":          firstNonEmpty(asString(req["reason"]), "clear_template_before_available"),
		}
		nodes = append(nodes, map[string]any{
			"id":    clearID,
			"type":  "clear_template",
			"phase": index,
			"index": index,
			"action": mergeMap(common, map[string]any{
				"type":           "clear_template",
				"abstractAction": "Clear Template Before Available",
			}),
		})
		index++
		nodes = append(nodes, map[string]any{
			"id":        returnID,
			"type":      "return_gpu",
			"phase":     index,
			"index":     index,
			"dependsOn": []string{clearID},
			"action": mergeMap(common, map[string]any{
				"type":           "return_gpu",
				"abstractAction": "Return GPU",
			}),
		})
		index++
	}
	return nodes
}

func hasActiveRepairPlan(client *kube.Client) bool {
	var list map[string]any
	if _, err := client.Get(kube.NamespacedResource(client.Namespace(), "migactionplans"), &list); err != nil {
		return false
	}
	for _, raw := range asSlice(list["items"]) {
		plan := asMap(raw)
		spec := asMap(plan["spec"])
		meta := asMap(plan["metadata"])
		labels := asMap(meta["labels"])
		if asString(spec["planType"]) != "repair" && asString(labels["mig.or-sim.io/plan-type"]) != "repair" {
			continue
		}
		phase := asString(asMap(plan["status"])["phase"])
		if phase != "Executed" && phase != "Failed" {
			return true
		}
	}
	return false
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
			log.Printf("epoch watch %s ended: %v", label, err)
		}
		time.Sleep(time.Second)
	}
}

func resourceVersion(client *kube.Client, apiPath string) string {
	var obj map[string]any
	if _, err := client.Get(apiPath, &obj); err != nil {
		log.Printf("epoch list before watch failed for %s: %v", apiPath, err)
		return ""
	}
	return asString(asMap(obj["metadata"])["resourceVersion"])
}

func signal(trigger chan<- struct{}) {
	select {
	case trigger <- struct{}{}:
	default:
	}
}

func shouldOpen(st *state, current map[string]float64, models []system.DemandModel) (bool, string) {
	if st.epoch == 0 {
		return true, "initial_observed_demand"
	}
	if time.Since(st.lastSnapshot) < 30*time.Second {
		return false, ""
	}
	for _, model := range models {
		slo := registeredSLOMs[model.Model]
		if slo > 0 && model.AvgLatencyMs > slo {
			return true, "slo_latency_violation:" + model.Model
		}
		if model.ErrorRate > 0 {
			return true, "error_rate_nonzero:" + model.Model
		}
		if model.Queued > 0 {
			return true, "queue_nonempty:" + model.Model
		}
	}
	for model, value := range current {
		prev := st.lastArrival[model]
		if prev == 0 && value > 0 {
			return true, "new_model_demand:" + model
		}
		if prev > 0 && math.Abs(value-prev)/prev >= 0.30 {
			return true, "arrival_drift:" + model
		}
	}
	return false, ""
}

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

func intNumber(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func firstNonEmptyMap(values ...any) map[string]any {
	for _, value := range values {
		switch typed := value.(type) {
		case map[string]any:
			if len(typed) > 0 {
				return typed
			}
		case map[string]float64:
			if len(typed) > 0 {
				out := map[string]any{}
				for key, item := range typed {
					out[key] = item
				}
				return out
			}
		}
	}
	return map[string]any{}
}

func copyFloatMap(in map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func sloFromArrival(target, source map[string]float64, latency map[string]float64) map[string]any {
	out := map[string]any{}
	for model, demand := range target {
		row := map[string]any{"demandRate": demand}
		if sourceDemand, ok := source[model]; ok {
			row["sourceDemandRate"] = sourceDemand
		}
		if latencyMs, ok := latency[model]; ok && latencyMs > 0 {
			row["latencyMs"] = latencyMs
		}
		out[model] = row
	}
	for model, sourceDemand := range source {
		if _, ok := out[model]; ok {
			continue
		}
		row := map[string]any{"sourceDemandRate": sourceDemand}
		if latencyMs, ok := latency[model]; ok && latencyMs > 0 {
			row["latencyMs"] = latencyMs
		}
		out[model] = row
	}
	return out
}

func sloFromArrivalAny(target, source, latency map[string]any) map[string]any {
	out := map[string]any{}
	for model, rawDemand := range target {
		row := map[string]any{"demandRate": rawDemand}
		if sourceDemand, ok := source[model]; ok {
			row["sourceDemandRate"] = sourceDemand
		}
		if latencyMs, ok := latency[model]; ok {
			row["latencyMs"] = latencyMs
		}
		out[model] = row
	}
	for model, sourceDemand := range source {
		if _, ok := out[model]; ok {
			continue
		}
		row := map[string]any{"sourceDemandRate": sourceDemand}
		if latencyMs, ok := latency[model]; ok {
			row["latencyMs"] = latencyMs
		}
		out[model] = row
	}
	return out
}

func demandRateMapFromSLO(slo map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		if direct := asMap(slo[key]); len(direct) > 0 {
			return direct
		}
	}
	out := map[string]any{}
	keySet := map[string]bool{}
	for _, key := range keys {
		keySet[key] = true
	}
	for model, raw := range slo {
		modelSLO := asMap(raw)
		for key := range keySet {
			if n, ok := optionalFloat(modelSLO[key]); ok {
				out[model] = n
				break
			}
		}
	}
	return out
}

func asFloat(v any) float64 {
	value, _ := optionalFloat(v)
	return value
}

func optionalFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		value, _ := n.Float64()
		return value, true
	default:
		return 0, false
	}
}

func sanitize(value string) string {
	out := strings.ToLower(value)
	out = strings.ReplaceAll(out, "_", "-")
	out = strings.ReplaceAll(out, ".", "-")
	out = strings.ReplaceAll(out, "/", "-")
	return out
}

func mergeMap(left, right map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range left {
		out[key] = value
	}
	for key, value := range right {
		out[key] = value
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
