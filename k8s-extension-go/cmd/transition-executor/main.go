package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
	"or-sim/k8s-extension-go/internal/system"
)

func main() {
	ns := env("NAMESPACE", "or-sim")
	router := env("ROUTER_URL", "http://runtime-router:8080")
	client, err := kube.NewInCluster(ns)
	if err != nil {
		log.Fatal(err)
	}
	go loop(client, router)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "component": "transition-executor"})
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loop(client *kube.Client, router string) {
	trigger := make(chan struct{}, 1)
	go watchTrigger(client, kube.NamespacedResource(client.Namespace(), "migactionplans"), "migactionplans", trigger)
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		if err := reconcile(client, router); err != nil {
			log.Printf("executor reconcile failed: %v", err)
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
			log.Printf("executor watch %s ended: %v", label, err)
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
		log.Printf("executor list before watch failed for %s: %v", apiPath, err)
		return ""
	}
	return asString(asMap(list["metadata"])["resourceVersion"])
}

func reconcile(client *kube.Client, router string) error {
	var list map[string]any
	if _, err := client.Get(kube.NamespacedResource(client.Namespace(), "migactionplans"), &list); err != nil {
		return err
	}
	nodes, err := nodeIPs(client)
	if err != nil {
		return err
	}
	for _, item := range asSlice(list["items"]) {
		plan := asMap(item)
		meta := asMap(plan["metadata"])
		spec := asMap(plan["spec"])
		status := asMap(plan["status"])
		name := asString(meta["name"])
		phase := asString(status["phase"])
		if asString(spec["executor"]) != "go-transition-executor" || phase == "Executed" || phase == "Failed" {
			continue
		}
		trace := newExecutionTrace()
		trace.Mark("executorStartedAt")
		patchExecutionStatus(client, name, "Executing", "transition execution started", trace, nil)
		runtimes := parseRuntimes(spec)
		actions := parseActionNodes(spec)
		if len(actions) == 0 {
			trace.Mark("executorFinishedAt")
			_, err = client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
				"status": map[string]any{"phase": "Executed", "message": "empty action DAG; no-op plan", "transitionExecution": trace.Status(nil)},
			}, nil)
			if err != nil {
				return err
			}
			continue
		}
		verification, actionStatuses, err := executeActionDAG(client, router, nodes, runtimes, actions, name, trace)
		trace.Mark("executorFinishedAt")
		if err != nil {
			_, patchErr := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
				"status": map[string]any{"phase": "Failed", "message": err.Error(), "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
			}, nil)
			if patchErr != nil {
				return patchErr
			}
			continue
		}
		if err := persistFinalLogicalBindings(client, name, spec); err != nil {
			_, patchErr := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
				"status": map[string]any{"phase": "Failed", "message": err.Error(), "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
			}, nil)
			if patchErr != nil {
				return patchErr
			}
			continue
		}
		_, err = client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
			"status": map[string]any{"phase": "Executed", "message": "action DAG executed by Go transition executor", "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
		}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type executionTrace struct {
	start            time.Time
	timestamps       map[string]time.Time
	metrics          map[string]any
	runtimeReadiness map[string]any
}

func newExecutionTrace() *executionTrace {
	return &executionTrace{start: time.Now(), timestamps: map[string]time.Time{}, metrics: map[string]any{}}
}

func (t *executionTrace) Mark(name string) {
	t.timestamps[name] = time.Now()
}

func (t *executionTrace) Has(name string) bool {
	_, ok := t.timestamps[name]
	return ok
}

func (t *executionTrace) SetRuntimeReadiness(value map[string]any) {
	t.runtimeReadiness = value
}

func (t *executionTrace) SetMetric(name string, value any) {
	t.metrics[name] = value
}

func (t *executionTrace) Status(verification map[string]any) map[string]any {
	out := map[string]any{
		"timestamps":       map[string]any{},
		"durationsSeconds": map[string]any{},
	}
	for name, value := range t.timestamps {
		out["timestamps"].(map[string]any)[name] = value.Format(time.RFC3339Nano)
	}
	durations := out["durationsSeconds"].(map[string]any)
	t.duration(durations, "waitRuntimePodsGone", "staleRoutesDeletedAt", "runtimePodsGoneAt")
	t.duration(durations, "waitDrain", "drainWaitStartedAt", "drainWaitFinishedAt")
	t.duration(durations, "clearGPUBinding", "gpuBindingClearStartedAt", "gpuBindingClearFinishedAt")
	t.duration(durations, "clear", "clearStartedAt", "clearFinishedAt")
	t.duration(durations, "migApply", "slotsApplyStartedAt", "slotsApplyFinishedAt")
	t.duration(durations, "refreshCDI", "cdiRefreshStartedAt", "cdiRefreshFinishedAt")
	t.duration(durations, "resolveMIGUUIDs", "migUUIDResolveStartedAt", "migUUIDResolveFinishedAt")
	t.duration(durations, "uuidResourcePropagationAndStableWait", "allocatableWaitStartedAt", "allocatableWaitFinishedAt")
	t.duration(durations, "runtimeDeploymentCreate", "runtimeDeploymentCreateStartedAt", "runtimeDeploymentCreatedAt")
	t.duration(durations, "runtimeReadyAndCUDAVerify", "runtimeDeploymentCreatedAt", "runtimeReadyAndCUDAVerifiedAt")
	t.duration(durations, "batchApply", "batchApplyStartedAt", "batchApplyFinishedAt")
	t.duration(durations, "batchVerify", "batchVerifyStartedAt", "batchVerifyFinishedAt")
	t.duration(durations, "routeSync", "runtimeReadyAndCUDAVerifiedAt", "routeSyncedAt")
	t.duration(durations, "returnGPU", "gpuReturnStartedAt", "gpuReturnFinishedAt")
	t.duration(durations, "total", "executorStartedAt", "executorFinishedAt")
	if len(t.metrics) > 0 {
		out["metrics"] = t.metrics
	}
	if verification != nil {
		out["cudaVerification"] = verification
	}
	if t.runtimeReadiness != nil {
		out["runtimeReadiness"] = t.runtimeReadiness
	}
	return out
}

func (t *executionTrace) duration(out map[string]any, key, startName, endName string) {
	start, okStart := t.timestamps[startName]
	end, okEnd := t.timestamps[endName]
	if okStart && okEnd {
		out[key] = end.Sub(start).Seconds()
	}
}

func patchExecutionStatus(client *kube.Client, planName, phase, message string, trace *executionTrace, verification map[string]any) {
	_, err := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", planName)+"/status", map[string]any{
		"status": map[string]any{"phase": phase, "message": message, "transitionExecution": trace.Status(verification)},
	}, nil)
	if err != nil {
		log.Printf("patch execution status for %s failed: %v", planName, err)
	}
}

type actionNode struct {
	ID        string
	Type      string
	Phase     int
	Index     int
	DependsOn []string
	Action    map[string]any
}

func executeActionDAG(client *kube.Client, router string, nodes map[string]string, runtimes []system.ModelRuntimeSpec, actions []actionNode, planName string, trace *executionTrace) (map[string]any, []map[string]any, error) {
	verified := map[string]any{}
	statuses := []map[string]any{}
	pending := map[string]actionNode{}
	known := map[string]bool{}
	completed := map[string]bool{}
	blockedOrFailed := map[string]string{}
	for _, node := range actions {
		if node.ID == "" {
			return verified, statuses, fmt.Errorf("action DAG contains a node without id")
		}
		if known[node.ID] {
			return verified, statuses, fmt.Errorf("action DAG contains duplicate node id %q", node.ID)
		}
		pending[node.ID] = node
		known[node.ID] = true
	}
	var firstErr error
	for len(pending) > 0 {
		progress := false
		ready := readyActionIDs(pending)
		for _, id := range ready {
			node := pending[id]
			blockReason := blockedDependencyReason(node, known, completed, blockedOrFailed)
			if blockReason != "" {
				statuses = append(statuses, actionStatus(node, "blocked", 0, blockReason))
				blockedOrFailed[id] = blockReason
				delete(pending, id)
				progress = true
				continue
			}
			if !dependenciesCompleted(node, completed) {
				continue
			}
			start := time.Now()
			if err := executeAction(client, router, nodes, runtimes, node, verified, planName, trace); err != nil {
				duration := time.Since(start).Seconds()
				statuses = append(statuses, actionStatus(node, "failed", duration, err.Error()))
				blockedOrFailed[id] = err.Error()
				if firstErr == nil {
					firstErr = fmt.Errorf("action %s (%s) failed: %w", node.ID, node.Type, err)
				}
			} else {
				statuses = append(statuses, actionStatus(node, "completed", time.Since(start).Seconds(), ""))
				completed[id] = true
			}
			delete(pending, id)
			progress = true
		}
		if progress {
			continue
		}
		for _, id := range readyActionIDs(pending) {
			node := pending[id]
			reason := "blocked by unresolved dependency cycle"
			if blockReason := blockedDependencyReason(node, known, completed, blockedOrFailed); blockReason != "" {
				reason = blockReason
			}
			statuses = append(statuses, actionStatus(node, "blocked", 0, reason))
			blockedOrFailed[id] = reason
			delete(pending, id)
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("action DAG could not make progress; remaining actions were blocked")
		}
	}
	if firstErr != nil {
		return verified, statuses, firstErr
	}
	return verified, statuses, nil
}

func readyActionIDs(pending map[string]actionNode) []string {
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := pending[ids[i]], pending[ids[j]]
		if a.Phase != b.Phase {
			return a.Phase < b.Phase
		}
		if a.Index != b.Index {
			return a.Index < b.Index
		}
		return a.ID < b.ID
	})
	return ids
}

func dependenciesCompleted(node actionNode, completed map[string]bool) bool {
	for _, dep := range node.DependsOn {
		if !completed[dep] {
			return false
		}
	}
	return true
}

func blockedDependencyReason(node actionNode, known, completed map[string]bool, blockedOrFailed map[string]string) string {
	for _, dep := range node.DependsOn {
		if !known[dep] {
			return "blocked by missing dependency " + dep
		}
		if reason := blockedOrFailed[dep]; reason != "" {
			return "blocked by dependency " + dep + ": " + reason
		}
		if !completed[dep] {
			continue
		}
	}
	return ""
}

func actionStatus(node actionNode, status string, duration float64, message string) map[string]any {
	out := map[string]any{
		"id": node.ID, "type": node.Type, "phase": node.Phase,
		"status": status, "durationSeconds": duration,
	}
	if len(node.DependsOn) > 0 {
		out["dependsOn"] = node.DependsOn
	}
	if message != "" {
		out["message"] = message
	}
	return out
}

func executeAction(client *kube.Client, router string, nodes map[string]string, runtimes []system.ModelRuntimeSpec, node actionNode, verified map[string]any, planName string, trace *executionTrace) error {
	action := node.Action
	actionType := node.Type
	physicalID := firstNonEmpty(asString(action["physical_gpu_id"]), asString(action["physicalGpuId"]), asString(action["gpu"]))
	gpuRuntimes := runtimesForGPU(runtimes, physicalID)
	switch actionType {
	case "allocate_gpu":
		return updateLogicalBinding(client, planName, action, "pending")
	case "bind_target_gpu":
		return updateLogicalBinding(client, planName, action, "active")
	case "keep_gpu_layout", "keep_runtime", "validate_target_allocation":
		return nil
	case "stop_accepting_new", "stop_gpu_traffic", "deactivate_route":
		return deleteRoute(router, modelFromAction(action))
	case "wait_drain", "drain_runtime", "mark_draining_instance":
		trace.Mark("drainWaitStartedAt")
		err := waitDrain(router, action, 60*time.Second)
		trace.Mark("drainWaitFinishedAt")
		return err
	case "delete_pods", "delete_runtime":
		model := asString(action["workload"])
		if model == "" {
			model = asString(action["model"])
		}
		if model != "" {
			if err := deleteRuntimeDeployment(client, model); err != nil {
				return err
			}
			return waitForRuntimeModelPodsGone(client, model, 120*time.Second)
		} else if physicalID != "" {
			if err := deleteRuntimeDeploymentsForGPU(client, physicalID); err != nil {
				return err
			}
			return waitForRuntimePodsGone(client, map[string]bool{nodeNameFromPhysicalGPU(physicalID) + "|" + physicalID: true}, 120*time.Second)
		}
		return nil
	case "clear_full_template", "clear_gpu", "clear_template":
		trace.Mark("clearStartedAt")
		err := clearGPUs(nodes, map[string]bool{nodeNameFromPhysicalGPU(physicalID) + "|" + physicalID: true})
		trace.Mark("clearFinishedAt")
		return err
	case "clear_gpu_binding":
		trace.Mark("gpuBindingClearStartedAt")
		err := updateLogicalBinding(client, planName, action, "clearing")
		if err == nil {
			err = clearGPUBinding(client, router, physicalID, action)
		}
		trace.Mark("gpuBindingClearFinishedAt")
		return err
	case "configure_full_template", "apply_slots":
		createSpec := asString(action["createSpec"])
		if createSpec == "" {
			createSpec = slotsToCreateSpec(asSlice(action["slots"]))
		}
		trace.Mark("slotsApplyStartedAt")
		applyResult, err := applySlots(nodes, physicalID, createSpec)
		trace.Mark("slotsApplyFinishedAt")
		trace.SetMetric("applySlotsNodeAgent", nodeAgentTransactionSummary(applyResult))
		return err
	case "configure_partial_profile", "patch_slots":
		trace.Mark("slotsApplyStartedAt")
		patchResult, err := patchSlots(nodes, physicalID, asString(action["deleteSpec"]), asString(action["createSpec"]), asString(action["preserveSpec"]))
		trace.Mark("slotsApplyFinishedAt")
		trace.SetMetric("patchSlotsNodeAgent", nodeAgentTransactionSummary(patchResult))
		return err
	case "register_mig_devices", "refresh_slot_resources":
		trace.Mark("cdiRefreshStartedAt")
		refreshResult, err := refreshCDI(nodes, physicalID)
		trace.SetMetric("refreshCDINodeAgent", nodeAgentTransactionSummary(refreshResult))
		if err != nil {
			trace.Mark("cdiRefreshFinishedAt")
			return err
		}
		trace.Mark("cdiRefreshFinishedAt")
		trace.Mark("migUUIDResolveStartedAt")
		resolved, err := resolveRuntimeDeviceBindings(nodes, gpuRuntimes)
		if err != nil {
			trace.Mark("migUUIDResolveFinishedAt")
			return err
		}
		trace.Mark("migUUIDResolveFinishedAt")
		targets := allocatableTargets(resolved)
		trace.SetMetric("uuidResourceTargetCount", len(targets))
		transactionReady, transactionMissing := nodeAgentRegisteredTargets(refreshResult, targets)
		trace.SetMetric("nodeAgentRegisteredTargetResources", transactionReady)
		trace.SetMetric("nodeAgentRegisteredTargetMissing", transactionMissing)
		trace.Mark("allocatableWaitStartedAt")
		waitMetrics, err := waitForAllocatableTargets(client, targets, 2*time.Second, 1)
		trace.Mark("allocatableWaitFinishedAt")
		for key, value := range waitMetrics {
			trace.SetMetric(key, value)
		}
		if err != nil && transactionReady {
			trace.SetMetric("allocatableWaitBypassedAfterNodeAgentRegistration", true)
			trace.SetMetric("allocatableWaitBypassReason", err.Error())
			return nil
		}
		return err
	case "deploy_target_workloads", "place_instance", "bridge_place_instance", "workload_change", "create_runtime":
		resolved, err := resolveRuntimeDeviceBindings(nodes, gpuRuntimes)
		if err != nil {
			return err
		}
		trace.Mark("runtimeDeploymentCreateStartedAt")
		err = syncRuntimes(client, resolved)
		trace.Mark("runtimeDeploymentCreatedAt")
		return err
	case "activate_serving_route", "sync_route", "verify_cuda_binding":
		if !trace.Has("runtimeDeploymentCreatedAt") {
			trace.Mark("runtimeDeploymentCreatedAt")
		}
		resolved, err := resolveRuntimeDeviceBindings(nodes, gpuRuntimes)
		if err != nil {
			return err
		}
		perGPUVerification, readiness, err := waitForRuntimeReadyAndCUDA(client, nodes, resolved, trace.timestamps["runtimeDeploymentCreatedAt"], 180*time.Second)
		if err != nil {
			return err
		}
		trace.SetRuntimeReadiness(readiness)
		for key, value := range perGPUVerification {
			verified[key] = value
		}
		trace.Mark("runtimeReadyAndCUDAVerifiedAt")
		if actionType != "verify_cuda_binding" {
			if err := syncRoutes(router, resolved, nodes); err != nil {
				return err
			}
			trace.Mark("routeSyncedAt")
		}
		return nil
	case "patch_batch_config":
		target, err := batchTarget(action, runtimes)
		if err != nil {
			return err
		}
		trace.SetMetric("batchPatchTarget."+target.Model, target.BatchSize)
		return nil
	case "apply_batch":
		target, err := batchTarget(action, runtimes)
		if err != nil {
			return err
		}
		trace.Mark("batchApplyStartedAt")
		err = applyBatch(router, target)
		trace.Mark("batchApplyFinishedAt")
		return err
	case "verify_batch":
		target, err := batchTarget(action, runtimes)
		if err != nil {
			return err
		}
		trace.Mark("batchVerifyStartedAt")
		err = verifyBatch(router, target, 30*time.Second)
		trace.Mark("batchVerifyFinishedAt")
		return err
	case "return_gpu":
		trace.Mark("gpuReturnStartedAt")
		err := verifyGPUReturned(client, nodes, physicalID, 60*time.Second)
		if err == nil {
			err = updateLogicalBinding(client, planName, action, "returned")
		}
		trace.Mark("gpuReturnFinishedAt")
		return err
	default:
		return fmt.Errorf("unsupported action type %q in %s", actionType, node.ID)
	}
}

func runtimesForGPU(runtimes []system.ModelRuntimeSpec, gpu string) []system.ModelRuntimeSpec {
	out := []system.ModelRuntimeSpec{}
	for _, rt := range runtimes {
		if gpu == "" || rt.GPU == gpu {
			out = append(out, rt)
		}
	}
	return out
}

func modelFromAction(action map[string]any) string {
	return firstNonEmpty(asString(action["workload"]), asString(action["model"]), asString(action["runtime"]), asString(action["target"]))
}

func updateLogicalBinding(client *kube.Client, planName string, action map[string]any, phase string) error {
	physicalID := firstNonEmpty(asString(action["physical_gpu_id"]), asString(action["physicalGpuId"]), asString(action["gpu"]))
	if physicalID == "" {
		return fmt.Errorf("%s binding action requires physical_gpu_id", phase)
	}
	ledger := loadLogicalBindingLedger(client)
	bindings := asMap(ledger["bindings"])
	entry := asMap(bindings[physicalID])
	logicalID := logicalIDFromAction(action)
	if logicalID == "" {
		logicalID = firstNonEmpty(asString(entry["activeLogicalGpuId"]), asString(entry["pendingLogicalGpuId"]), asString(entry["logicalGpuId"]))
	}
	now := time.Now().Format(time.RFC3339Nano)
	switch phase {
	case "pending":
		if logicalID == "" {
			return fmt.Errorf("allocate_gpu for %s requires logical gpu id", physicalID)
		}
		entry["physicalGpuId"] = physicalID
		entry["logicalGpuId"] = logicalID
		entry["pendingLogicalGpuId"] = logicalID
		entry["state"] = "pending"
	case "active":
		if logicalID == "" {
			return fmt.Errorf("bind_target_gpu for %s requires logical gpu id", physicalID)
		}
		entry["physicalGpuId"] = physicalID
		entry["logicalGpuId"] = logicalID
		entry["activeLogicalGpuId"] = logicalID
		delete(entry, "pendingLogicalGpuId")
		entry["state"] = "active"
	case "clearing":
		if logicalID == "" {
			return fmt.Errorf("clear_gpu_binding for %s requires logical gpu id", physicalID)
		}
		entry["physicalGpuId"] = physicalID
		entry["logicalGpuId"] = logicalID
		delete(entry, "activeLogicalGpuId")
		entry["pendingLogicalGpuId"] = logicalID
		entry["state"] = "clearing"
	case "returned":
		delete(bindings, physicalID)
	default:
		return fmt.Errorf("unsupported logical binding ledger phase %q", phase)
	}
	if phase != "returned" {
		entry["lastPlanName"] = planName
		entry["lastActionType"] = asString(action["type"])
		entry["updatedAt"] = now
		bindings[physicalID] = entry
	}
	ledger["bindings"] = bindings
	ledger["updatedAt"] = now
	ledger["lastPlanName"] = planName
	return persistLogicalBindingLedger(client, ledger)
}

func logicalIDFromAction(action map[string]any) string {
	for _, key := range []string{"logical_gpu_id", "logicalGpuId", "gpu_id", "gpuId"} {
		if value := asString(action[key]); value != "" {
			return value
		}
		if value, ok := intString(action[key]); ok {
			return value
		}
	}
	return ""
}

func persistFinalLogicalBindings(client *kube.Client, planName string, spec map[string]any) error {
	physicalByLogical := finalPhysicalIDMap(spec)
	if len(physicalByLogical) == 0 {
		return nil
	}
	now := time.Now().Format(time.RFC3339Nano)
	ledger := loadLogicalBindingLedger(client)
	bindings := map[string]any{}
	for logicalID, physicalID := range physicalByLogical {
		if physicalID == "" {
			continue
		}
		bindings[physicalID] = map[string]any{
			"physicalGpuId":      physicalID,
			"logicalGpuId":       logicalID,
			"activeLogicalGpuId": logicalID,
			"state":              "active",
			"lastPlanName":       planName,
			"lastActionType":     "finalize_logical_bindings",
			"updatedAt":          now,
		}
	}
	if len(bindings) == 0 {
		return nil
	}
	ledger["bindings"] = bindings
	ledger["updatedAt"] = now
	ledger["lastPlanName"] = planName
	return persistLogicalBindingLedger(client, ledger)
}

func finalPhysicalIDMap(spec map[string]any) map[string]string {
	targetState := asMap(asMap(asMap(spec["validationTargets"])["targetAllocationPlan"])["targetState"])
	out := stringMap(asMap(targetState["metadata"]), "physical_id_map")
	if len(out) > 0 {
		return out
	}
	planningTrace := asMap(asMap(spec["plannerMetadata"])["planningTrace"])
	return stringMap(asMap(planningTrace["canonicalization"]), "canonicalPhysicalIds")
}

func stringMap(parent map[string]any, key string) map[string]string {
	out := map[string]string{}
	for logicalID, rawPhysicalID := range asMap(parent[key]) {
		physicalID := asString(rawPhysicalID)
		if physicalID != "" {
			out[logicalID] = physicalID
		}
	}
	return out
}

func loadLogicalBindingLedger(client *kube.Client) map[string]any {
	var cm map[string]any
	status, err := client.Get(configMapPath(client.Namespace(), "logical-gpu-binding-ledger"), &cm)
	if err != nil || status != http.StatusOK {
		return map[string]any{"version": "migrant.logical-binding-ledger/v1", "bindings": map[string]any{}}
	}
	raw := strings.TrimSpace(asString(asMap(cm["data"])["ledger.json"]))
	if raw == "" {
		return map[string]any{"version": "migrant.logical-binding-ledger/v1", "bindings": map[string]any{}}
	}
	var ledger map[string]any
	if err := json.Unmarshal([]byte(raw), &ledger); err != nil {
		return map[string]any{"version": "migrant.logical-binding-ledger/v1", "bindings": map[string]any{}}
	}
	if asMap(ledger["bindings"]) == nil {
		ledger["bindings"] = map[string]any{}
	}
	return ledger
}

func persistLogicalBindingLedger(client *kube.Client, ledger map[string]any) error {
	raw, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	body := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "logical-gpu-binding-ledger",
			"namespace": client.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":      "migrant-go",
				"app.kubernetes.io/component": "logical-binding-ledger",
				"migrant.io/state-kind":       "logical-binding-ledger",
			},
		},
		"data": map[string]any{
			"ledger.json": string(raw),
			"updatedAt":   time.Now().Format(time.RFC3339Nano),
		},
	}
	return client.Upsert(configMapPath(client.Namespace(), "logical-gpu-binding-ledger"), body, nil)
}

func configMapPath(ns, name string) string {
	return "/api/v1/namespaces/" + ns + "/configmaps/" + name
}

func waitDrain(router string, action map[string]any, timeout time.Duration) error {
	model := modelFromAction(action)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		routes, err := routeSnapshots(router)
		if err != nil {
			return err
		}
		found := false
		draining := []string{}
		for _, route := range routes {
			if model != "" && asString(route["model"]) != model {
				continue
			}
			found = true
			if intNumber(route["inflight"]) > 0 || intNumber(route["queued"]) > 0 {
				draining = append(draining, fmt.Sprintf("%s(inflight=%d,queued=%d)", asString(route["model"]), intNumber(route["inflight"]), intNumber(route["queued"])))
			}
		}
		if !found || len(draining) == 0 {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for drain of %s", firstNonEmpty(model, "all routed runtimes"))
}

func clearGPUBinding(client *kube.Client, router, physicalID string, action map[string]any) error {
	if model := modelFromAction(action); model != "" {
		if err := deleteRoute(router, model); err != nil {
			return err
		}
	}
	if physicalID == "" {
		return nil
	}
	if err := waitForRuntimePodsGone(client, map[string]bool{nodeNameFromPhysicalGPU(physicalID) + "|" + physicalID: true}, 120*time.Second); err != nil {
		return err
	}
	runtimes, err := runtimeDeploymentsForGPU(client, physicalID)
	if err != nil {
		return err
	}
	if len(runtimes) > 0 {
		return fmt.Errorf("cannot clear GPU binding for %s while runtime deployments remain: %s", physicalID, strings.Join(runtimes, ","))
	}
	return nil
}

func batchTarget(action map[string]any, runtimes []system.ModelRuntimeSpec) (system.ModelRuntimeSpec, error) {
	model := modelFromAction(action)
	batch := intNumber(firstNonNil(action["batchSize"], action["targetBatchSize"], action["newBatchSize"]))
	for _, rt := range runtimes {
		if model != "" && rt.Model != model {
			continue
		}
		target := rt
		if batch > 0 {
			target.BatchSize = batch
		}
		if target.BatchSize <= 0 {
			return target, fmt.Errorf("batch action for %s has no positive target batchSize", target.Model)
		}
		return target, nil
	}
	if model == "" {
		return system.ModelRuntimeSpec{}, fmt.Errorf("batch action requires workload/model when plan has no runtime target")
	}
	return system.ModelRuntimeSpec{}, fmt.Errorf("batch action target runtime %q not found in plan", model)
}

func applyBatch(router string, target system.ModelRuntimeSpec) error {
	endpoint, err := routeEndpoint(router, target.Model)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"batchSize": target.BatchSize})
	resp, err := http.Post(endpoint+"/control/batch", "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("batch apply for %s returned %d", target.Model, resp.StatusCode)
	}
	return nil
}

func verifyBatch(router string, target system.ModelRuntimeSpec, timeout time.Duration) error {
	endpoint, err := routeEndpoint(router, target.Model)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		metrics, err := getJSON(endpoint + "/metrics")
		if err == nil && intNumber(metrics["batchSize"]) == target.BatchSize {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timed out verifying batchSize=%d for %s", target.BatchSize, target.Model)
}

func routeEndpoint(router, model string) (string, error) {
	routes, err := routeSnapshots(router)
	if err != nil {
		return "", err
	}
	for _, route := range routes {
		if asString(route["model"]) == model {
			endpoint := strings.TrimRight(asString(route["endpoint"]), "/")
			if endpoint == "" {
				return "", fmt.Errorf("route for %s has empty endpoint", model)
			}
			return endpoint, nil
		}
	}
	return "", fmt.Errorf("route for %s not found", model)
}

func routeSnapshots(router string) ([]map[string]any, error) {
	payload, err := getJSON(strings.TrimRight(router, "/") + "/routes")
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, raw := range asSlice(payload["routes"]) {
		out = append(out, asMap(raw))
	}
	return out, nil
}

func verifyGPUReturned(client *kube.Client, nodes map[string]string, physicalID string, timeout time.Duration) error {
	if physicalID == "" {
		return fmt.Errorf("return_gpu requires physical_gpu_id")
	}
	key := map[string]bool{nodeNameFromPhysicalGPU(physicalID) + "|" + physicalID: true}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := waitForRuntimePodsGone(client, key, time.Second); err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		empty, err := gpuHasNoMIGSlots(nodes, physicalID)
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s to return to available empty state", physicalID)
}

func gpuHasNoMIGSlots(nodes map[string]string, physicalID string) (bool, error) {
	_, gpuIndex, ip, err := resolvePhysicalGPU(nodes, physicalID)
	if err != nil {
		return false, err
	}
	payload, err := getJSON(fmt.Sprintf("http://%s:10684/list?gpuIndex=%d", ip, gpuIndex))
	if err != nil {
		return false, err
	}
	return len(asSlice(payload["migSlots"])) == 0, nil
}

func runtimeDeploymentsForGPU(client *kube.Client, gpu string) ([]string, error) {
	var list map[string]any
	if _, err := client.Get(kube.Deployments(client.Namespace()), &list); err != nil {
		return nil, err
	}
	out := []string{}
	for _, item := range asSlice(list["items"]) {
		dep := asMap(item)
		meta := asMap(dep["metadata"])
		labels := asMap(meta["labels"])
		if asString(labels["app.kubernetes.io/name"]) == "migrant-model-runtime" && asString(labels["migrant.io/gpu"]) == gpu {
			out = append(out, asString(meta["name"]))
		}
	}
	sort.Strings(out)
	return out, nil
}

func deleteRuntimeDeployment(client *kube.Client, model string) error {
	if model == "" {
		return nil
	}
	var list map[string]any
	if _, err := client.Get(kube.Deployments(client.Namespace()), &list); err != nil {
		return err
	}
	for _, item := range asSlice(list["items"]) {
		dep := asMap(item)
		meta := asMap(dep["metadata"])
		labels := asMap(meta["labels"])
		if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" || asString(labels["migrant.io/model"]) != model {
			continue
		}
		name := asString(meta["name"])
		if status, err := client.Delete(kube.Deployment(client.Namespace(), name)); err != nil && status != http.StatusNotFound {
			return err
		}
	}
	return nil
}

func deleteRuntimeDeploymentsForGPU(client *kube.Client, gpu string) error {
	var list map[string]any
	if _, err := client.Get(kube.Deployments(client.Namespace()), &list); err != nil {
		return err
	}
	for _, item := range asSlice(list["items"]) {
		dep := asMap(item)
		meta := asMap(dep["metadata"])
		labels := asMap(meta["labels"])
		if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" || asString(labels["migrant.io/gpu"]) != gpu {
			continue
		}
		name := asString(meta["name"])
		if status, err := client.Delete(kube.Deployment(client.Namespace(), name)); err != nil && status != http.StatusNotFound {
			return err
		}
	}
	return nil
}

func applySlots(nodes map[string]string, physicalID, createSpec string) (map[string]any, error) {
	if createSpec == "" {
		return nil, fmt.Errorf("apply slots for %s requires createSpec", physicalID)
	}
	node, gpuIndex, ip, err := resolvePhysicalGPU(nodes, physicalID)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(map[string]any{"create": createSpec})
	resp, err := http.Post(fmt.Sprintf("http://%s:10684/apply-slots?gpuIndex=%d", ip, gpuIndex), "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := decodeNodeAgentResponse(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("apply slots on %s gpu%d returned %d", node, gpuIndex, resp.StatusCode)
	}
	return body, nil
}

func patchSlots(nodes map[string]string, physicalID, deleteSpec, createSpec, preserveSpec string) (map[string]any, error) {
	node, gpuIndex, ip, err := resolvePhysicalGPU(nodes, physicalID)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(map[string]any{"delete": deleteSpec, "create": createSpec, "preserve": preserveSpec})
	resp, err := http.Post(fmt.Sprintf("http://%s:10684/patch-slots?gpuIndex=%d", ip, gpuIndex), "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := decodeNodeAgentResponse(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("patch slots on %s gpu%d returned %d", node, gpuIndex, resp.StatusCode)
	}
	return body, nil
}

func refreshCDI(nodes map[string]string, physicalID string) (map[string]any, error) {
	node, gpuIndex, ip, err := resolvePhysicalGPU(nodes, physicalID)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s:10684/refresh-cdi?gpuIndex=%d", ip, gpuIndex), "application/json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := decodeNodeAgentResponse(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("refresh CDI on %s gpu%d returned %d", node, gpuIndex, resp.StatusCode)
	}
	return body, nil
}

func decodeNodeAgentResponse(resp *http.Response) map[string]any {
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return map[string]any{"decodeError": err.Error()}
	}
	return body
}

func nodeAgentTransactionSummary(body map[string]any) map[string]any {
	if body == nil {
		return nil
	}
	out := map[string]any{
		"success":           body["success"],
		"command":           body["command"],
		"gpuIndex":          body["gpuIndex"],
		"createSeconds":     body["createSeconds"],
		"deleteSeconds":     body["deleteSeconds"],
		"expectedResources": body["expectedResources"],
		"migSlotCount":      len(asSlice(body["migSlots"])),
	}
	refresh := asMap(body["devicePluginRefresh"])
	if len(refresh) > 0 {
		out["devicePluginRefresh"] = map[string]any{
			"success":             refresh["success"],
			"seconds":             refresh["seconds"],
			"slotCount":           refresh["slotCount"],
			"registeredResources": refresh["registeredResources"],
			"socket":              refresh["socket"],
			"statusCode":          refresh["statusCode"],
			"error":               refresh["error"],
		}
	}
	return out
}

func resolvePhysicalGPU(nodes map[string]string, physicalID string) (string, int, string, error) {
	node := nodeNameFromPhysicalGPU(physicalID)
	if node == "" {
		return "", 0, "", fmt.Errorf("cannot infer node from physical GPU %q", physicalID)
	}
	ip := nodes[node]
	if ip == "" {
		return "", 0, "", fmt.Errorf("node IP not found for %s", node)
	}
	gpuIndex, err := gpuIndexFromID(physicalID)
	if err != nil {
		return "", 0, "", err
	}
	return node, gpuIndex, ip, nil
}

func nodeNameFromPhysicalGPU(physicalID string) string {
	if idx := strings.LastIndex(physicalID, "-gpu"); idx > 0 {
		return physicalID[:idx]
	}
	return ""
}

func slotsToCreateSpec(values []any) string {
	parts := []string{}
	for _, raw := range values {
		slot := asMap(raw)
		start := intNumber(slot["start"])
		end := intNumber(slot["end"])
		profile := asString(slot["profile"])
		if end > start && profile != "" {
			parts = append(parts, fmt.Sprintf("%d:%d:%s", start, end-start, profile))
		}
	}
	return strings.Join(parts, ",")
}

func prepareRuntimeMutation(client *kube.Client, desired []system.ModelRuntimeSpec) ([]string, map[string]bool, error) {
	desiredByName := map[string]system.ModelRuntimeSpec{}
	satisfied := map[string]bool{}
	for _, rt := range desired {
		desiredByName[runtimeDeploymentName(rt)] = rt
	}
	var list map[string]any
	if _, err := client.Get(kube.Deployments(client.Namespace()), &list); err != nil {
		return nil, nil, err
	}
	deleted := []string{}
	gpusToClear := map[string]bool{}
	for _, item := range asSlice(list["items"]) {
		dep := asMap(item)
		meta := asMap(dep["metadata"])
		labels := asMap(meta["labels"])
		name := asString(meta["name"])
		if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" {
			continue
		}
		current := runtimeFromDeployment(dep)
		desiredRT, keep := desiredByName[name]
		if keep && runtimeEquivalent(current, desiredRT) {
			satisfied[name] = true
			continue
		}
		if status, err := client.Delete(kube.Deployment(client.Namespace(), name)); err != nil && status != http.StatusNotFound {
			return nil, nil, err
		}
		deleted = append(deleted, asString(labels["migrant.io/model"]))
		if current.GPU != "" {
			gpusToClear[current.Node+"|"+current.GPU] = true
		}
		if keep && desiredRT.GPU != "" {
			gpusToClear[desiredRT.Node+"|"+desiredRT.GPU] = true
		}
	}
	for _, rt := range desired {
		if rt.SlotResource != "" && !satisfied[runtimeDeploymentName(rt)] {
			gpusToClear[rt.Node+"|"+rt.GPU] = true
		}
	}
	return deleted, gpusToClear, nil
}

func runtimeFromDeployment(dep map[string]any) system.ModelRuntimeSpec {
	spec := asMap(dep["spec"])
	template := asMap(spec["template"])
	meta := asMap(template["metadata"])
	podSpec := asMap(template["spec"])
	labels := asMap(meta["labels"])
	annotations := asMap(meta["annotations"])
	rt := system.ModelRuntimeSpec{
		Model:           asString(labels["migrant.io/model"]),
		RuntimeID:       asString(labels["migrant.io/runtime-id"]),
		Node:            asString(asMap(podSpec["nodeSelector"])["kubernetes.io/hostname"]),
		Profile:         asString(labels["migrant.io/profile"]),
		GPU:             asString(labels["migrant.io/gpu"]),
		SlotResource:    asString(annotations["migrant.io/slot-resource"]),
		DeviceResource:  asString(annotations["migrant.io/device-resource"]),
		ExpectedMIGUUID: asString(annotations["migrant.io/expected-mig-uuid"]),
		Weight:          asFloat(annotations["migrant.io/route-weight"]),
		Capacity:        asFloat(annotations["migrant.io/capacity"]),
	}
	for _, container := range asSlice(podSpec["containers"]) {
		c := asMap(container)
		for _, env := range asSlice(c["env"]) {
			e := asMap(env)
			switch asString(e["name"]) {
			case "BATCH_SIZE":
				rt.BatchSize = intNumber(e["value"])
			}
		}
		for _, port := range asSlice(c["ports"]) {
			rt.HostPort = intNumber(asMap(port)["containerPort"])
			break
		}
	}
	return rt
}

func runtimeEquivalent(a, b system.ModelRuntimeSpec) bool {
	return runtimeID(a) == runtimeID(b) && a.Model == b.Model && a.BatchSize == b.BatchSize && a.Node == b.Node &&
		a.HostPort == b.HostPort && a.Profile == b.Profile && a.GPU == b.GPU &&
		a.SlotResource == b.SlotResource && a.DeviceResource == b.DeviceResource &&
		a.ExpectedMIGUUID == b.ExpectedMIGUUID && floatEqual(routeWeight(a), routeWeight(b)) && floatEqual(a.Capacity, b.Capacity)
}

func waitForRuntimePodsGone(client *kube.Client, gpus map[string]bool, timeout time.Duration) error {
	if len(gpus) == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var pods map[string]any
		if _, err := client.Get(kube.Pods(client.Namespace()), &pods); err != nil {
			return err
		}
		found := false
		for _, item := range asSlice(pods["items"]) {
			pod := asMap(item)
			meta := asMap(pod["metadata"])
			labels := asMap(meta["labels"])
			if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" {
				continue
			}
			spec := asMap(pod["spec"])
			key := asString(spec["nodeName"]) + "|" + asString(labels["migrant.io/gpu"])
			if gpus[key] {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for runtime pods to leave GPUs %v", gpus)
}

func waitForRuntimeModelPodsGone(client *kube.Client, model string, timeout time.Duration) error {
	if model == "" {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var pods map[string]any
		if _, err := client.Get(kube.Pods(client.Namespace()), &pods); err != nil {
			return err
		}
		found := false
		for _, item := range asSlice(pods["items"]) {
			pod := asMap(item)
			meta := asMap(pod["metadata"])
			labels := asMap(meta["labels"])
			if asString(labels["app.kubernetes.io/name"]) == "migrant-model-runtime" && asString(labels["migrant.io/model"]) == model {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for runtime pods for model %s to terminate", model)
}

func clearGPUs(nodes map[string]string, gpus map[string]bool) error {
	for key := range gpus {
		parts := strings.Split(key, "|")
		if len(parts) != 2 {
			continue
		}
		node, gpuID := parts[0], parts[1]
		ip := nodes[node]
		if ip == "" {
			return fmt.Errorf("node IP not found for %s", node)
		}
		gpuIndex, err := gpuIndexFromID(gpuID)
		if err != nil {
			return err
		}
		resp, err := http.Post(fmt.Sprintf("http://%s:10684/clear?gpuIndex=%d", ip, gpuIndex), "application/json", nil)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("clear %s %s returned %d", node, gpuID, resp.StatusCode)
		}
	}
	return nil
}

func gpuIndexFromID(gpuID string) (int, error) {
	pos := strings.LastIndex(gpuID, "gpu")
	if pos < 0 {
		return 0, fmt.Errorf("invalid gpu id %q", gpuID)
	}
	return strconv.Atoi(gpuID[pos+3:])
}

func syncRuntimes(client *kube.Client, runtimes []system.ModelRuntimeSpec) error {
	for _, rt := range runtimes {
		if rt.DeviceResource == "" || rt.ExpectedMIGUUID == "" {
			return fmt.Errorf("runtime %s missing resolved per-MIG UUID device binding for slot %s", rt.Model, rt.SlotResource)
		}
		body := deployment(client.Namespace(), rt)
		if err := client.Upsert(kube.Deployment(client.Namespace(), runtimeDeploymentName(rt)), body, nil); err != nil {
			return err
		}
	}
	return nil
}

func resolveRuntimeDeviceBindings(nodes map[string]string, runtimes []system.ModelRuntimeSpec) ([]system.ModelRuntimeSpec, error) {
	out := make([]system.ModelRuntimeSpec, 0, len(runtimes))
	for _, rt := range runtimes {
		resolved, err := resolveRuntimeDeviceBinding(nodes, rt)
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return out, nil
}

func resolveRuntimeDeviceBinding(nodes map[string]string, rt system.ModelRuntimeSpec) (system.ModelRuntimeSpec, error) {
	slot, err := parseSlotRequest(rt)
	if err != nil {
		return rt, err
	}
	ip := nodes[rt.Node]
	if ip == "" {
		return rt, fmt.Errorf("node IP not found for runtime %s on %s", rt.Model, rt.Node)
	}
	payload, err := getJSON(fmt.Sprintf("http://%s:10684/list?gpuIndex=%d", ip, slot.GPUIndex))
	if err != nil {
		return rt, err
	}
	for _, raw := range asSlice(payload["migSlots"]) {
		item := asMap(raw)
		if intNumber(item["slotStart"]) != slot.Start || intNumber(item["slotEnd"]) != slot.End || asString(item["profile"]) != slot.Profile {
			continue
		}
		uuid := asString(item["migDeviceUuid"])
		if uuid == "" {
			return rt, fmt.Errorf("slot %s matched but has no MIG UUID", rt.SlotResource)
		}
		rt.ExpectedMIGUUID = uuid
		rt.DeviceResource = migUUIDResourceName(uuid)
		return rt, nil
	}
	return rt, fmt.Errorf("slot %s not found in node-agent observation for %s gpu%d", rt.SlotResource, rt.Node, slot.GPUIndex)
}

func migUUIDResourceName(migUUID string) string {
	uuid := strings.ToLower(strings.TrimPrefix(migUUID, "MIG-"))
	return "or-sim.io/mig-" + uuid
}

func ensureSlotResources(client *kube.Client, nodes map[string]string, runtimes []system.ModelRuntimeSpec) error {
	groups := map[string][]slotRequest{}
	for _, rt := range runtimes {
		if rt.SlotResource == "" {
			continue
		}
		slot, err := parseSlotRequest(rt)
		if err != nil {
			return err
		}
		key := rt.Node + "|" + strconv.Itoa(slot.GPUIndex)
		groups[key] = append(groups[key], slot)
	}
	for key, slots := range groups {
		parts := strings.Split(key, "|")
		node := parts[0]
		gpuIndex, _ := strconv.Atoi(parts[1])
		ip := nodes[node]
		if ip == "" {
			return fmt.Errorf("node IP not found for slot resource node %s", node)
		}
		create := []string{}
		for _, slot := range slots {
			create = append(create, fmt.Sprintf("%d:%d:%s", slot.Start, slot.End-slot.Start, slot.Profile))
		}
		raw, _ := json.Marshal(map[string]any{"create": strings.Join(create, ",")})
		url := fmt.Sprintf("http://%s:10684/apply-slots?gpuIndex=%d", ip, gpuIndex)
		resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("apply slots on %s gpu%d returned %d", node, gpuIndex, resp.StatusCode)
		}
		refreshURL := fmt.Sprintf("http://%s:10684/refresh-cdi?gpuIndex=%d", ip, gpuIndex)
		refreshResp, err := http.Post(refreshURL, "application/json", nil)
		if err != nil {
			return err
		}
		refreshResp.Body.Close()
		if refreshResp.StatusCode < 200 || refreshResp.StatusCode >= 300 {
			return fmt.Errorf("refresh CDI on %s gpu%d returned %d", node, gpuIndex, refreshResp.StatusCode)
		}
	}
	for _, rt := range runtimes {
		if rt.SlotResource == "" {
			continue
		}
		if err := waitForAllocatable(client, rt.Node, rt.SlotResource, 90*time.Second); err != nil {
			return err
		}
	}
	return nil
}

type slotRequest struct {
	GPUIndex int
	Start    int
	End      int
	Profile  string
}

type allocatableTarget struct {
	Node     string
	Resource string
}

func parseSlotRequest(rt system.ModelRuntimeSpec) (slotRequest, error) {
	token := rt.SlotResource
	if slash := strings.LastIndex(token, "/"); slash >= 0 {
		token = token[slash+1:]
	}
	gpuMarker := "gpu"
	gpuPos := strings.LastIndex(rt.GPU, gpuMarker)
	if gpuPos < 0 {
		return slotRequest{}, fmt.Errorf("runtime %s has invalid gpu id %q", rt.Model, rt.GPU)
	}
	gpuIndex, err := strconv.Atoi(rt.GPU[gpuPos+len(gpuMarker):])
	if err != nil {
		return slotRequest{}, fmt.Errorf("runtime %s has invalid gpu index in %q: %w", rt.Model, rt.GPU, err)
	}
	prefix := rt.GPU + "-s"
	if !strings.HasPrefix(token, prefix) {
		return slotRequest{}, fmt.Errorf("slot resource %q does not match gpu %q", rt.SlotResource, rt.GPU)
	}
	parts := strings.Split(strings.TrimPrefix(token, prefix), "-")
	if len(parts) != 3 {
		return slotRequest{}, fmt.Errorf("slot resource %q must end with sSTART-END-PROFILE", rt.SlotResource)
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return slotRequest{}, err
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return slotRequest{}, err
	}
	if end <= start {
		return slotRequest{}, fmt.Errorf("slot resource %q has invalid range", rt.SlotResource)
	}
	return slotRequest{GPUIndex: gpuIndex, Start: start, End: end, Profile: parts[2]}, nil
}

func waitForAllocatable(client *kube.Client, nodeName, resource string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	stable := 0
	for time.Now().Before(deadline) {
		var node map[string]any
		if _, err := client.Get(kube.Node(nodeName), &node); err != nil {
			return err
		}
		status := asMap(node["status"])
		allocatable := asMap(status["allocatable"])
		if asString(allocatable[resource]) != "" && asString(allocatable[resource]) != "0" {
			stable++
			if stable >= 6 {
				return nil
			}
		} else {
			stable = 0
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for allocatable resource %s on %s", resource, nodeName)
}

func allocatableTargets(runtimes []system.ModelRuntimeSpec) []allocatableTarget {
	seen := map[string]bool{}
	out := []allocatableTarget{}
	for _, rt := range runtimes {
		if rt.Node == "" || rt.DeviceResource == "" {
			continue
		}
		key := rt.Node + "|" + rt.DeviceResource
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, allocatableTarget{Node: rt.Node, Resource: rt.DeviceResource})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Resource < out[j].Resource
	})
	return out
}

func waitForAllocatableTargets(client *kube.Client, targets []allocatableTarget, timeout time.Duration, requiredStablePolls int) (map[string]any, error) {
	if requiredStablePolls < 1 {
		requiredStablePolls = 1
	}
	metrics := map[string]any{
		"allocatableStablePollsRequired": requiredStablePolls,
		"allocatablePollIntervalMs":      500,
		"allocatableTimeoutSeconds":      timeout.Seconds(),
	}
	if len(targets) == 0 {
		metrics["allocatablePolls"] = 0
		metrics["allocatableTargets"] = []string{}
		return metrics, nil
	}
	targetsByNode := map[string][]string{}
	targetNames := []string{}
	for _, target := range targets {
		targetsByNode[target.Node] = append(targetsByNode[target.Node], target.Resource)
		targetNames = append(targetNames, target.Node+"/"+target.Resource)
	}
	sort.Strings(targetNames)
	metrics["allocatableTargets"] = targetNames
	deadline := time.Now().Add(timeout)
	stable := 0
	polls := 0
	lastMissing := []string{}
	for time.Now().Before(deadline) {
		polls++
		missing := []string{}
		for nodeName, resources := range targetsByNode {
			var node map[string]any
			if _, err := client.Get(kube.Node(nodeName), &node); err != nil {
				metrics["allocatablePolls"] = polls
				return metrics, err
			}
			allocatable := asMap(asMap(node["status"])["allocatable"])
			for _, resource := range resources {
				if asString(allocatable[resource]) == "" || asString(allocatable[resource]) == "0" {
					missing = append(missing, nodeName+"/"+resource)
				}
			}
		}
		sort.Strings(missing)
		lastMissing = missing
		if len(missing) == 0 {
			stable++
			if stable >= requiredStablePolls {
				metrics["allocatablePolls"] = polls
				metrics["allocatableFinalStablePolls"] = stable
				metrics["allocatableLastMissing"] = lastMissing
				return metrics, nil
			}
		} else {
			stable = 0
		}
		time.Sleep(500 * time.Millisecond)
	}
	metrics["allocatablePolls"] = polls
	metrics["allocatableFinalStablePolls"] = stable
	metrics["allocatableLastMissing"] = lastMissing
	return metrics, fmt.Errorf("timed out waiting for allocatable resources: %s", strings.Join(lastMissing, ", "))
}

func nodeAgentRegisteredTargets(body map[string]any, targets []allocatableTarget) (bool, []string) {
	if len(targets) == 0 {
		return true, nil
	}
	registered := map[string]bool{}
	for _, raw := range asSlice(asMap(body["devicePluginRefresh"])["registeredResources"]) {
		registered[asString(raw)] = true
	}
	if len(registered) == 0 {
		for _, raw := range asSlice(body["expectedResources"]) {
			registered[asString(raw)] = true
		}
	}
	missing := []string{}
	for _, target := range targets {
		if !registered[target.Resource] {
			missing = append(missing, target.Node+"/"+target.Resource)
		}
	}
	sort.Strings(missing)
	return len(missing) == 0, missing
}

type runtimeWaitResult struct {
	runtimeID    string
	verification map[string]any
	readiness    map[string]any
	err          error
}

func waitForRuntimeReadyAndCUDA(client *kube.Client, nodes map[string]string, runtimes []system.ModelRuntimeSpec, deploymentCreatedAt time.Time, timeout time.Duration) (map[string]any, map[string]any, error) {
	deadline := time.Now().Add(timeout)
	ch := make(chan runtimeWaitResult, len(runtimes))
	var wg sync.WaitGroup
	for _, rt := range runtimes {
		rt := rt
		wg.Add(1)
		go func() {
			defer wg.Done()
			verification, readiness, err := waitForOneRuntimeReadyAndCUDA(client, nodes, rt, deploymentCreatedAt, deadline)
			ch <- runtimeWaitResult{runtimeID: runtimeID(rt), verification: verification, readiness: readiness, err: err}
		}()
	}
	wg.Wait()
	close(ch)
	verified := map[string]any{}
	readiness := map[string]any{}
	for result := range ch {
		if result.err != nil {
			return verified, readiness, result.err
		}
		verified[result.runtimeID] = result.verification
		readiness[result.runtimeID] = result.readiness
	}
	return verified, readiness, nil
}

func waitForOneRuntimeReadyAndCUDA(client *kube.Client, nodes map[string]string, rt system.ModelRuntimeSpec, deploymentCreatedAt, deadline time.Time) (map[string]any, map[string]any, error) {
	ip := nodes[rt.Node]
	if ip == "" {
		return nil, nil, fmt.Errorf("node IP not found for runtime %s on %s", rt.Model, rt.Node)
	}
	readiness := map[string]any{
		"node":            rt.Node,
		"gpu":             rt.GPU,
		"slotResource":    rt.SlotResource,
		"deviceResource":  rt.DeviceResource,
		"expectedMigUUID": rt.ExpectedMIGUUID,
		"hostPort":        rt.HostPort,
	}
	pod, err := waitForRuntimePod(client, rt, deadline, readiness, deploymentCreatedAt)
	if err != nil {
		return nil, readiness, err
	}
	readiness["pod"] = asString(asMap(pod["metadata"])["name"])
	var health map[string]any
	for time.Now().Before(deadline) {
		health, err = getJSON("http://" + ip + ":" + strconv.Itoa(rt.HostPort) + "/healthz")
		if err == nil && health["ok"] == true {
			markReadinessTime(readiness, "healthReadyAt", time.Now(), deploymentCreatedAt)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if health == nil || health["ok"] != true {
		return nil, readiness, fmt.Errorf("runtime %s did not become healthy before timeout", rt.Model)
	}
	migUUID := asString(health["orSimMIGUUID"])
	if migUUID == "" {
		return nil, readiness, fmt.Errorf("runtime %s health did not report OR_SIM_MIG_UUID", rt.Model)
	}
	if rt.ExpectedMIGUUID != "" && migUUID != rt.ExpectedMIGUUID {
		return nil, readiness, fmt.Errorf("runtime %s got MIG UUID %s, expected %s", rt.Model, migUUID, rt.ExpectedMIGUUID)
	}
	processes, foundAt, err := waitForCUDAProcess(nodes[rt.Node], migUUID, deadline)
	if err != nil {
		return nil, readiness, fmt.Errorf("runtime %s CUDA verification failed: %w", rt.Model, err)
	}
	markReadinessTime(readiness, "cudaProcessFoundAt", foundAt, deploymentCreatedAt)
	return map[string]any{
		"node":            rt.Node,
		"gpu":             rt.GPU,
		"slotResource":    rt.SlotResource,
		"deviceResource":  rt.DeviceResource,
		"expectedMigUUID": rt.ExpectedMIGUUID,
		"migUUID":         migUUID,
		"processes":       processes,
		"health":          health,
	}, readiness, nil
}

func waitForRuntimePod(client *kube.Client, rt system.ModelRuntimeSpec, deadline time.Time, readiness map[string]any, deploymentCreatedAt time.Time) (map[string]any, error) {
	for time.Now().Before(deadline) {
		pod, ok, err := findRuntimePod(client, rt)
		if err != nil {
			return nil, err
		}
		if ok {
			recordRuntimePodReadiness(pod, readiness, deploymentCreatedAt)
			if runtimeContainerStartedAt(pod) != "" {
				return pod, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("runtime %s pod did not start before timeout", rt.Model)
}

func findRuntimePod(client *kube.Client, rt system.ModelRuntimeSpec) (map[string]any, bool, error) {
	var list map[string]any
	if _, err := client.Get(kube.Pods(client.Namespace()), &list); err != nil {
		return nil, false, err
	}
	var newestStarted map[string]any
	var newestStartedTime time.Time
	var newestCandidate map[string]any
	var newestCandidateTime time.Time
	for _, raw := range asSlice(list["items"]) {
		pod := asMap(raw)
		meta := asMap(pod["metadata"])
		if asString(meta["deletionTimestamp"]) != "" {
			continue
		}
		labels := asMap(meta["labels"])
		if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" || asString(labels["migrant.io/runtime-id"]) != runtimeID(rt) {
			continue
		}
		created := parseKubeTime(asString(meta["creationTimestamp"]))
		if runtimeContainerStartedAt(pod) != "" {
			if newestStarted == nil || created.After(newestStartedTime) {
				newestStarted = pod
				newestStartedTime = created
			}
			continue
		}
		if asString(asMap(pod["status"])["phase"]) == "Failed" || asString(asMap(pod["status"])["reason"]) == "UnexpectedAdmissionError" {
			continue
		}
		if newestCandidate == nil || created.After(newestCandidateTime) {
			newestCandidate = pod
			newestCandidateTime = created
		}
	}
	if newestStarted != nil {
		return newestStarted, true, nil
	}
	return newestCandidate, newestCandidate != nil, nil
}

func recordRuntimePodReadiness(pod map[string]any, readiness map[string]any, deploymentCreatedAt time.Time) {
	meta := asMap(pod["metadata"])
	status := asMap(pod["status"])
	if created := parseKubeTime(asString(meta["creationTimestamp"])); !created.IsZero() {
		markReadinessTime(readiness, "podCreatedAt", created, deploymentCreatedAt)
	}
	if started := parseKubeTime(asString(status["startTime"])); !started.IsZero() {
		markReadinessTime(readiness, "podStartTime", started, deploymentCreatedAt)
	}
	for _, raw := range asSlice(status["conditions"]) {
		condition := asMap(raw)
		if asString(condition["type"]) == "PodScheduled" && asString(condition["status"]) == "True" {
			if scheduled := parseKubeTime(asString(condition["lastTransitionTime"])); !scheduled.IsZero() {
				markReadinessTime(readiness, "podScheduledAt", scheduled, deploymentCreatedAt)
			}
		}
	}
	for _, raw := range asSlice(status["containerStatuses"]) {
		container := asMap(raw)
		if asString(container["name"]) != "runtime" {
			continue
		}
		running := asMap(asMap(container["state"])["running"])
		if started := parseKubeTime(asString(running["startedAt"])); !started.IsZero() {
			markReadinessTime(readiness, "containerStartedAt", started, deploymentCreatedAt)
		}
		readiness["containerReady"] = asBool(container["ready"])
	}
}

func runtimeContainerStartedAt(pod map[string]any) string {
	for _, raw := range asSlice(asMap(pod["status"])["containerStatuses"]) {
		container := asMap(raw)
		if asString(container["name"]) != "runtime" {
			continue
		}
		return asString(asMap(asMap(container["state"])["running"])["startedAt"])
	}
	return ""
}

func markReadinessTime(readiness map[string]any, key string, value time.Time, since time.Time) {
	if value.IsZero() {
		return
	}
	readiness[key] = value.Format(time.RFC3339Nano)
	if !since.IsZero() {
		readiness[key+"SinceDeploymentSeconds"] = value.Sub(since).Seconds()
	}
}

func waitForCUDAProcess(nodeIP, migUUID string, deadline time.Time) ([]any, time.Time, error) {
	for time.Now().Before(deadline) {
		payload, err := getJSON("http://" + nodeIP + ":10684/processes?migUuid=" + migUUID)
		if err == nil {
			processes := asSlice(payload["processes"])
			if len(processes) > 0 {
				return processes, time.Now(), nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, time.Time{}, fmt.Errorf("no active compute process found for %s before timeout", migUUID)
}

func getJSON(url string) (map[string]any, error) {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s returned %d", url, resp.StatusCode)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func deleteStaleRuntimes(client *kube.Client, desired []system.ModelRuntimeSpec) ([]string, error) {
	keep := map[string]bool{}
	for _, rt := range desired {
		keep[runtimeDeploymentName(rt)] = true
	}
	deleted := []string{}
	var list map[string]any
	if _, err := client.Get(kube.Deployments(client.Namespace()), &list); err != nil {
		return nil, err
	}
	for _, item := range asSlice(list["items"]) {
		dep := asMap(item)
		meta := asMap(dep["metadata"])
		labels := asMap(meta["labels"])
		name := asString(meta["name"])
		if asString(labels["app.kubernetes.io/name"]) == "migrant-model-runtime" && !keep[name] {
			if status, err := client.Delete(kube.Deployment(client.Namespace(), name)); err != nil && status != http.StatusNotFound {
				return nil, err
			}
			deleted = append(deleted, asString(labels["migrant.io/model"]))
		}
	}
	return deleted, nil
}

func syncRoutes(router string, runtimes []system.ModelRuntimeSpec, nodes map[string]string) error {
	cleared := map[string]bool{}
	for _, rt := range runtimes {
		if rt.Model == "" || cleared[rt.Model] {
			continue
		}
		if err := deleteRoute(router, rt.Model); err != nil {
			return err
		}
		cleared[rt.Model] = true
	}
	for _, rt := range runtimes {
		ip := nodes[rt.Node]
		if ip == "" {
			return fmt.Errorf("node IP not found for %s", rt.Node)
		}
		raw, _ := json.Marshal(map[string]any{
			"model":           rt.Model,
			"runtimeId":       runtimeID(rt),
			"endpoint":        "http://" + ip + ":" + strconv.Itoa(rt.HostPort),
			"weight":          routeWeight(rt),
			"capacity":        rt.Capacity,
			"profile":         rt.Profile,
			"batchSize":       rt.BatchSize,
			"gpu":             rt.GPU,
			"slotResource":    rt.SlotResource,
			"deviceResource":  rt.DeviceResource,
			"expectedMigUuid": rt.ExpectedMIGUUID,
			"active":          true,
			"acceptingNew":    true,
		})
		resp, err := http.Post(router+"/control/routes", "application/json", bytes.NewReader(raw))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("route update for %s returned %d", rt.Model, resp.StatusCode)
		}
	}
	return nil
}

func deleteRoute(router, model string) error {
	if model == "" {
		return nil
	}
	req, err := http.NewRequest(http.MethodDelete, router+"/control/routes?model="+model, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("route delete for %s returned %d", model, resp.StatusCode)
	}
	return nil
}

func deployment(ns string, rt system.ModelRuntimeSpec) map[string]any {
	name := runtimeDeploymentName(rt)
	rid := runtimeID(rt)
	container := map[string]any{
		"name":            "runtime",
		"image":           env("MODEL_RUNTIME_IMAGE", "localhost:10690/migrant-model-runtime:go"),
		"imagePullPolicy": "IfNotPresent",
		"args":            []string{"--addr=:" + strconv.Itoa(rt.HostPort)},
		"env": []map[string]any{
			{"name": "MODEL_NAME", "value": rt.Model},
			{"name": "RUNTIME_MODE", "value": env("MODEL_RUNTIME_MODE", "synthetic")},
			{"name": "TORCHVISION_WEIGHTS", "value": env("TORCHVISION_WEIGHTS", "default")},
			{"name": "TORCH_HOME", "value": env("MODEL_RUNTIME_TORCH_HOME", "/opt/torch-cache")},
			{"name": "XDG_CACHE_HOME", "value": env("MODEL_RUNTIME_XDG_CACHE_HOME", "/opt/cache")},
			{"name": "OR_SIM_RUNTIME_ID", "value": rid},
			{"name": "BATCH_SIZE", "value": strconv.Itoa(rt.BatchSize)},
			{"name": "OR_SIM_GPU", "value": rt.GPU},
			{"name": "OR_SIM_PROFILE", "value": rt.Profile},
			{"name": "OR_SIM_SLOT_RESOURCE", "value": rt.SlotResource},
			{"name": "OR_SIM_DEVICE_RESOURCE", "value": rt.DeviceResource},
			{"name": "OR_SIM_EXPECTED_MIG_UUID", "value": rt.ExpectedMIGUUID},
		},
		"ports": []map[string]any{{"containerPort": rt.HostPort}},
		"readinessProbe": map[string]any{"httpGet": map[string]any{
			"path": "/healthz", "port": rt.HostPort,
		}, "periodSeconds": 1, "failureThreshold": 3, "timeoutSeconds": 1},
	}
	if rt.DeviceResource != "" {
		container["resources"] = map[string]any{
			"requests": map[string]any{rt.DeviceResource: 1},
			"limits":   map[string]any{rt.DeviceResource: 1},
		}
	}
	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
			"labels": map[string]any{
				"app.kubernetes.io/name": "migrant-model-runtime",
				"migrant.io/model":       rt.Model,
				"migrant.io/runtime-id":  rid,
				"migrant.io/profile":     rt.Profile,
				"migrant.io/gpu":         rt.GPU,
			},
		},
		"spec": map[string]any{
			"replicas": 1,
			"strategy": map[string]any{"type": "Recreate"},
			"selector": map[string]any{"matchLabels": map[string]any{
				"app.kubernetes.io/name": "migrant-model-runtime",
				"migrant.io/model":       rt.Model,
				"migrant.io/runtime-id":  rid,
			}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{
					"app.kubernetes.io/name": "migrant-model-runtime",
					"migrant.io/model":       rt.Model,
					"migrant.io/runtime-id":  rid,
					"migrant.io/profile":     rt.Profile,
					"migrant.io/gpu":         rt.GPU,
				}, "annotations": map[string]any{
					"migrant.io/slot-resource":      rt.SlotResource,
					"migrant.io/device-resource":    rt.DeviceResource,
					"migrant.io/expected-mig-uuid":  rt.ExpectedMIGUUID,
					"migrant.io/route-weight":       fmt.Sprintf("%.6g", routeWeight(rt)),
					"migrant.io/capacity":           fmt.Sprintf("%.6g", rt.Capacity),
					"migrant.io/binding-mechanism":  "per-mig-uuid-resource",
					"migrant.io/binding-layer":      "kubelet-device-plugin-allocate",
					"migrant.io/long-term-identity": "logical-slot",
				}},
				"spec": map[string]any{
					"nodeSelector":     map[string]any{"kubernetes.io/hostname": rt.Node},
					"hostNetwork":      true,
					"dnsPolicy":        "ClusterFirstWithHostNet",
					"runtimeClassName": "nvidia",
					"tolerations":      []map[string]any{{"operator": "Exists"}},
					"containers":       []map[string]any{container},
				},
			},
		},
	}
}

func nodeIPs(client *kube.Client) (map[string]string, error) {
	var list map[string]any
	if _, err := client.Get(kube.Nodes(), &list); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, item := range asSlice(list["items"]) {
		node := asMap(item)
		name := asString(asMap(node["metadata"])["name"])
		for _, addr := range asSlice(asMap(node["status"])["addresses"]) {
			a := asMap(addr)
			if asString(a["type"]) == "InternalIP" {
				out[name] = asString(a["address"])
			}
		}
	}
	return out, nil
}

func parseRuntimes(spec map[string]any) []system.ModelRuntimeSpec {
	items := asSlice(asMap(spec["summary"])["desiredRuntimes"])
	if len(items) == 0 {
		items = asSlice(asMap(spec["podLifecyclePreview"])["desiredRuntimes"])
	}
	if len(items) == 0 {
		items = asSlice(asMap(asMap(spec["validationTargets"])["targetAllocationPlan"])["desiredRuntimes"])
	}
	out := []system.ModelRuntimeSpec{}
	for _, item := range items {
		m := asMap(item)
		out = append(out, system.ModelRuntimeSpec{
			Model:           asString(m["model"]),
			RuntimeID:       asString(m["runtimeId"]),
			BatchSize:       intNumber(m["batchSize"]),
			Node:            asString(m["node"]),
			HostPort:        intNumber(m["hostPort"]),
			Profile:         asString(m["profile"]),
			GPU:             asString(m["gpu"]),
			SlotResource:    asString(m["slotResource"]),
			DeviceResource:  asString(m["deviceResource"]),
			ExpectedMIGUUID: asString(m["expectedMigUuid"]),
			Weight:          asFloat(m["weight"]),
			Capacity:        asFloat(m["capacity"]),
		})
	}
	return out
}

func parseActionNodes(spec map[string]any) []actionNode {
	nodes := asSlice(asMap(spec["actionDag"])["nodes"])
	out := []actionNode{}
	for idx, raw := range nodes {
		node := asMap(raw)
		action := asMap(node["action"])
		actionType := firstNonEmpty(asString(action["type"]), asString(node["type"]))
		out = append(out, actionNode{
			ID:        firstNonEmpty(asString(node["id"]), fmt.Sprintf("action-%04d", idx)),
			Type:      actionType,
			Phase:     intNumber(node["phase"]),
			Index:     intNumber(node["index"]),
			DependsOn: stringList(asSlice(node["dependsOn"])),
			Action:    action,
		})
	}
	if len(out) == 0 {
		for idx, raw := range asSlice(spec["abstractActions"]) {
			action := asMap(raw)
			out = append(out, actionNode{
				ID:        firstNonEmpty(asString(action["actionKey"]), asString(action["id"]), fmt.Sprintf("action-%04d", idx)),
				Type:      asString(action["type"]),
				Phase:     idx,
				Index:     idx,
				DependsOn: stringList(asSlice(action["dependsOn"])),
				Action:    action,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Phase != out[j].Phase {
			return out[i].Phase < out[j].Phase
		}
		if out[i].Index != out[j].Index {
			return out[i].Index < out[j].Index
		}
		return out[i].ID < out[j].ID
	})
	return out
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

func parseKubeTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func runtimeID(rt system.ModelRuntimeSpec) string {
	if rt.RuntimeID != "" {
		return rt.RuntimeID
	}
	raw := strings.ToLower(strings.Join([]string{rt.Model, rt.GPU, rt.SlotResource}, "-"))
	replacer := strings.NewReplacer("/", "-", ".", "-", "_", "-", ":", "-", " ", "-")
	raw = replacer.Replace(raw)
	raw = strings.Trim(raw, "-")
	if raw == "" {
		return "runtime"
	}
	return raw
}

func runtimeDeploymentName(rt system.ModelRuntimeSpec) string {
	name := runtimeID(rt)
	if !strings.HasSuffix(name, "-runtime") {
		name += "-runtime"
	}
	return dnsLabel(name)
}

func dnsLabel(value string) string {
	out := strings.ToLower(value)
	replacer := strings.NewReplacer("/", "-", ".", "-", "_", "-", ":", "-", " ", "-")
	out = replacer.Replace(out)
	parts := strings.Split(out, "-")
	kept := []string{}
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	out = strings.Join(kept, "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	if out == "" {
		return "runtime"
	}
	return out
}

func routeWeight(rt system.ModelRuntimeSpec) float64 {
	if rt.Weight > 0 {
		return rt.Weight
	}
	if rt.Capacity > 0 {
		return rt.Capacity
	}
	switch rt.Profile {
	case "7g":
		return 7
	case "4g":
		return 4
	case "3g":
		return 3
	case "2g":
		return 2
	default:
		return 1
	}
}

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.000001
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

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case string:
		value, err := strconv.ParseFloat(x, 64)
		if err == nil {
			return value
		}
	}
	return 0
}

func intNumber(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		value, err := strconv.Atoi(x)
		if err == nil {
			return value
		}
	default:
		return 0
	}
	return 0
}

func intString(v any) (string, bool) {
	switch x := v.(type) {
	case float64:
		return strconv.Itoa(int(x)), true
	case int:
		return strconv.Itoa(x), true
	case string:
		if x != "" {
			return x, true
		}
	}
	return "", false
}
