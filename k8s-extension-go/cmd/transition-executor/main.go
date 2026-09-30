package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
	"or-sim/k8s-extension-go/internal/system"
)

// HTTP clients for node-agent, router and runtime calls.  Timeouts are upper
// bounds for a single call; the callers keep their own overall deadlines.
var (
	// MIG geometry calls retry busy nvidia-smi operations (30 s each), so they
	// get a generous bound.
	nodeAgentMIGClient  = &http.Client{Timeout: 5 * time.Minute}
	nodeAgentFastClient = &http.Client{Timeout: 2 * time.Minute}
	routerClient        = &http.Client{Timeout: 30 * time.Second}
	runtimeClient       = &http.Client{Timeout: 2 * time.Minute}
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
		if !executionGateOpen(asString(spec["phaseGate"])) {
			continue
		}
		trace := newExecutionTrace()
		trace.Mark("executorStartedAt")
		patchExecutionStatus(client, name, "Executing", "transition execution started", trace, nil)
		if snapshot, err := startRouterMonitor(router, name, spec); err != nil {
			trace.SetMetric("routerMonitorStartError", err.Error())
		} else {
			trace.SetMetric("routerMonitor", snapshot)
		}
		runtimes := parseRuntimes(spec)
		actions := parseActionNodes(spec)
		sourceGpuCount := intNumber(asMap(spec["summary"])["sourceGpuCount"])
		targetGpuCount := firstNonZeroInt(intNumber(asMap(spec["summary"])["targetGpuCount"]), intNumber(spec["targetGpuCount"]))
		trace.SetMetric("gpuCountBaseline", map[string]any{"source": sourceGpuCount, "target": targetGpuCount})
		verification := map[string]any{}
		actionStatuses := []map[string]any{}
		if len(actions) > 0 {
			verification, actionStatuses, err = executeActionDAG(client, router, nodes, runtimes, actions, name, sourceGpuCount, trace)
			if err != nil {
				trace.Mark("executorFinishedAt")
				closeRouterMonitor(trace, router, name)
				_, patchErr := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
					"status": map[string]any{"phase": "Failed", "message": err.Error(), "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
				}, nil)
				if patchErr != nil {
					return patchErr
				}
				continue
			}
		}
		trace.Mark("finalValidationStartedAt")
		finalVerification, err := validateFinalTargetAllocation(client, router, spec)
		trace.Mark("finalValidationFinishedAt")
		trace.SetMetric("finalValidation", finalVerification)
		if err != nil {
			trace.Mark("executorFinishedAt")
			closeRouterMonitor(trace, router, name)
			_, patchErr := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
				"status": map[string]any{"phase": "Failed", "message": err.Error(), "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
			}, nil)
			if patchErr != nil {
				return patchErr
			}
			continue
		}
		if err := persistFinalLogicalBindings(client, name, spec); err != nil {
			trace.Mark("executorFinishedAt")
			closeRouterMonitor(trace, router, name)
			_, patchErr := client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
				"status": map[string]any{"phase": "Failed", "message": err.Error(), "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
			}, nil)
			if patchErr != nil {
				return patchErr
			}
			continue
		}
		trace.Mark("finalValidationPassedAt")
		trace.Mark("executorFinishedAt")
		closeRouterMonitor(trace, router, name)
		message := "action DAG executed by Go transition executor"
		if len(actions) == 0 {
			message = "empty action DAG validated by Go transition executor"
		}
		_, err = client.PatchMerge(kube.NamespacedResourceName(client.Namespace(), "migactionplans", name)+"/status", map[string]any{
			"status": map[string]any{"phase": "Executed", "message": message, "transitionExecution": trace.Status(verification), "actionStatuses": actionStatuses},
		}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func executionGateOpen(gate string) bool {
	switch gate {
	case "", "auto", "approved":
		return true
	default:
		return false
	}
}

type executionTrace struct {
	mu               sync.Mutex
	start            time.Time
	timestamps       map[string]time.Time
	metrics          map[string]any
	runtimeReadiness map[string]any
}

func newExecutionTrace() *executionTrace {
	return &executionTrace{start: time.Now(), timestamps: map[string]time.Time{}, metrics: map[string]any{}}
}

func (t *executionTrace) Mark(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timestamps[name] = time.Now()
}

func (t *executionTrace) Has(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.timestamps[name]
	return ok
}

func (t *executionTrace) SetRuntimeReadiness(value map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.runtimeReadiness == nil {
		t.runtimeReadiness = map[string]any{}
	}
	for key, item := range value {
		t.runtimeReadiness[key] = item
	}
}

func (t *executionTrace) SetMetric(name string, value any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.metrics[name]; ok {
		if items, ok := existing.([]any); ok {
			t.metrics[name] = append(items, value)
		} else {
			t.metrics[name] = []any{existing, value}
		}
		return
	}
	t.metrics[name] = value
}

func (t *executionTrace) Timestamp(name string) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.timestamps[name]
}

func (t *executionTrace) Status(verification map[string]any) map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
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

func startRouterMonitor(router, planName string, spec map[string]any) (map[string]any, error) {
	planningInput := asMap(spec["planningInput"])
	payload := map[string]any{
		"phase":           "start",
		"planName":        planName,
		"sourceArrival":   asMap(planningInput["sourceArrival"]),
		"targetArrival":   asMap(planningInput["targetArrival"]),
		"registeredSLOMs": asMap(planningInput["registeredSLOMs"]),
		"slo":             asMap(planningInput["slo"]),
	}
	return postRouterMonitor(router, payload)
}

func closeRouterMonitor(trace *executionTrace, router, planName string) {
	snapshot, err := postRouterMonitor(router, map[string]any{"phase": "finish", "planName": planName})
	if err != nil {
		trace.SetMetric("routerMonitorFinishError", err.Error())
		return
	}
	trace.SetMetric("routerSLO", snapshot)
}

func postRouterMonitor(router string, payload map[string]any) (map[string]any, error) {
	raw, _ := json.Marshal(payload)
	resp, err := routerClient.Post(strings.TrimRight(router, "/")+"/control/monitor", "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Errorf("router monitor returned %d: %v", resp.StatusCode, out)
	}
	return out, nil
}

type actionNode struct {
	ID        string
	Type      string
	Phase     int
	Index     int
	DependsOn []string
	Action    map[string]any
}

type actionRunResult struct {
	node   actionNode
	status map[string]any
	err    error
}

func executeActionDAG(client *kube.Client, router string, nodes map[string]string, runtimes []system.ModelRuntimeSpec, actions []actionNode, planName string, initialActiveGPUCount int, trace *executionTrace) (map[string]any, []map[string]any, error) {
	verified := map[string]any{}
	var verifiedMu sync.Mutex
	statuses := []map[string]any{}
	if err := validatePhysicalAcquireLifecycle(actions); err != nil {
		return verified, statuses, err
	}
	pending := map[string]actionNode{}
	known := map[string]bool{}
	completed := map[string]bool{}
	blockedOrFailed := map[string]string{}
	// The planner DAG already expresses ordering between dependent actions.  The
	// scheduler additionally protects resources whose underlying Kubernetes/MIG
	// operations are not safe to overlap.  Keep that protection at the narrowest
	// useful scope: a geometry/lifecycle change owns its whole physical GPU, while
	// instance and route work owns only its slot.  In particular, two independent
	// slots on the same GPU must not be needlessly serialized.
	runningClaims := map[string][]actionResourceClaim{}
	acquiredPhysical := map[string]string{}
	done := make(chan actionRunResult, len(actions))
	running := 0
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

	launchReady := func() bool {
		progress := false
		ready := readyActionIDs(pending)
		// A GPU-exclusive action whose dependencies are done but that is held
		// back by running actions reserves its GPU: no new action starts there
		// until it runs, so it cannot be starved by a stream of slot actions.
		reservedGPU := map[string]string{}
		for _, id := range ready {
			node := pending[id]
			if !dependenciesCompleted(node, completed) {
				continue
			}
			claims := actionResourceClaims(node)
			if len(claims) == 1 && claims[0].kind == resourceGPUExclusive && resourceClaimsConflict(claims, runningClaims) {
				if _, ok := reservedGPU[claims[0].physicalID]; !ok {
					reservedGPU[claims[0].physicalID] = id
				}
			}
		}
		for _, id := range ready {
			node := pending[id]
			blockReason := blockedDependencyReason(node, known, completed, blockedOrFailed)
			if blockReason != "" {
				statuses = append(statuses, actionStatus(node, "blocked", time.Time{}, time.Time{}, trace.start, runtimes, blockReason))
				blockedOrFailed[id] = blockReason
				delete(pending, id)
				progress = true
				continue
			}
			if !dependenciesCompleted(node, completed) {
				continue
			}
			physicalID := physicalIDFromAction(node.Action)
			claims := actionResourceClaims(node)
			if resourceClaimsConflict(claims, runningClaims) {
				continue
			}
			if claimsReservedGPU(claims, reservedGPU, id) {
				continue
			}
			if node.Type == "allocate_gpu" && physicalID != "" {
				if owner := acquiredPhysical[physicalID]; owner != "" {
					reason := fmt.Sprintf("physical GPU %s is already acquired in this plan by %s", physicalID, owner)
					statuses = append(statuses, actionStatus(node, "blocked", time.Time{}, time.Time{}, trace.start, runtimes, reason))
					blockedOrFailed[id] = reason
					delete(pending, id)
					progress = true
					continue
				}
			}
			delete(pending, id)
			runningClaims[node.ID] = claims
			running++
			progress = true
			go func(node actionNode) {
				start := time.Now()
				actionTrace := newExecutionTrace()
				err := executeAction(client, router, nodes, runtimes, node, verified, &verifiedMu, planName, actionTrace)
				finished := time.Now()
				status := "completed"
				message := ""
				if err != nil {
					status = "failed"
					message = err.Error()
				}
				statusRow := actionStatus(node, status, start, finished, trace.start, runtimes, message)
				addActionTraceStatus(statusRow, actionTrace)
				done <- actionRunResult{
					node:   node,
					status: statusRow,
					err:    err,
				}
			}(node)
		}
		return progress
	}

	for len(pending) > 0 || running > 0 {
		progress := launchReady()
		if running == 0 {
			if progress {
				continue
			}
			for _, id := range readyActionIDs(pending) {
				node := pending[id]
				reason := "blocked by unresolved dependency cycle"
				if blockReason := blockedDependencyReason(node, known, completed, blockedOrFailed); blockReason != "" {
					reason = blockReason
				}
				statuses = append(statuses, actionStatus(node, "blocked", time.Time{}, time.Time{}, trace.start, runtimes, reason))
				blockedOrFailed[id] = reason
				delete(pending, id)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("action DAG could not make progress; remaining actions were blocked")
			}
			break
		}

		result := <-done
		running--
		delete(runningClaims, result.node.ID)
		statuses = append(statuses, result.status)
		if result.err != nil {
			blockedOrFailed[result.node.ID] = result.err.Error()
			if firstErr == nil {
				firstErr = fmt.Errorf("action %s (%s) failed: %w", result.node.ID, result.node.Type, result.err)
			}
		} else {
			completed[result.node.ID] = true
			if physicalID := physicalIDFromAction(result.node.Action); physicalID != "" {
				switch result.node.Type {
				case "allocate_gpu":
					acquiredPhysical[physicalID] = result.node.ID
				case "return_gpu":
					delete(acquiredPhysical, physicalID)
				}
			}
		}
	}
	sort.Slice(statuses, func(i, j int) bool {
		aStart, bStart := asFloat(statuses[i]["relativeStartSeconds"]), asFloat(statuses[j]["relativeStartSeconds"])
		if aStart != bStart {
			return aStart < bStart
		}
		aEnd, bEnd := asFloat(statuses[i]["relativeEndSeconds"]), asFloat(statuses[j]["relativeEndSeconds"])
		if aEnd != bEnd {
			return aEnd < bEnd
		}
		return asString(statuses[i]["id"]) < asString(statuses[j]["id"])
	})
	trace.SetMetric("actionScheduler", map[string]any{
		"policy":        "event_driven_dag",
		"dependencyKey": "dependsOn",
		"phaseBarrier":  false,
	})
	trace.SetMetric("actionSummary", summarizeActionStatuses(statuses, initialActiveGPUCount))
	if firstErr != nil {
		return verified, statuses, firstErr
	}
	return verified, statuses, nil
}

func validatePhysicalAcquireLifecycle(actions []actionNode) error {
	acquired := map[string]actionNode{}
	for _, node := range actions {
		physicalID := physicalIDFromAction(node.Action)
		if physicalID == "" {
			continue
		}
		switch node.Type {
		case "allocate_gpu":
			if owner, ok := acquired[physicalID]; ok {
				return fmt.Errorf(
					"action DAG has invalid physical GPU lifecycle: physical GPU %s is acquired by %s and then acquired again by %s before return_gpu",
					physicalID,
					owner.ID,
					node.ID,
				)
			}
			acquired[physicalID] = node
		case "return_gpu":
			delete(acquired, physicalID)
		}
	}
	return nil
}

// actionResourceClaim identifies a resource that must not overlap another
// action.  An exclusive GPU claim conflicts with every claim for that GPU;
// slot claims conflict only when the logical MIG slot is the same.
type actionResourceClaim struct {
	physicalID string
	kind       string
	key        string
}

const (
	resourceGPUExclusive = "gpu-exclusive"
	resourceSlot         = "slot"
	resourceTraffic      = "traffic"
	resourceRuntime      = "runtime"
	// resourceMIGDevices covers MIG device registration (CDI refresh, UUID
	// resolution, allocatable wait).  It excludes MIG geometry changes and
	// other registrations on the GPU, but not slot-scoped instance actions.
	resourceMIGDevices = "mig-devices"
	// resourceBinding covers logical-binding ledger updates that do not touch
	// the device (allocate/bind).  It excludes geometry changes and other
	// binding updates on the GPU, but not slot-scoped instance actions.
	resourceBinding = "binding"
)

func actionResourceClaims(node actionNode) []actionResourceClaim {
	physicalID := physicalIDFromAction(node.Action)
	if physicalID == "" {
		return nil
	}
	if serializesPhysicalGPU(node.Type) {
		return []actionResourceClaim{{physicalID: physicalID, kind: resourceGPUExclusive}}
	}
	switch node.Type {
	case "register_mig_devices", "refresh_slot_resources":
		return []actionResourceClaim{{physicalID: physicalID, kind: resourceMIGDevices}}
	case "allocate_gpu", "bind_target_gpu":
		return []actionResourceClaim{{physicalID: physicalID, kind: resourceBinding}}
	}

	slot, ok := actionSlot(node.Action)
	if !ok {
		// Instance-oriented actions without an explicit slot cannot safely be
		// scoped more narrowly than the GPU.
		switch node.Type {
		case "place_instance", "activate_instance_route", "deactivate_instance_route", "wait_instance_drain", "delete_instance", "patch_batch_config", "apply_batch", "verify_batch":
			return []actionResourceClaim{{physicalID: physicalID, kind: resourceGPUExclusive}}
		default:
			return nil
		}
	}
	slotKey := fmt.Sprintf("%d:%d:%s", slot.Start, slot.End, slot.Profile)
	// All operations touching one slot share the slot claim.  The specialised
	// traffic/runtime claims make the ownership model explicit and preserve a
	// stable place to add endpoint-specific locks later; the common slot claim
	// prevents an incomplete DAG from overlapping incompatible operations on one
	// instance.
	switch node.Type {
	case "place_instance", "delete_instance":
		return []actionResourceClaim{{physicalID: physicalID, kind: resourceSlot, key: slotKey}}
	case "activate_instance_route", "deactivate_instance_route", "wait_instance_drain":
		return []actionResourceClaim{
			{physicalID: physicalID, kind: resourceSlot, key: slotKey},
			{physicalID: physicalID, kind: resourceTraffic, key: slotKey},
		}
	case "patch_batch_config", "apply_batch", "verify_batch":
		runtimeKey := modelFromAction(node.Action) + "|" + slotKey
		return []actionResourceClaim{
			{physicalID: physicalID, kind: resourceSlot, key: slotKey},
			{physicalID: physicalID, kind: resourceRuntime, key: runtimeKey},
		}
	default:
		return nil
	}
}

// claimsReservedGPU reports whether claims touch a GPU reserved for a waiting
// GPU-exclusive action other than id.
func claimsReservedGPU(claims []actionResourceClaim, reserved map[string]string, id string) bool {
	for _, claim := range claims {
		if owner, ok := reserved[claim.physicalID]; ok && owner != id {
			return true
		}
	}
	return false
}

func resourceClaimsConflict(candidate []actionResourceClaim, running map[string][]actionResourceClaim) bool {
	for _, active := range running {
		for _, next := range candidate {
			for _, held := range active {
				if next.physicalID != held.physicalID {
					continue
				}
				if next.kind == resourceGPUExclusive || held.kind == resourceGPUExclusive {
					return true
				}
				if next.kind == held.kind && next.key == held.key {
					return true
				}
			}
		}
	}
	return false
}

// serializesPhysicalGPU lists actions that change MIG geometry or the GPU's
// lifetime and therefore exclude every other action on that GPU.
// clear_gpu_binding waits for all runtime pods on the GPU to disappear, so it
// must not overlap slot actions that could start new pods there.
func serializesPhysicalGPU(actionType string) bool {
	switch actionType {
	case "return_gpu",
		"configure_full_template", "apply_slots", "configure_partial_profile", "patch_slots",
		"clear_full_template", "clear_gpu", "clear_template", "clear_gpu_binding":
		return true
	default:
		return false
	}
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

func actionStatus(node actionNode, status string, startedAt, finishedAt, origin time.Time, runtimes []system.ModelRuntimeSpec, message string) map[string]any {
	out := map[string]any{
		"id": node.ID, "type": node.Type, "phase": node.Phase,
		"status": status, "durationSeconds": 0,
		"category": actionCategory(node.Type),
	}
	if !startedAt.IsZero() {
		out["startedAt"] = startedAt.Format(time.RFC3339Nano)
		out["relativeStartSeconds"] = round(startedAt.Sub(origin).Seconds(), 6)
	}
	if !finishedAt.IsZero() {
		out["finishedAt"] = finishedAt.Format(time.RFC3339Nano)
		out["relativeEndSeconds"] = round(finishedAt.Sub(origin).Seconds(), 6)
	}
	if !startedAt.IsZero() && !finishedAt.IsZero() {
		out["durationSeconds"] = round(finishedAt.Sub(startedAt).Seconds(), 6)
	}
	if physicalID := physicalIDFromAction(node.Action); physicalID != "" {
		out["physicalGpuId"] = physicalID
	}
	if logicalID := logicalIDFromAction(node.Action); logicalID != "" {
		out["logicalGpuId"] = logicalID
	}
	if model := modelFromAction(node.Action); model != "" {
		out["model"] = model
	}
	if count := affectedInstanceCount(node, runtimes); count > 0 {
		out["affectedInstanceCount"] = count
		switch actionCategory(node.Type) {
		case "create_instance":
			out["createdInstanceCount"] = count
		case "delete_instance":
			out["deletedInstanceCount"] = count
		}
	}
	if count := migCreateSlotCount(node.Action); count > 0 {
		out["migCreateSlotCount"] = count
	}
	if count := migDeleteSlotCount(node.Action); count > 0 {
		out["migDeleteSlotCount"] = count
	}
	if len(node.DependsOn) > 0 {
		out["dependsOn"] = node.DependsOn
	}
	if message != "" {
		out["message"] = message
	}
	return out
}

func addActionTraceStatus(status map[string]any, trace *executionTrace) {
	if trace == nil {
		return
	}
	raw := trace.Status(nil)
	for _, key := range []string{"timestamps", "durationsSeconds", "runtimeReadiness"} {
		if value := nonEmptyMap(raw[key]); value != nil {
			status[key] = value
		}
	}
	if rawMetrics := nonEmptyMap(raw["metrics"]); rawMetrics != nil {
		status["traceMetrics"] = rawMetrics
	}
}

func nonEmptyMap(value any) map[string]any {
	items, ok := value.(map[string]any)
	if !ok || len(items) == 0 {
		return nil
	}
	return items
}

func summarizeActionStatuses(statuses []map[string]any, initialActiveGPUCount int) map[string]any {
	byType := map[string]int{}
	byCategory := map[string]int{}
	completed := 0
	failed := 0
	blocked := 0
	createdInstances := 0
	deletedInstances := 0
	createdMIGSlots := 0
	deletedMIGSlots := 0
	activeGPUCount := initialActiveGPUCount
	activeGPUEvents := []map[string]any{
		{"relativeSeconds": 0, "activeGpuCount": activeGPUCount, "reason": "source_allocation"},
	}
	for _, status := range statuses {
		actionType := asString(status["type"])
		category := asString(status["category"])
		byType[actionType]++
		byCategory[category]++
		switch asString(status["status"]) {
		case "completed":
			completed++
		case "failed":
			failed++
		case "blocked":
			blocked++
		}
		createdInstances += intNumber(status["createdInstanceCount"])
		deletedInstances += intNumber(status["deletedInstanceCount"])
		createdMIGSlots += intNumber(status["migCreateSlotCount"])
		deletedMIGSlots += intNumber(status["migDeleteSlotCount"])
		if asString(status["status"]) != "completed" {
			continue
		}
		delta := 0
		switch actionType {
		case "allocate_gpu":
			delta = 1
		case "return_gpu":
			delta = -1
		}
		if delta == 0 {
			continue
		}
		activeGPUCount += delta
		activeGPUEvents = append(activeGPUEvents, map[string]any{
			"relativeSeconds": asFloat(status["relativeEndSeconds"]),
			"activeGpuCount":  activeGPUCount,
			"delta":           delta,
			"actionId":        status["id"],
			"actionType":      actionType,
			"physicalGpuId":   status["physicalGpuId"],
		})
	}
	return map[string]any{
		"totalDagNodes":            len(statuses),
		"completedDagNodes":        completed,
		"failedDagNodes":           failed,
		"blockedDagNodes":          blocked,
		"byType":                   byType,
		"byCategory":               byCategory,
		"reconfigurationNodes":     byCategory["reconfiguration"],
		"createInstanceNodes":      byCategory["create_instance"],
		"deleteInstanceNodes":      byCategory["delete_instance"],
		"createdInstanceCount":     createdInstances,
		"deletedInstanceCount":     deletedInstances,
		"createdMIGSlotCount":      createdMIGSlots,
		"deletedMIGSlotCount":      deletedMIGSlots,
		"activeGpuCountOverTime":   activeGPUEvents,
		"finalActiveGPUCountEvent": activeGPUCount,
	}
}

func actionCategory(actionType string) string {
	switch actionType {
	case "configure_full_template", "apply_slots", "configure_partial_profile", "patch_slots", "clear_full_template", "clear_gpu", "clear_template", "clear_gpu_binding":
		return "reconfiguration"
	case "place_instance":
		return "create_instance"
	case "delete_instance":
		return "delete_instance"
	case "patch_batch_config", "apply_batch", "verify_batch":
		return "instance_update"
	case "allocate_gpu":
		return "gpu_acquire"
	case "return_gpu":
		return "gpu_release"
	case "deactivate_instance_route", "activate_instance_route":
		return "routing"
	case "wait_instance_drain":
		return "drain"
	default:
		return "other"
	}
}

func affectedInstanceCount(node actionNode, runtimes []system.ModelRuntimeSpec) int {
	if modelFromAction(node.Action) != "" {
		return 1
	}
	if physicalID := physicalIDFromAction(node.Action); physicalID != "" {
		return len(runtimesForGPU(runtimes, physicalID))
	}
	return 0
}

func migCreateSlotCount(action map[string]any) int {
	actionType := asString(action["type"])
	switch actionType {
	case "configure_full_template", "apply_slots", "configure_partial_profile", "patch_slots":
		return firstNonZeroInt(len(asSlice(action["createSlots"])), slotSpecCount(asString(action["createSpec"])), len(asSlice(action["slots"])))
	default:
		return 0
	}
}

func migDeleteSlotCount(action map[string]any) int {
	actionType := asString(action["type"])
	switch actionType {
	case "configure_partial_profile", "patch_slots":
		return firstNonZeroInt(len(asSlice(action["deleteSlots"])), slotSpecCount(asString(action["deleteSpec"])))
	case "clear_full_template", "clear_gpu", "clear_template":
		return firstNonZeroInt(len(asSlice(action["deleteSlots"])), slotSpecCount(asString(action["deleteSpec"])), len(asSlice(action["slots"])))
	default:
		return 0
	}
}

func slotSpecCount(spec string) int {
	count := 0
	for _, part := range strings.Split(spec, ",") {
		if strings.TrimSpace(part) != "" {
			count++
		}
	}
	return count
}

func executeAction(client *kube.Client, router string, nodes map[string]string, runtimes []system.ModelRuntimeSpec, node actionNode, verified map[string]any, verifiedMu *sync.Mutex, planName string, trace *executionTrace) error {
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
	case "deactivate_instance_route":
		return markRouteDrainingForAction(router, action)
	case "wait_instance_drain":
		trace.Mark("drainWaitStartedAt")
		err := waitInstanceDrain(router, action, 60*time.Second)
		trace.Mark("drainWaitFinishedAt")
		return err
	case "delete_instance":
		// Withdraw the route before deleting the runtime so the router never
		// points at a replica whose pod is going away.  Stage 3 has already
		// drained it (deactivate_instance_route + wait_instance_drain).
		model := modelFromAction(action)
		routedIDs := routeRuntimeIDsForAction(router, action)
		if err := deleteRouteEndpoints(router, model, routedIDs); err != nil {
			return err
		}
		runtimeIDs, err := deleteRuntimeDeploymentForAction(client, action)
		if err != nil {
			return err
		}
		if len(runtimeIDs) == 0 {
			runtimeIDs = routedIDs
		}
		if err := waitForRuntimeIDsGone(client, runtimeIDs, 120*time.Second); err != nil {
			return err
		}
		// Deployments found without a matching route (e.g. route already gone)
		// may still have a stale route entry under their runtime id.
		return deleteRouteEndpoints(router, model, missingIDs(runtimeIDs, routedIDs))
	case "clear_full_template", "clear_gpu", "clear_template":
		trace.Mark("clearStartedAt")
		err := clearGPUs(nodes, map[string]bool{nodeNameFromPhysicalGPU(physicalID) + "|" + physicalID: true})
		trace.Mark("clearFinishedAt")
		return err
	case "clear_gpu_binding":
		trace.Mark("gpuBindingClearStartedAt")
		// Do not publish a clearing ledger state until the precondition is
		// true.  A timeout here used to strand pendingLogicalGpuId and block
		// the registry even though the GPU still had runtime pods.
		err := clearGPUBinding(client, router, physicalID, action)
		if err == nil {
			err = updateLogicalBinding(client, planName, action, "clearing")
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
		registerRuntimes := runtimesForActionSlots(action, gpuRuntimes)
		resolved, err := resolveRuntimeDeviceBindings(nodes, registerRuntimes)
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
		waitMetrics, err := waitForAllocatableTargets(client, targets, 30*time.Second, 2)
		trace.Mark("allocatableWaitFinishedAt")
		for key, value := range waitMetrics {
			trace.SetMetric(key, value)
		}
		return err
	case "place_instance":
		target, err := targetRuntimeForAction(action, runtimes)
		if err != nil {
			return err
		}
		resolved, err := resolveRuntimeDeviceBindings(nodes, []system.ModelRuntimeSpec{target})
		if err != nil {
			return err
		}
		trace.Mark("cdiDeviceWaitStartedAt")
		if err := waitForRuntimeCDIDevices(nodes, resolved, 45*time.Second); err != nil {
			trace.Mark("cdiDeviceWaitFinishedAt")
			return err
		}
		trace.Mark("cdiDeviceWaitFinishedAt")
		trace.Mark("runtimeDeploymentCreateStartedAt")
		err = syncRuntimes(client, resolved)
		trace.Mark("runtimeDeploymentCreatedAt")
		return err
	case "activate_instance_route":
		if !trace.Has("runtimeDeploymentCreatedAt") {
			trace.Mark("runtimeDeploymentCreatedAt")
		}
		if physicalIDFromAction(action) == "" {
			return fmt.Errorf("activate_instance_route requires physical_gpu_id")
		}
		if _, ok := actionSlot(action); !ok {
			return fmt.Errorf("activate_instance_route requires slot")
		}
		target, err := targetRuntimeForAction(action, runtimes)
		if err != nil {
			return err
		}
		resolved, err := resolveRuntimeDeviceBindings(nodes, []system.ModelRuntimeSpec{target})
		if err != nil {
			return err
		}
		perGPUVerification, readiness, err := waitForRuntimeReadyAndCUDA(client, nodes, resolved, trace.Timestamp("runtimeDeploymentCreatedAt"), runtimeReadyTimeout())
		if err != nil {
			return err
		}
		trace.SetRuntimeReadiness(readiness)
		verifiedMu.Lock()
		for key, value := range perGPUVerification {
			verified[key] = value
		}
		verifiedMu.Unlock()
		trace.Mark("runtimeReadyAndCUDAVerifiedAt")
		if err := upsertRoutes(router, resolved, nodes); err != nil {
			return err
		}
		trace.Mark("routeSyncedAt")
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
		err = applyBatch(router, action, target)
		trace.Mark("batchApplyFinishedAt")
		return err
	case "verify_batch":
		target, err := batchTarget(action, runtimes)
		if err != nil {
			return err
		}
		trace.Mark("batchVerifyStartedAt")
		err = verifyBatch(router, action, target, 30*time.Second)
		trace.Mark("batchVerifyFinishedAt")
		if err != nil {
			return err
		}
		// Keep the router's batching policy aligned with the now-verified
		// runtime value.  Updating it only after verification avoids routing a
		// larger batch to a runtime that has not accepted the change yet.
		return syncRouteBatchSize(router, action, target.BatchSize)
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

func runtimesForActionSlots(action map[string]any, runtimes []system.ModelRuntimeSpec) []system.ModelRuntimeSpec {
	slots := []slotRequest{}
	for _, raw := range asSlice(action["slots"]) {
		item := asSlice(raw)
		if len(item) != 3 {
			continue
		}
		slots = append(slots, slotRequest{
			Start:   intNumber(item[0]),
			End:     intNumber(item[1]),
			Profile: asString(item[2]),
		})
	}
	if len(slots) == 0 {
		return runtimes
	}
	out := []system.ModelRuntimeSpec{}
	for _, rt := range runtimes {
		rtSlot, err := parseSlotRequest(rt)
		if err != nil {
			continue
		}
		for _, slot := range slots {
			slot.GPUIndex = rtSlot.GPUIndex
			if slotRequestsEquivalent(rtSlot, slot) {
				out = append(out, rt)
				break
			}
		}
	}
	return out
}

func modelFromAction(action map[string]any) string {
	return firstNonEmpty(asString(action["workload"]), asString(action["model"]), asString(action["runtime"]), asString(action["target"]))
}

func physicalIDFromAction(action map[string]any) string {
	return firstNonEmpty(asString(action["physical_gpu_id"]), asString(action["physicalGpuId"]), asString(action["gpu"]))
}

// logicalBindingLedgerMu serializes read-modify-write of the single ledger
// ConfigMap.  Binding actions on different GPUs run concurrently; without it a
// concurrent update could be lost.
var logicalBindingLedgerMu sync.Mutex

func updateLogicalBinding(client *kube.Client, planName string, action map[string]any, phase string) error {
	physicalID := physicalIDFromAction(action)
	if physicalID == "" {
		return fmt.Errorf("%s binding action requires physical_gpu_id", phase)
	}
	logicalBindingLedgerMu.Lock()
	defer logicalBindingLedgerMu.Unlock()
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
	logicalBindingLedgerMu.Lock()
	defer logicalBindingLedgerMu.Unlock()
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

func validateFinalTargetAllocation(client *kube.Client, router string, spec map[string]any) (map[string]any, error) {
	deadline := time.Now().Add(90 * time.Second)
	var last map[string]any
	var lastErr error
	for {
		out, err := validateFinalTargetAllocationOnce(client, router, spec)
		if err == nil || asBool(out["skipped"]) {
			return out, err
		}
		last, lastErr = out, err
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if last == nil {
		last = map[string]any{"ok": false}
	}
	last["timedOut"] = true
	return last, lastErr
}

func validateFinalTargetAllocationOnce(client *kube.Client, router string, spec map[string]any) (map[string]any, error) {
	targetPlan := asMap(asMap(spec["validationTargets"])["targetAllocationPlan"])
	targetState := asMap(targetPlan["targetState"])
	if len(targetState) == 0 {
		return map[string]any{"skipped": true, "reason": "missing targetState"}, nil
	}
	expectedMIG := expectedMIGSlotsFromTargetState(targetState)
	expectedRuntimes := expectedRuntimeBindingsFromTargetPlan(targetPlan)
	expectedPhysical := map[string]bool{}
	for key := range expectedMIG {
		expectedPhysical[strings.SplitN(key, "|", 2)[0]] = true
	}
	for key := range expectedRuntimes {
		expectedPhysical[strings.SplitN(key, "|", 2)[0]] = true
	}

	var registry map[string]any
	status, err := client.Get(kube.NamespacedResourceName(client.Namespace(), "physicalgpuregistries", "default"), &registry)
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}, err
	}
	if status != http.StatusOK {
		err := fmt.Errorf("final target validation could not read PhysicalGpuRegistry/default: status %d", status)
		return map[string]any{"ok": false, "status": status}, err
	}
	bindings := asMap(asMap(registry["status"])["bindings"])
	if canonicalGPUs := asMap(asMap(asMap(registry["status"])["currentAllocation"])["gpus"]); len(canonicalGPUs) > 0 {
		bindings = canonicalGPUs
	}
	actualMIG := map[string]bool{}
	actualRuntimes := map[string]bool{}
	for physicalID, raw := range bindings {
		gpu := asMap(raw)
		for _, rawDevice := range asSlice(gpu["migDevices"]) {
			device := asMap(rawDevice)
			start := intNumber(device["start"])
			end := intNumber(device["end"])
			profile := asString(device["profile"])
			if profile == "" || end <= start {
				continue
			}
			key := migSlotKey(physicalID, start, end, profile)
			actualMIG[key] = true
		}
		for _, rawRuntime := range asSlice(gpu["runtimeBindings"]) {
			rt := asMap(rawRuntime)
			model := asString(rt["model"])
			slotResource := asString(rt["slotResource"])
			if model == "" || slotResource == "" {
				continue
			}
			actualRuntimes[runtimeBindingKeyWithBatch(physicalID, slotResource, model, intNumber(rt["batchSize"]))] = true
		}
		if !expectedPhysical[physicalID] && (len(asSlice(gpu["migDevices"])) > 0 || len(asSlice(gpu["runtimeBindings"])) > 0) {
			actualMIG["unexpected-gpu|"+physicalID] = true
		}
	}

	missingMIG, extraMIG := diffStringSets(expectedMIG, actualMIG)
	missingRuntime, extraRuntime := diffStringSets(expectedRuntimes, actualRuntimes)
	liveBatchFailures := validateLiveRuntimeBatches(router, targetPlan)
	out := map[string]any{
		"ok":                     len(missingMIG) == 0 && len(extraMIG) == 0 && len(missingRuntime) == 0 && len(extraRuntime) == 0 && len(liveBatchFailures) == 0,
		"expectedMigSlotCount":   len(expectedMIG),
		"actualMigSlotCount":     len(actualMIG),
		"expectedRuntimeCount":   len(expectedRuntimes),
		"actualRuntimeCount":     len(actualRuntimes),
		"missingMigSlots":        missingMIG,
		"extraMigSlots":          extraMIG,
		"missingRuntimeBindings": missingRuntime,
		"extraRuntimeBindings":   extraRuntime,
		"liveBatchFailures":      liveBatchFailures,
	}
	if !asBool(out["ok"]) {
		return out, fmt.Errorf("final target validation failed: missingMig=%d extraMig=%d missingRuntime=%d extraRuntime=%d liveBatch=%d", len(missingMIG), len(extraMIG), len(missingRuntime), len(extraRuntime), len(liveBatchFailures))
	}
	return out, nil
}

func expectedMIGSlotsFromTargetState(targetState map[string]any) map[string]bool {
	metadata := asMap(targetState["metadata"])
	physicalByLogical := stringMap(metadata, "physical_id_map")
	displayIDs := stringMap(metadata, "display_id_map")
	out := map[string]bool{}
	for _, rawGPU := range asSlice(targetState["gpus"]) {
		gpu := asMap(rawGPU)
		logicalID := asString(gpu["gpuId"])
		if logicalID == "" {
			if id, ok := intString(gpu["gpuId"]); ok {
				logicalID = id
			}
		}
		physicalID := physicalByLogical[logicalID]
		if physicalID == "" {
			physicalID = physicalByLogical[displayIDs[logicalID]]
		}
		if physicalID == "" {
			continue
		}
		for _, rawInst := range asSlice(gpu["instances"]) {
			inst := asMap(rawInst)
			profile := asString(inst["profile"])
			if profile == "" || profile == "void" {
				continue
			}
			start := intNumber(inst["start"])
			end := start + placementProfileSize(profile, intNumber(inst["end"])-start)
			out[migSlotKey(physicalID, start, end, profile)] = true
		}
	}
	return out
}

func expectedRuntimeBindingsFromTargetPlan(targetPlan map[string]any) map[string]bool {
	out := map[string]bool{}
	for _, raw := range asSlice(targetPlan["desiredRuntimes"]) {
		rt := asMap(raw)
		model := asString(rt["model"])
		physicalID := asString(rt["gpu"])
		slotResource := asString(rt["slotResource"])
		if model == "" || physicalID == "" || slotResource == "" {
			continue
		}
		out[runtimeBindingKeyWithBatch(physicalID, slotResource, model, intNumber(rt["batchSize"]))] = true
	}
	return out
}

func migSlotKey(physicalID string, start, end int, profile string) string {
	return physicalID + "|" + strconv.Itoa(start) + "|" + strconv.Itoa(end) + "|" + profile
}

func runtimeBindingKey(physicalID, slotResource, model string) string {
	return physicalID + "|" + slotResource + "|" + model
}

func runtimeBindingKeyWithBatch(physicalID, slotResource, model string, batch int) string {
	return runtimeBindingKey(physicalID, slotResource, model) + "|batch=" + strconv.Itoa(batch)
}

// validateLiveRuntimeBatches verifies the running process directly.  Registry
// observations are useful for planning, but can lag a just-completed batch
// update and must not by themselves certify the final target state.
func validateLiveRuntimeBatches(router string, targetPlan map[string]any) []map[string]any {
	if strings.TrimSpace(router) == "" {
		return []map[string]any{{"error": "router endpoint is empty"}}
	}
	routesPayload, err := getJSON(strings.TrimRight(router, "/") + "/routes")
	if err != nil {
		return []map[string]any{{"error": "could not read router routes: " + err.Error()}}
	}
	routes := asSlice(routesPayload["routes"])
	failures := []map[string]any{}
	for _, raw := range asSlice(targetPlan["desiredRuntimes"]) {
		expected := asMap(raw)
		expectedBatch := intNumber(expected["batchSize"])
		if expectedBatch <= 0 {
			failures = append(failures, map[string]any{"runtimeId": asString(expected["runtimeId"]), "error": "target batchSize is missing"})
			continue
		}
		route := matchingRuntimeRoute(routes, expected)
		if len(route) == 0 {
			failures = append(failures, map[string]any{"runtimeId": asString(expected["runtimeId"]), "expectedBatchSize": expectedBatch, "error": "matching route is missing"})
			continue
		}
		endpoint := strings.TrimRight(asString(route["endpoint"]), "/")
		if endpoint == "" {
			failures = append(failures, map[string]any{"runtimeId": asString(expected["runtimeId"]), "expectedBatchSize": expectedBatch, "error": "route endpoint is empty"})
			continue
		}
		metrics, err := getJSON(endpoint + "/metrics")
		if err != nil {
			failures = append(failures, map[string]any{"runtimeId": asString(expected["runtimeId"]), "expectedBatchSize": expectedBatch, "endpoint": endpoint, "error": "could not read runtime metrics: " + err.Error()})
			continue
		}
		observed := intNumber(metrics["batchSize"])
		if observed != expectedBatch {
			failures = append(failures, map[string]any{"runtimeId": asString(expected["runtimeId"]), "expectedBatchSize": expectedBatch, "observedBatchSize": observed, "endpoint": endpoint})
		}
	}
	return failures
}

func matchingRuntimeRoute(routes []any, expected map[string]any) map[string]any {
	expectedID := asString(expected["runtimeId"])
	for _, raw := range routes {
		route := asMap(raw)
		if expectedID != "" && asString(route["runtimeId"]) == expectedID {
			return route
		}
	}
	for _, raw := range routes {
		route := asMap(raw)
		if asString(route["model"]) == asString(expected["model"]) &&
			asString(route["gpu"]) == asString(expected["gpu"]) &&
			asString(route["slotResource"]) == asString(expected["slotResource"]) {
			return route
		}
	}
	return nil
}

func placementProfileSize(profile string, fallback int) int {
	switch profile {
	case "7g":
		return 8
	case "4g", "3g":
		return 4
	case "2g":
		return 2
	case "1g":
		return 1
	default:
		return fallback
	}
}

func diffStringSets(expected, actual map[string]bool) ([]string, []string) {
	missing := []string{}
	extra := []string{}
	for key := range expected {
		if !actual[key] {
			missing = append(missing, key)
		}
	}
	for key := range actual {
		if !expected[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func stringMap(parent map[string]any, key string) map[string]string {
	out := map[string]string{}
	for logicalID, rawPhysicalID := range asMap(parent[key]) {
		physicalID := asString(rawPhysicalID)
		if physicalID == "" {
			physicalID, _ = intString(rawPhysicalID)
		}
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

func markRouteDrainingForAction(router string, action map[string]any) error {
	route, err := routeEndpointForAction(router, action)
	if err != nil {
		if errors.Is(err, errRouteNotFound) {
			return nil
		}
		return err
	}
	return patchRouteEndpoint(router, asString(route["model"]), asString(route["runtimeId"]), map[string]any{
		"acceptingNew": false,
		"draining":     true,
		"active":       true,
	})
}

func waitInstanceDrain(router string, action map[string]any, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	record, err := routeEndpointForAction(router, action)
	if err != nil {
		if errors.Is(err, errRouteNotFound) {
			return nil
		}
		return err
	}
	runtimeID := asString(record["runtimeId"])
	for time.Now().Before(deadline) {
		// Poll only this replica's row so each check queries one runtime's
		// metrics instead of every runtime behind the router.
		rows, err := routeSnapshots(router, runtimeID)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		route := rows[0]
		inflight := intNumber(route["endpointInflight"])
		if _, ok := route["endpointInflight"]; !ok {
			inflight = intNumber(route["inflight"])
		}
		queued := intNumber(route["endpointQueued"])
		if _, ok := route["endpointQueued"]; !ok {
			queued = intNumber(route["queued"])
		}
		if inflight == 0 && queued == 0 {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for drain of %s", routeActionLabel(action))
}

func routeEndpointForAction(router string, action map[string]any) (map[string]any, error) {
	model := modelFromAction(action)
	if model == "" {
		return nil, fmt.Errorf("%s requires workload/model", asString(action["type"]))
	}
	if physicalIDFromAction(action) == "" {
		return nil, fmt.Errorf("%s requires physical_gpu_id", asString(action["type"]))
	}
	if _, ok := actionSlot(action); !ok {
		return nil, fmt.Errorf("%s requires slot", asString(action["type"]))
	}
	routes, err := routeRecords(router)
	if err != nil {
		return nil, err
	}
	matches := []map[string]any{}
	for _, route := range routes {
		if routeMatchesAction(route, action) {
			matches = append(matches, route)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w for %s", errRouteNotFound, routeActionLabel(action))
	}
	return nil, fmt.Errorf("route ambiguous for %s: %d matches", routeActionLabel(action), len(matches))
}

var errRouteNotFound = errors.New("route not found")

// routeRecords returns the router's stored route records without querying
// runtime metrics; it carries the identity fields matching needs.
func routeRecords(router string) ([]map[string]any, error) {
	payload, err := getJSON(strings.TrimRight(router, "/") + "/control/routes")
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, raw := range asSlice(payload["routes"]) {
		out = append(out, asMap(raw))
	}
	return out, nil
}

// patchRouteEndpoint changes only the given fields of one replica's route,
// atomically in the router, so the rest of the stored route is preserved.
func patchRouteEndpoint(router, model, runtimeID string, fields map[string]any) error {
	if model == "" || runtimeID == "" {
		return fmt.Errorf("route patch requires model and runtimeId (model=%q runtimeId=%q)", model, runtimeID)
	}
	body := map[string]any{"model": model, "runtimeId": runtimeID}
	for key, value := range fields {
		body[key] = value
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPatch, strings.TrimRight(router, "/")+"/control/routes", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := routerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("route patch for %s/%s returned %d", model, runtimeID, resp.StatusCode)
	}
	return nil
}

func missingIDs(ids, exclude []string) []string {
	skip := map[string]bool{}
	for _, id := range exclude {
		skip[id] = true
	}
	out := []string{}
	for _, id := range ids {
		if id != "" && !skip[id] {
			out = append(out, id)
		}
	}
	return out
}

func routeRuntimeIDsForAction(router string, action map[string]any) []string {
	route, err := routeEndpointForAction(router, action)
	if err != nil {
		return nil
	}
	if runtimeID := asString(route["runtimeId"]); runtimeID != "" {
		return []string{runtimeID}
	}
	return nil
}

func routeMatchesAction(route, action map[string]any) bool {
	model := modelFromAction(action)
	if model != "" && asString(route["model"]) != model {
		return false
	}
	physicalID := physicalIDFromAction(action)
	if physicalID != "" && asString(route["gpu"]) != physicalID {
		return false
	}
	slot, ok := actionSlot(action)
	if !ok {
		return false
	}
	return slotResourceMatches(physicalID, asString(route["slotResource"]), slot)
}

func routeActionLabel(action map[string]any) string {
	return fmt.Sprintf("model=%q gpu=%q slot=%v", modelFromAction(action), physicalIDFromAction(action), action["slot"])
}

func clearGPUBinding(client *kube.Client, router, physicalID string, action map[string]any) error {
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
	target, err := targetRuntimeForAction(action, runtimes)
	if err != nil {
		return system.ModelRuntimeSpec{}, err
	}
	batch := intNumber(firstNonNil(action["batchSize"], action["targetBatchSize"], action["newBatchSize"]))
	if batch > 0 {
		target.BatchSize = batch
	}
	if target.BatchSize <= 0 {
		return target, fmt.Errorf("batch action for %s has no positive target batchSize", target.Model)
	}
	return target, nil
}

func targetRuntimeForAction(action map[string]any, runtimes []system.ModelRuntimeSpec) (system.ModelRuntimeSpec, error) {
	model := modelFromAction(action)
	physicalID := physicalIDFromAction(action)
	slot, hasSlot := actionSlot(action)
	matches := []system.ModelRuntimeSpec{}
	for _, rt := range runtimes {
		if model != "" && rt.Model != model {
			continue
		}
		if physicalID != "" && rt.GPU != physicalID {
			continue
		}
		if hasSlot {
			rtSlot, err := parseSlotRequest(rt)
			if err != nil {
				continue
			}
			if !slotRequestsEquivalent(rtSlot, slot) {
				continue
			}
		}
		matches = append(matches, rt)
	}
	if len(matches) == 1 {
		return applyActionRuntimeOverrides(matches[0], action), nil
	}
	if len(matches) == 0 {
		return system.ModelRuntimeSpec{}, fmt.Errorf("target runtime not found for action %s model=%q gpu=%q slot=%v", asString(action["type"]), model, physicalID, action["slot"])
	}
	return system.ModelRuntimeSpec{}, fmt.Errorf("target runtime ambiguous for action %s model=%q gpu=%q slot=%v: %d matches", asString(action["type"]), model, physicalID, action["slot"], len(matches))
}

func applyActionRuntimeOverrides(rt system.ModelRuntimeSpec, action map[string]any) system.ModelRuntimeSpec {
	if batch := intNumber(firstNonNil(action["batch"], action["batchSize"], action["targetBatchSize"], action["newBatchSize"])); batch > 0 {
		rt.BatchSize = batch
	}
	return rt
}

func actionSlot(action map[string]any) (slotRequest, bool) {
	values := asSlice(action["slot"])
	if len(values) < 3 {
		return slotRequest{}, false
	}
	start := intNumber(values[0])
	end := intNumber(values[1])
	profile := asString(values[2])
	if end <= start || profile == "" {
		return slotRequest{}, false
	}
	gpuIndex := 0
	if physicalID := physicalIDFromAction(action); physicalID != "" {
		if parsed, err := gpuIndexFromID(physicalID); err == nil {
			gpuIndex = parsed
		}
	}
	return slotRequest{GPUIndex: gpuIndex, Start: start, End: end, Profile: profile}, true
}

func slotRequestsEquivalent(a, b slotRequest) bool {
	if a.GPUIndex != b.GPUIndex || a.Start != b.Start || a.Profile != b.Profile {
		return false
	}
	if a.End == b.End {
		return true
	}
	// A100 3g placements occupy four physical columns in the exact-slot
	// resource name (for example s4-8-3g) while planner actions describe the
	// logical 3g interval (for example [4,7,"3g"]).
	if a.Profile == "3g" && absInt(a.End-b.End) == 1 {
		return true
	}
	return false
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func applyBatch(router string, action map[string]any, target system.ModelRuntimeSpec) error {
	endpoint, err := routeEndpointForBatchAction(router, action)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"batchSize": target.BatchSize})
	resp, err := runtimeClient.Post(endpoint+"/control/batch", "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("batch apply for %s returned %d", target.Model, resp.StatusCode)
	}
	return nil
}

func verifyBatch(router string, action map[string]any, target system.ModelRuntimeSpec, timeout time.Duration) error {
	endpoint, err := routeEndpointForBatchAction(router, action)
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

func syncRouteBatchSize(router string, action map[string]any, batch int) error {
	if batch <= 0 {
		return fmt.Errorf("cannot sync non-positive batchSize %d", batch)
	}
	route, err := routeEndpointForAction(router, action)
	if err != nil {
		return err
	}
	return patchRouteEndpoint(router, asString(route["model"]), asString(route["runtimeId"]), map[string]any{"batchSize": batch})
}

func routeEndpointForBatchAction(router string, action map[string]any) (string, error) {
	route, err := routeEndpointForAction(router, action)
	if err != nil {
		return "", err
	}
	endpoint := strings.TrimRight(asString(route["endpoint"]), "/")
	if endpoint == "" {
		return "", fmt.Errorf("route for %s has empty endpoint", routeActionLabel(action))
	}
	return endpoint, nil
}

// routeSnapshots returns /routes rows (with runtime metrics).  A non-empty
// runtimeID limits the rows and the metric queries to that replica.
func routeSnapshots(router, runtimeID string) ([]map[string]any, error) {
	endpoint := strings.TrimRight(router, "/") + "/routes"
	if runtimeID != "" {
		endpoint += "?runtimeId=" + url.QueryEscape(runtimeID)
	}
	payload, err := getJSON(endpoint)
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

func deleteRuntimeDeploymentForAction(client *kube.Client, action map[string]any) ([]string, error) {
	var list map[string]any
	if _, err := client.Get(kube.Deployments(client.Namespace()), &list); err != nil {
		return nil, err
	}
	model := modelFromAction(action)
	physicalID := physicalIDFromAction(action)
	slot, hasSlot := actionSlot(action)
	deleted := []string{}
	for _, item := range asSlice(list["items"]) {
		dep := asMap(item)
		meta := asMap(dep["metadata"])
		labels := asMap(meta["labels"])
		if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" {
			continue
		}
		if model != "" && asString(labels["migrant.io/model"]) != model {
			continue
		}
		if physicalID != "" && asString(labels["migrant.io/gpu"]) != physicalID {
			continue
		}
		if hasSlot && !deploymentMatchesSlot(dep, physicalID, slot) {
			continue
		}
		name := asString(meta["name"])
		if status, err := client.Delete(kube.Deployment(client.Namespace(), name)); err != nil && status != http.StatusNotFound {
			return nil, err
		}
		if runtimeID := asString(labels["migrant.io/runtime-id"]); runtimeID != "" {
			deleted = append(deleted, runtimeID)
		}
	}
	return deleted, nil
}

func deploymentMatchesSlot(dep map[string]any, physicalID string, slot slotRequest) bool {
	spec := asMap(dep["spec"])
	template := asMap(spec["template"])
	meta := asMap(template["metadata"])
	annotations := asMap(meta["annotations"])
	slotResource := asString(annotations["migrant.io/slot-resource"])
	if slotResource == "" {
		return false
	}
	if physicalID == "" {
		labels := asMap(asMap(dep["metadata"])["labels"])
		physicalID = asString(labels["migrant.io/gpu"])
	}
	return slotResourceMatches(physicalID, slotResource, slot)
}

func slotResourceMatches(physicalID, slotResource string, expected slotRequest) bool {
	observed, err := parseSlotRequest(system.ModelRuntimeSpec{
		GPU:          physicalID,
		SlotResource: slotResource,
	})
	return err == nil && slotRequestsEquivalent(observed, expected)
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
	resp, err := nodeAgentMIGClient.Post(fmt.Sprintf("http://%s:10684/apply-slots?gpuIndex=%d", ip, gpuIndex), "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := decodeNodeAgentResponse(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("apply slots on %s gpu%d returned %d%s", node, gpuIndex, resp.StatusCode, nodeAgentErrorDetail(body))
	}
	return body, nil
}

func patchSlots(nodes map[string]string, physicalID, deleteSpec, createSpec, preserveSpec string) (map[string]any, error) {
	node, gpuIndex, ip, err := resolvePhysicalGPU(nodes, physicalID)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(map[string]any{"delete": deleteSpec, "create": createSpec, "preserve": preserveSpec})
	resp, err := nodeAgentMIGClient.Post(fmt.Sprintf("http://%s:10684/patch-slots?gpuIndex=%d", ip, gpuIndex), "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := decodeNodeAgentResponse(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("patch slots on %s gpu%d returned %d%s", node, gpuIndex, resp.StatusCode, nodeAgentErrorDetail(body))
	}
	return body, nil
}

func refreshCDI(nodes map[string]string, physicalID string) (map[string]any, error) {
	node, gpuIndex, ip, err := resolvePhysicalGPU(nodes, physicalID)
	if err != nil {
		return nil, err
	}
	resp, err := nodeAgentFastClient.Post(fmt.Sprintf("http://%s:10684/refresh-cdi?gpuIndex=%d", ip, gpuIndex), "application/json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := decodeNodeAgentResponse(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("refresh CDI on %s gpu%d returned %d%s", node, gpuIndex, resp.StatusCode, nodeAgentErrorDetail(body))
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

func nodeAgentErrorDetail(body map[string]any) string {
	if body == nil {
		return ""
	}
	if message := asString(body["message"]); message != "" {
		return ": " + message
	}
	if err := asString(body["error"]); err != "" {
		return ": " + err
	}
	if err := asString(body["decodeError"]); err != "" {
		return ": " + err
	}
	return ""
}

func nodeAgentTransactionSummary(body map[string]any) map[string]any {
	if body == nil {
		return nil
	}
	out := map[string]any{
		"success":           body["success"],
		"command":           body["command"],
		"gpuIndex":          body["gpuIndex"],
		"message":           body["message"],
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
		RuntimeModel:    asString(labels["migrant.io/runtime-model"]),
		RequestClass:    asString(labels["migrant.io/request-class"]),
		PromptLen:       intNumber(labels["migrant.io/prompt-len"]),
		OutputTokens:    intNumber(labels["migrant.io/output-tokens"]),
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
			case "OR_SIM_WORKLOAD":
				if value := asString(e["value"]); value != "" {
					rt.Model = value
				}
			case "MODEL_NAME":
				if value := asString(e["value"]); value != "" {
					rt.RuntimeModel = value
				}
			case "OR_SIM_REQUEST_CLASS":
				if value := asString(e["value"]); value != "" {
					rt.RequestClass = value
				}
			case "PROMPT_LEN":
				if rt.PromptLen == 0 {
					rt.PromptLen = intNumber(e["value"])
				}
			case "OUTPUT_TOKENS":
				if rt.OutputTokens == 0 {
					rt.OutputTokens = intNumber(e["value"])
				}
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
	return runtimeID(a) == runtimeID(b) && a.Model == b.Model && runtimeModel(a) == runtimeModel(b) &&
		a.RequestClass == b.RequestClass && a.PromptLen == b.PromptLen && a.OutputTokens == b.OutputTokens &&
		a.BatchSize == b.BatchSize && a.Node == b.Node &&
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

func waitForRuntimeIDsGone(client *kube.Client, runtimeIDs []string, timeout time.Duration) error {
	if len(runtimeIDs) == 0 {
		return nil
	}
	wanted := map[string]bool{}
	for _, runtimeID := range runtimeIDs {
		if runtimeID != "" {
			wanted[runtimeID] = true
		}
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
			if asString(labels["app.kubernetes.io/name"]) == "migrant-model-runtime" && wanted[asString(labels["migrant.io/runtime-id"])] {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for runtime pods %v to terminate", runtimeIDs)
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
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s:10684/clear?gpuIndex=%d", ip, gpuIndex), nil)
		if err != nil {
			return err
		}
		resp, err := (&http.Client{Timeout: 45 * time.Second}).Do(req)
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
	nodeCPU := map[string]nodeCPUConfig{}
	for _, rt := range runtimes {
		if rt.DeviceResource == "" || rt.ExpectedMIGUUID == "" {
			return fmt.Errorf("runtime %s missing resolved per-MIG UUID device binding for slot %s", rt.Model, rt.SlotResource)
		}
		cfg, ok := nodeCPU[rt.Node]
		if !ok {
			var err error
			if cfg, err = runtimeNodeCPUConfig(client, rt.Node); err != nil {
				return err
			}
			nodeCPU[rt.Node] = cfg
		}
		placement, err := runtimeCPUPlacementFor(cfg, rt)
		if err != nil {
			return err
		}
		body := deployment(client.Namespace(), rt, placement)
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
		observed := slotRequest{
			GPUIndex: slot.GPUIndex,
			Start:    intNumber(item["slotStart"]),
			End:      intNumber(item["slotEnd"]),
			Profile:  asString(item["profile"]),
		}
		if !slotRequestsEquivalent(observed, slot) {
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
		resp, err := nodeAgentMIGClient.Post(url, "application/json", bytes.NewReader(raw))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("apply slots on %s gpu%d returned %d", node, gpuIndex, resp.StatusCode)
		}
		refreshURL := fmt.Sprintf("http://%s:10684/refresh-cdi?gpuIndex=%d", ip, gpuIndex)
		refreshResp, err := nodeAgentFastClient.Post(refreshURL, "application/json", nil)
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

func waitForRuntimeCDIDevices(nodes map[string]string, runtimes []system.ModelRuntimeSpec, timeout time.Duration) error {
	if len(runtimes) == 0 {
		return nil
	}
	// Refresh CDI only for GPUs with an expected device missing from the
	// node's CDI spec.  register_mig_devices normally refreshed it already,
	// and a CDI refresh takes the node-wide lock, so refreshing
	// unconditionally serializes every place_instance on the host.
	refreshed := map[string]bool{}
	for _, rt := range runtimes {
		if rt.ExpectedMIGUUID == "" {
			return fmt.Errorf("runtime %s missing expected MIG UUID for CDI readiness", rt.Model)
		}
		key := rt.Node + "|" + rt.GPU
		if refreshed[key] {
			continue
		}
		if ready, err := runtimeCDIDeviceReady(nodes, rt); err == nil && ready {
			continue
		}
		if _, err := refreshCDI(nodes, rt.GPU); err != nil {
			return err
		}
		refreshed[key] = true
	}
	deadline := time.Now().Add(timeout)
	lastMissing := []string{}
	for time.Now().Before(deadline) {
		missing := []string{}
		for _, rt := range runtimes {
			ready, err := runtimeCDIDeviceReady(nodes, rt)
			if err != nil || !ready {
				detail := rt.Node + "/" + rt.ExpectedMIGUUID
				if err != nil {
					detail += ": " + err.Error()
				}
				missing = append(missing, detail)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		lastMissing = missing
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for CDI devices: %s", strings.Join(lastMissing, ", "))
}

func runtimeCDIDeviceReady(nodes map[string]string, rt system.ModelRuntimeSpec) (bool, error) {
	ip := nodes[rt.Node]
	if ip == "" {
		return false, fmt.Errorf("node IP not found for %s", rt.Node)
	}
	u := fmt.Sprintf("http://%s:10684/cdi-ready?migUuid=%s", ip, url.QueryEscape(rt.ExpectedMIGUUID))
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

func nodeAgentRegisteredTargets(body map[string]any, targets []allocatableTarget) (bool, []string) {
	if len(targets) == 0 {
		return true, nil
	}
	registered := map[string]bool{}
	for _, raw := range asSlice(asMap(body["devicePluginRefresh"])["registeredResources"]) {
		registered[asString(raw)] = true
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
	if loadTimings := asMap(health["loadTimings"]); len(loadTimings) > 0 {
		readiness["loadTimings"] = loadTimings
	}
	readiness["healthModelId"] = asString(health["modelId"])
	readiness["healthRuntimeMode"] = asString(health["runtimeMode"])
	readiness["healthDevice"] = asString(health["device"])
	readiness["healthLoaded"] = asBool(health["loaded"])
	migUUID := asString(firstNonNil(health["orSimMIGUUID"], health["migUuid"], health["migUUID"]))
	if migUUID == "" {
		return nil, readiness, fmt.Errorf("runtime %s health did not report OR_SIM_MIG_UUID", rt.Model)
	}
	if rt.ExpectedMIGUUID != "" && migUUID != rt.ExpectedMIGUUID {
		return nil, readiness, fmt.Errorf("runtime %s got MIG UUID %s, expected %s", rt.Model, migUUID, rt.ExpectedMIGUUID)
	}
	parentGPUUUID, _ := parentGPUUUIDForRuntime(nodes[rt.Node], rt)
	if runtimeContainerPredates(pod, deploymentCreatedAt) {
		// The container was already running before this action (a batch
		// change on a replica whose CUDA placement was verified when it was
		// placed).  Health and the reported MIG UUID are checked above; the
		// nvidia-smi process query (1.5-2.7 s per call under load) would only
		// re-confirm the same process.
		readiness["cudaVerification"] = "skipped: runtime container predates this action"
		return map[string]any{
			"node":                    rt.Node,
			"gpu":                     rt.GPU,
			"slotResource":            rt.SlotResource,
			"deviceResource":          rt.DeviceResource,
			"expectedMigUUID":         rt.ExpectedMIGUUID,
			"migUUID":                 migUUID,
			"parentGPUUUID":           parentGPUUUID,
			"cudaVerificationSkipped": true,
			"health":                  health,
		}, readiness, nil
	}
	processes, foundAt, err := waitForCUDAProcess(nodes[rt.Node], migUUID, parentGPUUUID, deadline)
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
		"parentGPUUUID":   parentGPUUUID,
		"processes":       processes,
		"health":          health,
	}, readiness, nil
}

// runtimeContainerStartGrace allows for the whole-second resolution of the
// container start time when deciding that a container predates an action.
const runtimeContainerStartGrace = 5 * time.Second

// runtimeContainerPredates reports whether the runtime container started
// before `since` (by more than the grace) and has not restarted.
func runtimeContainerPredates(pod map[string]any, since time.Time) bool {
	started, err := time.Parse(time.RFC3339, runtimeContainerStartedAt(pod))
	if err != nil || since.IsZero() {
		return false
	}
	for _, raw := range asSlice(asMap(pod["status"])["containerStatuses"]) {
		container := asMap(raw)
		if asString(container["name"]) == "runtime" && fmt.Sprint(container["restartCount"]) != "0" {
			return false
		}
	}
	return started.Before(since.Add(-runtimeContainerStartGrace))
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

func waitForCUDAProcess(nodeIP, migUUID, parentGPUUUID string, deadline time.Time) ([]any, time.Time, error) {
	for time.Now().Before(deadline) {
		payload, err := getJSON("http://" + nodeIP + ":10684/processes?migUuid=" + migUUID)
		if err == nil {
			processes := asSlice(payload["processes"])
			if len(processes) > 0 {
				return processes, time.Now(), nil
			}
		}
		if parentGPUUUID != "" {
			payload, err = getJSON("http://" + nodeIP + ":10684/processes")
			if err == nil {
				processes := processesForUUID(payload, parentGPUUUID)
				if len(processes) > 0 {
					return processes, time.Now(), nil
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if parentGPUUUID != "" {
		return nil, time.Time{}, fmt.Errorf("no active compute process found for %s or parent GPU %s before timeout", migUUID, parentGPUUUID)
	}
	return nil, time.Time{}, fmt.Errorf("no active compute process found for %s before timeout", migUUID)
}

func getJSON(url string) (map[string]any, error) {
	client := http.Client{Timeout: 15 * time.Second}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
		} else {
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				lastErr = fmt.Errorf("GET %s returned %d", url, resp.StatusCode)
			} else {
				var payload map[string]any
				if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
					return nil, err
				}
				return payload, nil
			}
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	return nil, lastErr
}

func parentGPUUUIDForRuntime(nodeIP string, rt system.ModelRuntimeSpec) (string, error) {
	slot, err := parseSlotRequest(rt)
	if err != nil {
		return "", err
	}
	payload, err := getJSON(fmt.Sprintf("http://%s:10684/list?gpuIndex=%d", nodeIP, slot.GPUIndex))
	if err != nil {
		return "", err
	}
	return parseGPUUUIDFromNvidiaSMIL(asString(payload["nvidiaSmiL"]), slot.GPUIndex), nil
}

func parseGPUUUIDFromNvidiaSMIL(out string, gpuIndex int) string {
	prefix := fmt.Sprintf("GPU %d:", gpuIndex)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		start := strings.Index(line, "(UUID: ")
		end := strings.LastIndex(line, ")")
		if start < 0 || end <= start {
			return ""
		}
		return strings.TrimSpace(line[start+len("(UUID: ") : end])
	}
	return ""
}

func processesForUUID(payload map[string]any, uuid string) []any {
	byUUID := asMap(payload["processesByUUID"])
	if len(byUUID) == 0 {
		return nil
	}
	return asSlice(byUUID[uuid])
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

func upsertRoutes(router string, runtimes []system.ModelRuntimeSpec, nodes map[string]string) error {
	for _, rt := range runtimes {
		ip := nodes[rt.Node]
		if ip == "" {
			return fmt.Errorf("node IP not found for %s", rt.Node)
		}
		raw, _ := json.Marshal(map[string]any{
			"model":           rt.Model,
			"runtimeModel":    runtimeModel(rt),
			"requestClass":    rt.RequestClass,
			"promptLen":       rt.PromptLen,
			"outputTokens":    rt.OutputTokens,
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
		if err := postRouteRaw(router, raw, rt.Model); err != nil {
			return err
		}
	}
	return nil
}

func postRouteEndpoint(router string, route map[string]any) error {
	raw, _ := json.Marshal(route)
	return postRouteRaw(router, raw, asString(route["model"]))
}

func postRouteRaw(router string, raw []byte, label string) error {
	resp, err := routerClient.Post(strings.TrimRight(router, "/")+"/control/routes", "application/json", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("route update for %s returned %d", label, resp.StatusCode)
	}
	return nil
}

func deleteRouteEndpoints(router, model string, runtimeIDs []string) error {
	if model == "" {
		return nil
	}
	for _, runtimeID := range runtimeIDs {
		if runtimeID == "" {
			continue
		}
		if err := deleteRouteEndpoint(router, model, runtimeID); err != nil {
			return err
		}
	}
	return nil
}

func deleteRouteEndpoint(router, model, runtimeID string) error {
	query := url.Values{}
	query.Set("model", model)
	query.Set("runtimeId", runtimeID)
	req, err := http.NewRequest(http.MethodDelete, strings.TrimRight(router, "/")+"/control/routes?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := routerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("route endpoint delete for %s/%s returned %d", model, runtimeID, resp.StatusCode)
	}
	return nil
}

const (
	// runtimeCPUExcludeAnnotation lists host CPUs (e.g. "1,33") runtimes on
	// that node must not run on; the runtime drops them from its affinity.
	runtimeCPUExcludeAnnotation = "mig.or-sim.io/runtime-cpu-exclude"
	// runtimeCPUPoolAnnotation lists dedicated per-runtime CPU sets separated
	// by ";" (e.g. "2,58;4,60", one physical core each).  A runtime on GPU g
	// with slot start s gets entry g*8+s; slots never overlap on a node, so
	// runtimes never share a core.  Spreading one runtime over many cores
	// makes GPU-launch-bound latency bimodal.
	runtimeCPUPoolAnnotation = "mig.or-sim.io/runtime-cpu-pool"
	// runtimeFrontendCPUAnnotation lists the host CPUs (e.g. "34,90,36,92")
	// the runtimes' HTTP processes share; inference runs in a separate process
	// on the runtime's pool core, so HTTP parsing never competes with kernel
	// launches for that core or the interpreter lock.
	runtimeFrontendCPUAnnotation = "mig.or-sim.io/runtime-frontend-cpus"
	migSlotsPerGPU               = 8
)

type nodeCPUConfig struct {
	Exclude  string
	Pool     []string
	Frontend string
}

type runtimeCPUPlacement struct {
	Exclude  string
	Set      string
	Frontend string
}

func runtimeNodeCPUConfig(client *kube.Client, nodeName string) (nodeCPUConfig, error) {
	var node map[string]any
	if _, err := client.Get(kube.Node(nodeName), &node); err != nil {
		return nodeCPUConfig{}, err
	}
	annotations := asMap(asMap(node["metadata"])["annotations"])
	cfg := nodeCPUConfig{
		Exclude:  strings.TrimSpace(asString(annotations[runtimeCPUExcludeAnnotation])),
		Frontend: strings.TrimSpace(asString(annotations[runtimeFrontendCPUAnnotation])),
	}
	for _, entry := range strings.Split(asString(annotations[runtimeCPUPoolAnnotation]), ";") {
		if entry = strings.TrimSpace(entry); entry != "" {
			cfg.Pool = append(cfg.Pool, entry)
		}
	}
	return cfg, nil
}

func runtimeCPUPlacementFor(cfg nodeCPUConfig, rt system.ModelRuntimeSpec) (runtimeCPUPlacement, error) {
	placement := runtimeCPUPlacement{Exclude: cfg.Exclude, Frontend: cfg.Frontend}
	if len(cfg.Pool) == 0 {
		return placement, nil
	}
	slot, err := parseSlotRequest(rt)
	if err != nil {
		return placement, err
	}
	idx := slot.GPUIndex*migSlotsPerGPU + slot.Start
	if idx < 0 || idx >= len(cfg.Pool) {
		return placement, fmt.Errorf("%s on %s has %d entries, runtime %s needs entry %d (gpu%d slot %d)",
			runtimeCPUPoolAnnotation, rt.Node, len(cfg.Pool), rt.Model, idx, slot.GPUIndex, slot.Start)
	}
	placement.Set = cfg.Pool[idx]
	return placement, nil
}

func deployment(ns string, rt system.ModelRuntimeSpec, cpu runtimeCPUPlacement) map[string]any {
	name := runtimeDeploymentName(rt)
	rid := runtimeID(rt)
	modelName := runtimeModel(rt)
	modelLabel := labelValue(rt.Model)
	runtimeModelLabel := labelValue(modelName)
	requestClassLabel := labelValue(rt.RequestClass)
	envVars := []map[string]any{
		{"name": "MODEL_NAME", "value": modelName},
		{"name": "OR_SIM_WORKLOAD", "value": rt.Model},
		{"name": "OR_SIM_REQUEST_CLASS", "value": rt.RequestClass},
		{"name": "PROMPT_LEN", "value": strconv.Itoa(rt.PromptLen)},
		{"name": "OUTPUT_TOKENS", "value": strconv.Itoa(rt.OutputTokens)},
		{"name": "RUNTIME_MODE", "value": runtimeMode(rt)},
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
	}
	if cpu.Set != "" {
		envVars = append(envVars, map[string]any{"name": "OR_SIM_CPU_SET", "value": cpu.Set})
	}
	if cpu.Exclude != "" {
		envVars = append(envVars, map[string]any{"name": "OR_SIM_CPU_EXCLUDE", "value": cpu.Exclude})
	}
	if cpu.Frontend != "" {
		envVars = append(envVars, map[string]any{"name": "OR_SIM_FRONTEND_CPU_SET", "value": cpu.Frontend})
	}
	if isLLMRuntime(rt) {
		if modelID := llmModelID(rt); modelID != "" {
			envVars = append(envVars, map[string]any{"name": "MODEL_ID", "value": modelID})
		}
		envVars = append(envVars,
			map[string]any{"name": "HF_HOME", "value": env("MODEL_RUNTIME_HF_HOME", "/opt/hf-cache")},
			map[string]any{"name": "HUGGINGFACE_HUB_CACHE", "value": env("MODEL_RUNTIME_HF_HUB_CACHE", "/opt/hf-cache/hub")},
			map[string]any{"name": "TRANSFORMERS_CACHE", "value": env("MODEL_RUNTIME_TRANSFORMERS_CACHE", "/opt/hf-cache/hub")},
			map[string]any{"name": "HF_TOKEN", "valueFrom": map[string]any{"secretKeyRef": map[string]any{
				"name":     env("HF_TOKEN_SECRET_NAME", "hf-token"),
				"key":      env("HF_TOKEN_SECRET_KEY", "HF_TOKEN"),
				"optional": true,
			}}},
		)
	}
	container := map[string]any{
		"name":            "runtime",
		"image":           runtimeImage(rt),
		"imagePullPolicy": "IfNotPresent",
		"args":            []string{"--addr=:" + strconv.Itoa(rt.HostPort)},
		"env":             envVars,
		"ports":           []map[string]any{{"containerPort": rt.HostPort}},
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
				"app.kubernetes.io/name":   "migrant-model-runtime",
				"migrant.io/model":         modelLabel,
				"migrant.io/runtime-model": runtimeModelLabel,
				"migrant.io/request-class": requestClassLabel,
				"migrant.io/prompt-len":    strconv.Itoa(rt.PromptLen),
				"migrant.io/output-tokens": strconv.Itoa(rt.OutputTokens),
				"migrant.io/runtime-id":    rid,
				"migrant.io/profile":       rt.Profile,
				"migrant.io/gpu":           rt.GPU,
			},
		},
		"spec": map[string]any{
			"replicas": 1,
			"strategy": map[string]any{"type": "Recreate"},
			"selector": map[string]any{"matchLabels": map[string]any{
				"app.kubernetes.io/name":   "migrant-model-runtime",
				"migrant.io/model":         modelLabel,
				"migrant.io/runtime-model": runtimeModelLabel,
				"migrant.io/request-class": requestClassLabel,
				"migrant.io/prompt-len":    strconv.Itoa(rt.PromptLen),
				"migrant.io/output-tokens": strconv.Itoa(rt.OutputTokens),
				"migrant.io/runtime-id":    rid,
			}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{
					"app.kubernetes.io/name":   "migrant-model-runtime",
					"migrant.io/model":         modelLabel,
					"migrant.io/runtime-model": runtimeModelLabel,
					"migrant.io/request-class": requestClassLabel,
					"migrant.io/prompt-len":    strconv.Itoa(rt.PromptLen),
					"migrant.io/output-tokens": strconv.Itoa(rt.OutputTokens),
					"migrant.io/runtime-id":    rid,
					"migrant.io/profile":       rt.Profile,
					"migrant.io/gpu":           rt.GPU,
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
				"spec": runtimePodSpec(rt, container),
			},
		},
	}
}

func runtimePodSpec(rt system.ModelRuntimeSpec, container map[string]any) map[string]any {
	spec := map[string]any{
		"nodeSelector":                  map[string]any{"kubernetes.io/hostname": rt.Node},
		"hostNetwork":                   true,
		"dnsPolicy":                     "ClusterFirstWithHostNet",
		"runtimeClassName":              "nvidia",
		"terminationGracePeriodSeconds": runtimeTerminationGraceSeconds(),
		"tolerations":                   []map[string]any{{"operator": "Exists"}},
		"containers":                    []map[string]any{container},
	}
	if !isLLMRuntime(rt) {
		return spec
	}
	container["volumeMounts"] = []map[string]any{{"name": "hf-cache", "mountPath": env("MODEL_RUNTIME_HF_HOME", "/opt/hf-cache")}}
	spec["volumes"] = []map[string]any{{"name": "hf-cache", "hostPath": map[string]any{
		"path": env("MODEL_RUNTIME_HF_CACHE_HOST_PATH", "/var/lib/or-sim/hf-cache"),
		"type": "DirectoryOrCreate",
	}}}
	spec["initContainers"] = []map[string]any{{
		"name":    "prepare-hf-cache",
		"image":   env("RUNTIME_INIT_IMAGE", "busybox:1.36"),
		"command": []string{"sh", "-c", "mkdir -p /cache/hub && chown -R 65532:65532 /cache"},
		"volumeMounts": []map[string]any{{
			"name":      "hf-cache",
			"mountPath": "/cache",
		}},
	}}
	return spec
}

func nodeIPs(client *kube.Client) (map[string]string, error) {
	var list map[string]any
	if _, err := client.Get(kube.Nodes(), &list); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, item := range asSlice(list["items"]) {
		node := asMap(item)
		meta := asMap(node["metadata"])
		name := asString(meta["name"])
		annotations := asMap(meta["annotations"])
		if provided := asString(annotations["alpha.kubernetes.io/provided-node-ip"]); provided != "" {
			out[name] = provided
			continue
		}
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
	items := asSlice(asMap(asMap(spec["validationTargets"])["targetAllocationPlan"])["desiredRuntimes"])
	if len(items) == 0 {
		items = asSlice(asMap(spec["summary"])["desiredRuntimes"])
	}
	if len(items) == 0 {
		items = asSlice(asMap(spec["podLifecyclePreview"])["desiredRuntimes"])
	}
	out := []system.ModelRuntimeSpec{}
	for _, item := range items {
		m := asMap(item)
		out = append(out, system.ModelRuntimeSpec{
			Model:           asString(m["model"]),
			RuntimeModel:    asString(m["runtimeModel"]),
			RequestClass:    asString(m["requestClass"]),
			PromptLen:       intNumber(m["promptLen"]),
			OutputTokens:    intNumber(m["outputTokens"]),
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

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds * float64(time.Second))
}

func runtimeReadyTimeout() time.Duration {
	return envDurationSeconds("RUNTIME_READY_TIMEOUT_SECONDS", 600*time.Second)
}

func runtimeTerminationGraceSeconds() int {
	raw := strings.TrimSpace(os.Getenv("RUNTIME_TERMINATION_GRACE_SECONDS"))
	if raw == "" {
		return 30
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		return 30
	}
	return seconds
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

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
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

func labelValue(value string) string {
	out := make([]rune, 0, len(value))
	lastDash := false
	for _, ch := range value {
		ok := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '*'
		if ok {
			out = append(out, ch)
			lastDash = false
			continue
		}
		if !lastDash {
			out = append(out, '-')
			lastDash = true
		}
	}
	label := strings.Trim(string(out), "-.*")
	if label == "" {
		return "x"
	}
	if len(label) > 63 {
		label = strings.Trim(label[:63], "-.*")
	}
	if label == "" {
		return "x"
	}
	return label
}

func runtimeModel(rt system.ModelRuntimeSpec) string {
	model := strings.TrimSpace(rt.RuntimeModel)
	if model == "" {
		model = strings.TrimSpace(rt.Model)
	}
	lower := strings.ToLower(model)
	switch {
	case strings.HasPrefix(lower, "gpt2_"):
		return "gpt2"
	case strings.HasPrefix(lower, "llama_"):
		return "llama"
	case strings.HasSuffix(lower, "_image"):
		return strings.TrimSuffix(lower, "_image")
	default:
		return model
	}
}

func runtimeImage(rt system.ModelRuntimeSpec) string {
	if isLLMRuntime(rt) {
		model := strings.ToLower(runtimeModel(rt))
		if strings.Contains(model, "gpt2") {
			return env("GPT2_MODEL_RUNTIME_IMAGE", env("LLM_GPT2_MODEL_RUNTIME_IMAGE", env("LLM_MODEL_RUNTIME_IMAGE", "localhost:10690/migrant-model-runtime:llm-transformers-20260622")))
		}
		if strings.Contains(model, "llama") {
			return env("LLAMA_MODEL_RUNTIME_IMAGE", env("LLM_LLAMA_MODEL_RUNTIME_IMAGE", env("LLM_MODEL_RUNTIME_IMAGE", "localhost:10690/migrant-model-runtime:llm-transformers-20260622")))
		}
		return env("LLM_MODEL_RUNTIME_IMAGE", "localhost:10690/migrant-model-runtime:llm-transformers-20260622")
	}
	if isVisionRuntime(rt) {
		return env("VISION_MODEL_RUNTIME_IMAGE", env("MODEL_RUNTIME_IMAGE", "localhost:10690/migrant-model-runtime:torchvision-20260602"))
	}
	return env("MODEL_RUNTIME_IMAGE", "localhost:10690/migrant-model-runtime:go")
}

func llmModelID(rt system.ModelRuntimeSpec) string {
	model := strings.ToLower(runtimeModel(rt))
	if strings.Contains(model, "gpt2") {
		return env("GPT2_MODEL_ID", env("LLM_GPT2_MODEL_ID", ""))
	}
	if strings.Contains(model, "llama") {
		return env("LLAMA_MODEL_ID", env("LLM_LLAMA_MODEL_ID", ""))
	}
	return env("LLM_MODEL_ID", "")
}

func runtimeMode(rt system.ModelRuntimeSpec) string {
	if isLLMRuntime(rt) {
		return env("LLM_RUNTIME_MODE", "transformers")
	}
	if isVisionRuntime(rt) {
		return env("VISION_RUNTIME_MODE", "torchvision")
	}
	return env("MODEL_RUNTIME_MODE", "synthetic")
}

func isLLMRuntime(rt system.ModelRuntimeSpec) bool {
	model := strings.ToLower(strings.TrimSpace(runtimeModel(rt)))
	workload := strings.ToLower(strings.TrimSpace(rt.Model))
	return strings.HasPrefix(model, "gpt") || strings.HasPrefix(workload, "gpt") ||
		strings.Contains(model, "llama") || strings.Contains(workload, "llama")
}

func isVisionRuntime(rt system.ModelRuntimeSpec) bool {
	model := strings.ToLower(strings.TrimSpace(runtimeModel(rt)))
	workload := strings.ToLower(strings.TrimSpace(rt.Model))
	switch model {
	case "resnet50", "resnet101", "vgg16", "vit_base", "vit_b_16", "mobilenet_v3_large", "efficientnet_b0", "convnext_tiny":
		return true
	}
	return strings.HasSuffix(workload, "_image")
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

func round(value float64, digits int) float64 {
	scale := math.Pow(10, float64(digits))
	return math.Round(value*scale) / scale
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
