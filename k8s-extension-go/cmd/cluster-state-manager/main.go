package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"or-sim/k8s-extension-go/internal/kube"
)

type runtimeBinding struct {
	Model           string
	BatchSize       int
	Pod             string
	Phase           string
	SlotResource    string
	DeviceResource  string
	ExpectedMIGUUID string
	Route           map[string]any
}

type nodeObservation struct {
	Success    bool      `json:"success"`
	NvidiaSMIL string    `json:"nvidiaSmiL"`
	Message    string    `json:"message"`
	MIGSlots   []migSlot `json:"migSlots"`
}

type migDevice struct {
	Profile string `json:"profile"`
	UUID    string `json:"uuid"`
	Start   int    `json:"start,omitempty"`
	End     int    `json:"end,omitempty"`
	Source  string `json:"source,omitempty"`
}

type migObservation struct {
	GPUUUIDs   map[int]string
	GPUProduct map[int]string
	Devices    map[int][]migDevice
}

type migSlot struct {
	SlotStart     int    `json:"slotStart"`
	SlotEnd       int    `json:"slotEnd"`
	Profile       string `json:"profile"`
	MIGDeviceUUID string `json:"migDeviceUuid"`
	Source        string `json:"source"`
}

var gpuLineRe = regexp.MustCompile(`^GPU ([0-9]+): (.+) \(UUID: ([^)]+)\)`)
var migLineRe = regexp.MustCompile(`^\s*MIG ([0-9]+g)\.[0-9]+gb\s+Device\s+[0-9]+:\s+\(UUID:\s+([^)]+)\)`)

func main() {
	ns := env("NAMESPACE", "or-sim")
	router := env("ROUTER_URL", "http://runtime-router:8080")
	client, err := kube.NewInCluster(ns)
	if err != nil {
		log.Fatal(err)
	}

	go reconcileLoop(client, router)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "component": "cluster-state-manager"})
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func reconcileLoop(client *kube.Client, router string) {
	trigger := make(chan struct{}, 1)
	go watchTrigger(client, kube.Nodes(), "nodes", trigger)
	go watchTrigger(client, kube.Pods(client.Namespace()), "pods", trigger)
	go watchTrigger(client, configMaps(client.Namespace()), "configmaps", trigger)
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		if err := reconcile(client, router); err != nil {
			log.Printf("reconcile failed: %v", err)
		}
		select {
		case <-trigger:
		case <-tick.C:
		}
	}
}

func configMaps(ns string) string {
	return "/api/v1/namespaces/" + ns + "/configmaps"
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
			log.Printf("cluster-state-manager watch %s ended: %v", label, err)
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
		log.Printf("cluster-state-manager list before watch failed for %s: %v", apiPath, err)
		return ""
	}
	return asString(asMap(list["metadata"])["resourceVersion"])
}

func reconcile(client *kube.Client, router string) error {
	var nodes map[string]any
	if _, err := client.Get(kube.Nodes(), &nodes); err != nil {
		return err
	}
	routingState := observeRoutingState(router)
	routeLookup := routesByModel(routingState)
	logicalLedger := observeLogicalBindingLedger(client)
	logicalBindings := asMap(logicalLedger["bindings"])
	runtimeByGPU, err := observeRuntimeBindings(client, routeLookup)
	if err != nil {
		return err
	}
	profileOverlay := observeProfileOverlay(router)
	bindings := map[string]any{}
	activeQueue := []string{}
	availableQueue := []string{}
	transitioningQueue := []string{}
	for _, item := range asSlice(nodes["items"]) {
		node := asMap(item)
		meta := asMap(node["metadata"])
		name := asString(meta["name"])
		labels := asMap(meta["labels"])
		if asString(labels["nvidia.com/mig.capable"]) != "true" {
			continue
		}
		observedMIG := observeNodeAgent(name, nodeInternalIP(node))
		gpuIndexes := observedGPUIndexes(observedMIG)
		if len(gpuIndexes) == 0 {
			count := atoi(asString(labels["nvidia.com/gpu.count"]))
			for i := 0; i < count; i++ {
				gpuIndexes = append(gpuIndexes, i)
			}
		}
		for _, i := range gpuIndexes {
			id := name + "-gpu" + strconv.Itoa(i)
			product := firstNonEmpty(observedMIG.GPUProduct[i], asString(labels["nvidia.com/gpu.product"]))
			migCapable := asString(labels["nvidia.com/mig.capable"]) == "true" && strings.Contains(strings.ToUpper(product), "A100")
			if !migCapable {
				continue
			}
			devices := observeNodeAgentSlots(name, nodeInternalIP(node), i)
			if len(devices) == 0 {
				devices = observedMIG.Devices[i]
			}
			repairPaused := asString(labels["mig.or-sim.io/repair-paused"]) == "true"
			runtimes := runtimeByGPU[id]
			logicalBinding := asMap(logicalBindings[id])
			activeLogicalID := asString(logicalBinding["activeLogicalGpuId"])
			pendingLogicalID := asString(logicalBinding["pendingLogicalGpuId"])
			state := "available"
			reason := "empty"
			requiredAction := ""
			if pendingLogicalID != "" {
				state = "transitioning"
				reason = "logical_binding_pending"
				transitioningQueue = append(transitioningQueue, id)
			} else if activeLogicalID != "" {
				state = "active"
				reason = "logical_binding_active"
				activeQueue = append(activeQueue, id)
			} else if len(runtimes) > 0 {
				state = "active"
				reason = "runtime_pod_running"
				activeQueue = append(activeQueue, id)
			} else if len(devices) > 0 {
				state = "transitioning"
				if repairPaused {
					reason = "repair_paused_mig_devices_present_without_runtime"
				} else {
					reason = "mig_devices_present_without_runtime"
					requiredAction = "clear_template_before_available"
				}
				transitioningQueue = append(transitioningQueue, id)
			} else {
				availableQueue = append(availableQueue, id)
			}
			cleanliness := "unknown"
			switch {
			case len(devices) == 0:
				cleanliness = "empty"
			case len(runtimes) > 0:
				cleanliness = "active"
			default:
				cleanliness = "dirty"
			}
			bindings[id] = map[string]any{
				"node":                name,
				"nodeName":            name,
				"gpuIndex":            i,
				"deviceIndex":         i,
				"product":             product,
				"gpuUUID":             observedMIG.GPUUUIDs[i],
				"gpuUuid":             observedMIG.GPUUUIDs[i],
				"physicalGpuId":       id,
				"migCapable":          migCapable,
				"migConfig":           asString(labels["nvidia.com/mig.config"]),
				"migConfigState":      asString(labels["nvidia.com/mig.config.state"]),
				"migDevices":          migDevicesAsMaps(devices),
				"logicalMigSlots":     migDevicesAsMaps(devices),
				"runtimeBindings":     runtimeBindingsAsMaps(runtimes),
				"cleanliness":         cleanliness,
					"state":               state,
					"availabilityReason":  reason,
					"requiredAction":      requiredAction,
					"repairPaused":        repairPaused,
					"activeLogicalGpuId":  nilIfEmpty(activeLogicalID),
				"pendingLogicalGpuId": nilIfEmpty(pendingLogicalID),
				"logicalBinding":      logicalBinding,
			}
		}
	}
	currentAllocation := buildCurrentAllocation(bindings, activeQueue, availableQueue)
	health := buildRegistryHealth(bindings, activeQueue, availableQueue, transitioningQueue)
	body := map[string]any{
		"apiVersion": "mig.or-sim.io/v1alpha1",
		"kind":       "PhysicalGpuRegistry",
		"metadata": map[string]any{
			"name":      "default",
			"namespace": client.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name": "migrant-go",
			},
		},
		"spec": map[string]any{"policy": map[string]any{
			"source":                                   "cluster-state-manager",
			"emptyMigConfig":                           nil,
			"requireMigConfigStateSuccessBeforeAvailable": nil,
		}},
	}
	namePath := kube.NamespacedResourceName(client.Namespace(), "physicalgpuregistries", "default")
	if err := client.Upsert(namePath, body, nil); err != nil {
		return err
	}
	status := map[string]any{
		"phase":              "Observed",
		"bindings":           bindings,
		"activeQueue":        activeQueue,
		"availableQueue":     availableQueue,
		"transitioningQueue": transitioningQueue,
		"queueCounts": map[string]any{
			"active":        len(activeQueue),
			"available":     len(availableQueue),
			"transitioning": len(transitioningQueue),
		},
		"profileCalibrationOverlay": profileOverlay,
		"routingState":              routingState,
		"logicalBindingLedger":      logicalLedger,
		"currentAllocation":         currentAllocation,
		"health":                    health,
		"observedAt":                time.Now().Format(time.RFC3339Nano),
	}
	return replaceRegistryStatus(client, namePath, status)
}

func buildRegistryHealth(bindings map[string]any, activeQueue, availableQueue, transitioningQueue []string) map[string]any {
	reasons := []string{}
	requiredActions := []map[string]any{}
	requiredByGPU := map[string]bool{}
	stable := len(transitioningQueue) == 0
	addClearAction := func(id string, binding map[string]any, reason string) {
		if id == "" || requiredByGPU[id] {
			return
		}
		if asBool(binding["repairPaused"]) {
			reasons = append(reasons, id+" repair paused: "+reason)
			return
		}
		requiredByGPU[id] = true
		requiredActions = append(requiredActions, map[string]any{
			"type":          "clear_template_before_available",
			"physicalGpuId": id,
			"node":          binding["node"],
			"gpuIndex":      binding["gpuIndex"],
			"reason":        reason,
		})
	}

	ids := make([]string, 0, len(bindings))
	for id := range bindings {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		binding := asMap(bindings[id])
		state := asString(binding["state"])
		cleanliness := asString(binding["cleanliness"])
		requiredAction := asString(binding["requiredAction"])
		migDevices := asSlice(binding["migDevices"])
		runtimes := asSlice(binding["runtimeBindings"])

		if state == "transitioning" || cleanliness == "dirty" {
			stable = false
			reason := id + " is not stable: state=" + state + ", cleanliness=" + cleanliness + ", reason=" + asString(binding["availabilityReason"])
			reasons = append(reasons, reason)
			addClearAction(id, binding, reason)
		}
		if requiredAction == "clear_template_before_available" {
			addClearAction(id, binding, firstNonEmpty(asString(binding["availabilityReason"]), "explicit clear_template_before_available"))
		}
		if state == "available" {
			if cleanliness != "empty" || len(migDevices) != 0 {
				stable = false
				reason := id + " is marked available but is not empty"
				reasons = append(reasons, reason)
				addClearAction(id, binding, reason)
			}
		}
		if state == "active" {
			if len(runtimes) == 0 {
				stable = false
				reason := id + " is marked active but has no runtime bindings"
				reasons = append(reasons, reason)
				addClearAction(id, binding, reason)
			}
			if binding["activeLogicalGpuId"] == nil || asString(binding["activeLogicalGpuId"]) == "" {
				stable = false
				reasons = append(reasons, id+" is marked active but has no active logical GPU id")
			}
			for _, rawRuntime := range runtimes {
				runtime := asMap(rawRuntime)
				if len(asMap(runtime["route"])) == 0 {
					stable = false
					reasons = append(reasons, id+" runtime "+asString(runtime["model"])+" has no active route snapshot")
				}
				if asString(runtime["expectedMigUuid"]) == "" {
					stable = false
					reasons = append(reasons, id+" runtime "+asString(runtime["model"])+" has no expected MIG UUID")
				}
			}
		}
	}
	return map[string]any{
		"stable":          stable,
		"repairRequired":  len(requiredActions) > 0,
		"reasons":         reasons,
		"requiredActions": requiredActions,
		"queueCounts": map[string]any{
			"active":        len(activeQueue),
			"available":     len(availableQueue),
			"transitioning": len(transitioningQueue),
		},
		"observedAt": time.Now().Format(time.RFC3339Nano),
	}
}

func replaceRegistryStatus(client *kube.Client, namePath string, status map[string]any) error {
	var current map[string]any
	if _, err := client.Get(namePath, &current); err != nil {
		return err
	}
	current["status"] = status
	code, err := client.Put(namePath+"/status", current, nil)
	if err != nil {
		return err
	}
	if code < 200 || code >= 300 {
		return httpError("replace registry status", namePath+"/status", code)
	}
	return nil
}

func httpError(action, path string, code int) error {
	return &httpStatusError{action: action, path: path, code: code}
}

type httpStatusError struct {
	action string
	path   string
	code   int
}

func (e *httpStatusError) Error() string {
	return e.action + " " + e.path + " returned " + strconv.Itoa(e.code)
}

func observeProfileOverlay(router string) map[string]any {
	resp, err := http.Get(strings.TrimRight(router, "/") + "/metrics/profile-observations")
	if err != nil {
		return map[string]any{"available": false, "error": err.Error()}
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return map[string]any{"available": false, "error": err.Error()}
	}
	payload["available"] = true
	return payload
}

func observeRoutingState(router string) map[string]any {
	resp, err := http.Get(strings.TrimRight(router, "/") + "/routes")
	if err != nil {
		return map[string]any{"available": false, "error": err.Error(), "routes": []any{}}
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return map[string]any{"available": false, "error": err.Error(), "routes": []any{}}
	}
	payload["available"] = true
	return payload
}

func observeLogicalBindingLedger(client *kube.Client) map[string]any {
	var cm map[string]any
	status, err := client.Get("/api/v1/namespaces/"+client.Namespace()+"/configmaps/logical-gpu-binding-ledger", &cm)
	if err != nil || status != http.StatusOK {
		return map[string]any{"available": false, "bindings": map[string]any{}}
	}
	raw := strings.TrimSpace(asString(asMap(cm["data"])["ledger.json"]))
	if raw == "" {
		return map[string]any{"available": true, "bindings": map[string]any{}}
	}
	var ledger map[string]any
	if err := json.Unmarshal([]byte(raw), &ledger); err != nil {
		return map[string]any{"available": false, "error": err.Error(), "bindings": map[string]any{}}
	}
	ledger["available"] = true
	if _, ok := ledger["bindings"]; !ok {
		ledger["bindings"] = map[string]any{}
	}
	return ledger
}

func buildCurrentAllocation(bindings map[string]any, activeQueue, availableQueue []string) map[string]any {
	activeRows := []map[string]any{}
	for _, physicalID := range activeQueue {
		binding := asMap(bindings[physicalID])
		if len(binding) == 0 {
			continue
		}
		previousLogicalID := asString(binding["activeLogicalGpuId"])
		row := map[string]any{
			"physicalGpuId":   physicalID,
			"node":            binding["node"],
			"gpuIndex":        binding["gpuIndex"],
			"layout":          layoutFromMIGDevices(asSlice(binding["migDevices"])),
			"migDevices":      binding["migDevices"],
			"runtimeBindings": binding["runtimeBindings"],
			"state":           binding["state"],
			"cleanliness":     binding["cleanliness"],
			"_sortKey":        physicalID,
		}
		if previousLogicalID != "" {
			row["previousLogicalGpuId"] = previousLogicalID
			row["_sortKey"] = previousLogicalID
		}
		activeRows = append(activeRows, row)
	}
	sort.Slice(activeRows, func(i, j int) bool {
		left := logicalSortKey(asString(activeRows[i]["_sortKey"]))
		right := logicalSortKey(asString(activeRows[j]["_sortKey"]))
		if left != right {
			return left < right
		}
		return asString(activeRows[i]["physicalGpuId"]) < asString(activeRows[j]["physicalGpuId"])
	})
	physicalIDMap := map[string]any{}
	logicalIDMap := map[string]any{}
	canonicalObservedMap := map[string]any{}
	gpus := map[string]any{}
	logicalGpus := []map[string]any{}
	for idx, row := range activeRows {
		newID := strconv.Itoa(idx)
		previousID := asString(row["previousLogicalGpuId"])
		physicalID := asString(row["physicalGpuId"])
		delete(row, "_sortKey")
		row["logicalGpuId"] = idx
		row["canonicalLogicalGpuId"] = idx
		if previousID != "" {
			row["logicalBindingSource"] = "ledger"
			logicalIDMap[previousID] = idx
		} else {
			row["activeLogicalGpuId"] = idx
			row["logicalBindingSource"] = "canonical-observed"
			canonicalObservedMap[physicalID] = idx
		}
		physicalIDMap[newID] = physicalID
		logicalGpus = append(logicalGpus, row)
		gpu := copyMap(asMap(bindings[physicalID]))
		gpu["logicalGpuId"] = idx
		gpu["canonicalLogicalGpuId"] = idx
		if previousID != "" {
			gpu["previousLogicalGpuId"] = previousID
			gpu["logicalBindingSource"] = "ledger"
		} else if gpu["activeLogicalGpuId"] == nil || asString(gpu["activeLogicalGpuId"]) == "" {
			gpu["activeLogicalGpuId"] = strconv.Itoa(idx)
			gpu["logicalBindingSource"] = "canonical-observed"
		}
		bindings[physicalID] = gpu
		gpus[physicalID] = gpu
	}
	return map[string]any{
		"format":          "migrant.current-allocation/v1",
		"source":          "cluster-state-manager",
		"canonicalizedAt": time.Now().Format(time.RFC3339Nano),
		"logicalGpuCount": len(logicalGpus),
		"logicalGpus":     logicalGpus,
		"gpus":            gpus,
		"metadata": map[string]any{
			"physical_id_map":                 physicalIDMap,
			"logical_id_map":                  logicalIDMap,
			"canonical_observed_physical_map": canonicalObservedMap,
		},
		"freePhysicalGpuPool": availableQueue,
	}
}

func layoutFromMIGDevices(devices []any) string {
	profiles := []string{}
	for _, raw := range devices {
		profile := asString(asMap(raw)["profile"])
		if profile != "" {
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		return "empty"
	}
	sort.Strings(profiles)
	return strings.Join(profiles, "+")
}

func logicalSortKey(value string) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 1 << 30
	}
	return parsed
}

func copyMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func routesByModel(routingState map[string]any) map[string][]map[string]any {
	out := map[string][]map[string]any{}
	for _, rawRoute := range asSlice(routingState["routes"]) {
		route := asMap(rawRoute)
		model := asString(route["model"])
		if model == "" {
			continue
		}
		out[model] = append(out[model], route)
	}
	return out
}

func observeRuntimeBindings(client *kube.Client, routes map[string][]map[string]any) (map[string][]runtimeBinding, error) {
	var pods map[string]any
	if _, err := client.Get(kube.Pods(client.Namespace()), &pods); err != nil {
		return nil, err
	}
	out := map[string][]runtimeBinding{}
	for _, item := range asSlice(pods["items"]) {
		pod := asMap(item)
		meta := asMap(pod["metadata"])
		labels := asMap(meta["labels"])
		if asString(labels["app.kubernetes.io/name"]) != "migrant-model-runtime" {
			continue
		}
		spec := asMap(pod["spec"])
		status := asMap(pod["status"])
		gpuID := asString(labels["migrant.io/gpu"])
		if gpuID == "" || asString(status["phase"]) != "Running" {
			continue
		}
		model := asString(labels["migrant.io/model"])
		slotResource := asString(asMap(meta["annotations"])["migrant.io/slot-resource"])
		expectedMIGUUID := asString(asMap(meta["annotations"])["migrant.io/expected-mig-uuid"])
		out[gpuID] = append(out[gpuID], runtimeBinding{
			Model:           model,
			BatchSize:       runtimeBatchSize(spec),
			Pod:             asString(meta["name"]),
			Phase:           asString(status["phase"]),
			SlotResource:    slotResource,
			DeviceResource:  asString(asMap(meta["annotations"])["migrant.io/device-resource"]),
			ExpectedMIGUUID: expectedMIGUUID,
			Route:           routeSummaryForBinding(routes[model], slotResource, expectedMIGUUID),
		})
	}
	return out, nil
}

func routeSummaryForBinding(routes []map[string]any, slotResource, expectedMIGUUID string) map[string]any {
	route := matchingRoute(routes, slotResource, expectedMIGUUID)
	if len(route) == 0 {
		return nil
	}
	keys := []string{
		"runtimeId", "endpoint", "weight", "capacity", "profile", "batchSize", "gpu", "slotResource", "deviceResource", "expectedMigUuid",
		"active", "acceptingNew", "draining", "arrivalRate", "requests", "errors", "errorRate",
		"inflight", "queued", "avgLatencyMs", "endpointRequests", "endpointInflight", "endpointAvgLatencyMs", "runtimeMetricsAvailable", "runtime.batchSize",
		"runtime.migUuid", "runtime.slotResource", "runtime.avgLatencyMs", "runtime.requests", "runtime.errors",
	}
	out := map[string]any{}
	for _, key := range keys {
		if value, ok := route[key]; ok {
			out[key] = value
		}
	}
	return out
}

func matchingRoute(routes []map[string]any, slotResource, expectedMIGUUID string) map[string]any {
	if len(routes) == 0 {
		return nil
	}
	for _, route := range routes {
		if expectedMIGUUID != "" && asString(route["expectedMigUuid"]) == expectedMIGUUID {
			return route
		}
		if expectedMIGUUID != "" && asString(route["runtime.migUuid"]) == expectedMIGUUID {
			return route
		}
		if slotResource != "" && asString(route["slotResource"]) == slotResource {
			return route
		}
		if slotResource != "" && asString(route["runtime.slotResource"]) == slotResource {
			return route
		}
	}
	return routes[0]
}

func runtimeBatchSize(podSpec map[string]any) int {
	for _, rawContainer := range asSlice(podSpec["containers"]) {
		container := asMap(rawContainer)
		if asString(container["name"]) != "runtime" {
			continue
		}
		for _, rawEnv := range asSlice(container["env"]) {
			env := asMap(rawEnv)
			if asString(env["name"]) == "BATCH_SIZE" {
				return atoi(asString(env["value"]))
			}
		}
	}
	return 0
}

func observeNodeAgent(nodeName, ip string) migObservation {
	if ip == "" {
		return migObservation{GPUUUIDs: map[int]string{}, Devices: map[int][]migDevice{}}
	}
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + ip + ":10684/list")
	if err != nil {
		log.Printf("node-agent list failed for %s: %v", nodeName, err)
		return migObservation{GPUUUIDs: map[int]string{}, Devices: map[int][]migDevice{}}
	}
	defer resp.Body.Close()
	var obs nodeObservation
	if err := json.NewDecoder(resp.Body).Decode(&obs); err != nil {
		log.Printf("node-agent list decode failed for %s: %v", nodeName, err)
		return migObservation{GPUUUIDs: map[int]string{}, Devices: map[int][]migDevice{}}
	}
	if !obs.Success {
		log.Printf("node-agent list unsuccessful for %s: %s", nodeName, obs.Message)
		return migObservation{GPUUUIDs: map[int]string{}, GPUProduct: map[int]string{}, Devices: map[int][]migDevice{}}
	}
	return parseNvidiaSMIL(obs.NvidiaSMIL)
}

func observeNodeAgentSlots(nodeName, ip string, gpuIndex int) []migDevice {
	if ip == "" {
		return nil
	}
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + ip + ":10684/list?gpuIndex=" + strconv.Itoa(gpuIndex))
	if err != nil {
		log.Printf("node-agent slot list failed for %s gpu%d: %v", nodeName, gpuIndex, err)
		return nil
	}
	defer resp.Body.Close()
	var obs nodeObservation
	if err := json.NewDecoder(resp.Body).Decode(&obs); err != nil {
		log.Printf("node-agent slot list decode failed for %s gpu%d: %v", nodeName, gpuIndex, err)
		return nil
	}
	if !obs.Success {
		log.Printf("node-agent slot list unsuccessful for %s gpu%d: %s", nodeName, gpuIndex, obs.Message)
		return nil
	}
	devices := []migDevice{}
	for _, slot := range obs.MIGSlots {
		if slot.Profile == "" || slot.SlotEnd <= slot.SlotStart {
			continue
		}
		devices = append(devices, migDevice{
			Profile: slot.Profile,
			UUID:    slot.MIGDeviceUUID,
			Start:   slot.SlotStart,
			End:     slot.SlotEnd,
			Source:  slot.Source,
		})
	}
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].Start != devices[j].Start {
			return devices[i].Start < devices[j].Start
		}
		if devices[i].End != devices[j].End {
			return devices[i].End < devices[j].End
		}
		return devices[i].Profile < devices[j].Profile
	})
	return devices
}

func parseNvidiaSMIL(raw string) migObservation {
	out := migObservation{GPUUUIDs: map[int]string{}, GPUProduct: map[int]string{}, Devices: map[int][]migDevice{}}
	currentGPU := -1
	for _, line := range strings.Split(raw, "\n") {
		if match := gpuLineRe.FindStringSubmatch(line); len(match) == 4 {
			currentGPU = atoi(match[1])
			out.GPUProduct[currentGPU] = strings.TrimSpace(match[2])
			out.GPUUUIDs[currentGPU] = match[3]
			out.Devices[currentGPU] = out.Devices[currentGPU]
			continue
		}
		if currentGPU >= 0 {
			if match := migLineRe.FindStringSubmatch(line); len(match) == 3 {
				out.Devices[currentGPU] = append(out.Devices[currentGPU], migDevice{Profile: match[1], UUID: match[2]})
			}
		}
	}
	return out
}

func observedGPUIndexes(obs migObservation) []int {
	seen := map[int]bool{}
	for idx := range obs.GPUUUIDs {
		seen[idx] = true
	}
	for idx := range obs.GPUProduct {
		seen[idx] = true
	}
	for idx := range obs.Devices {
		seen[idx] = true
	}
	out := make([]int, 0, len(seen))
	for idx := range seen {
		out = append(out, idx)
	}
	sort.Ints(out)
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

func migDevicesAsMaps(devices []migDevice) []map[string]any {
	out := []map[string]any{}
	for _, device := range devices {
		row := map[string]any{"profile": device.Profile, "uuid": device.UUID}
		if device.End > device.Start {
			row["start"] = device.Start
			row["end"] = device.End
		}
		if device.Source != "" {
			row["source"] = device.Source
		}
		out = append(out, row)
	}
	return out
}

func runtimeBindingsAsMaps(bindings []runtimeBinding) []map[string]any {
	out := []map[string]any{}
	for _, binding := range bindings {
		out = append(out, map[string]any{
			"model": binding.Model, "batchSize": binding.BatchSize, "pod": binding.Pod, "phase": binding.Phase,
			"slotResource": binding.SlotResource, "deviceResource": binding.DeviceResource, "expectedMigUuid": binding.ExpectedMIGUUID,
		})
		if len(binding.Route) > 0 {
			out[len(out)-1]["route"] = binding.Route
		}
	}
	return out
}

func nodeInternalIP(node map[string]any) string {
	meta := asMap(node["metadata"])
	annotations := asMap(meta["annotations"])
	if provided := asString(annotations["alpha.kubernetes.io/provided-node-ip"]); provided != "" {
		return provided
	}
	for _, addr := range asSlice(asMap(node["status"])["addresses"]) {
		item := asMap(addr)
		if asString(item["type"]) == "InternalIP" {
			return asString(item["address"])
		}
	}
	return ""
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
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
	if rows, ok := v.([]map[string]any); ok {
		out := make([]any, 0, len(rows))
		for _, row := range rows {
			out = append(out, row)
		}
		return out
	}
	if values, ok := v.([]string); ok {
		out := make([]any, 0, len(values))
		for _, value := range values {
			out = append(out, value)
		}
		return out
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

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}
