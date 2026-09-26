package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
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
)

type route struct {
	Model    string `json:"model"`
	Endpoint string `json:"endpoint"`
}

type routeEndpoint struct {
	Model           string  `json:"model"`
	RuntimeModel    string  `json:"runtimeModel,omitempty"`
	RequestClass    string  `json:"requestClass,omitempty"`
	PromptLen       int     `json:"promptLen,omitempty"`
	OutputTokens    int     `json:"outputTokens,omitempty"`
	RuntimeID       string  `json:"runtimeId"`
	Endpoint        string  `json:"endpoint"`
	Weight          float64 `json:"weight,omitempty"`
	Capacity        float64 `json:"capacity,omitempty"`
	Profile         string  `json:"profile,omitempty"`
	BatchSize       int     `json:"batchSize,omitempty"`
	GPU             string  `json:"gpu,omitempty"`
	SlotResource    string  `json:"slotResource,omitempty"`
	DeviceResource  string  `json:"deviceResource,omitempty"`
	ExpectedMIGUUID string  `json:"expectedMigUuid,omitempty"`
	Active          bool    `json:"active"`
	AcceptingNew    bool    `json:"acceptingNew"`
	Draining        bool    `json:"draining,omitempty"`
}

type batchResponse struct {
	Status      int
	ContentType string
	Body        []byte
}

type batchRequest struct {
	Model      string
	Body       []byte
	ArrivedAt  time.Time
	ModelStats *modelMetrics
	Done       chan batchResponse
}

type endpointBatcher struct {
	mu        sync.Mutex
	queue     []*batchRequest
	timer     *time.Timer
	runtimeID string
}

type latencySample struct {
	At        time.Time
	LatencyMs float64
	Failed    bool
}

type modelMetrics struct {
	mu           sync.Mutex
	Arrivals     []time.Time
	Latencies    []latencySample
	Requests     int64
	Errors       int64
	Inflight     int64
	TotalLatency float64
}

type monitorState struct {
	PlanName      string
	Active        bool
	StartedAt     time.Time
	FinishedAt    time.Time
	SourceArrival map[string]float64
	TargetArrival map[string]float64
	LatencySLOMs  map[string]float64
	Stats         map[string]*monitorStats
}

type monitorStats struct {
	Requests                 int64
	Errors                   int64
	TotalLatencyMs           float64
	MaxLatencyMs             float64
	LatencyViolations        int64
	LatencyViolationExcessMs float64
	FirstViolationAt         time.Time
	LastViolationAt          time.Time
}

type routerState struct {
	mu              sync.RWMutex
	routes          map[string][]routeEndpoint
	metrics         map[string]*modelMetrics
	endpointMetrics map[string]*modelMetrics
	batchers        map[string]*endpointBatcher
	routeGCFailures map[string]int
	monitor         monitorState
	window          time.Duration
	visionBatchWait time.Duration
	http            *http.Client
	kube            *kube.Client
	store           string
}

func main() {
	var addr string
	var window time.Duration
	var visionBatchWait time.Duration
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.DurationVar(&window, "arrival-window", 60*time.Second, "arrival-rate window")
	flag.DurationVar(&visionBatchWait, "vision-max-batch-wait", durationEnv("VISION_MAX_BATCH_WAIT", 5*time.Millisecond), "maximum router-side micro-batch wait for vision workloads")
	flag.Parse()

	ns := strings.TrimSpace(os.Getenv("NAMESPACE"))
	if ns == "" {
		ns = "or-sim"
	}
	store := strings.TrimSpace(os.Getenv("ROUTE_STORE_CONFIGMAP"))
	if store == "" {
		store = "runtime-router-routes"
	}
	kubeClient, err := kube.NewInCluster(ns)
	if err != nil {
		log.Printf("runtime-router route persistence disabled: %v", err)
	}
	routes := initialRoutes()
	for model, endpoints := range loadStoredRoutes(kubeClient, ns, store) {
		routes[model] = upsertEndpoints(routes[model], endpoints...)
	}
	state := &routerState{
		routes:          routes,
		metrics:         map[string]*modelMetrics{},
		endpointMetrics: map[string]*modelMetrics{},
		batchers:        map[string]*endpointBatcher{},
		routeGCFailures: map[string]int{},
		monitor:         monitorState{Stats: map[string]*monitorStats{}},
		window:          window,
		visionBatchWait: visionBatchWait,
		http:            &http.Client{Timeout: 30 * time.Second},
		kube:            kubeClient,
		store:           store,
	}
	if len(routes) > 0 {
		if err := state.persistRoutes(routes); err != nil {
			log.Printf("runtime-router startup route persist failed: %v", err)
		}
	}
	go state.routeGCLoop(
		durationEnv("ROUTE_GC_INTERVAL", 30*time.Second),
		durationEnv("ROUTE_GC_TIMEOUT", 3*time.Second),
		positiveIntEnv("ROUTE_GC_FAILURE_THRESHOLD", 3),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("/infer/", state.handleInfer)
	mux.HandleFunc("/routes", state.handleRouteSnapshot)
	mux.HandleFunc("/control/routes", state.handleRoutes)
	mux.HandleFunc("/control/monitor", state.handleMonitorControl)
	mux.HandleFunc("/metrics/demand", state.handleDemand)
	mux.HandleFunc("/metrics/slo", state.handleSLO)
	mux.HandleFunc("/metrics/profile-observations", state.handleProfileObservations)

	log.Printf("runtime-router listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *routerState) handleInfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "POST required"})
		return
	}
	model := strings.TrimPrefix(r.URL.Path, "/infer/")
	model = strings.Trim(model, "/")
	if model == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing model"})
		return
	}
	selected, ok := s.routeFor(model)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown model", "model": model})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	logicalCount := logicalRequestCountFromBody(body)
	if isVisionModel(model) && s.visionBatchWait >= 0 && !driverBatchedRequest(body) {
		s.handleBatchedInfer(w, r, model, selected, body)
		return
	}

	s.proxyInfer(w, r, model, selected, body, logicalCount)
}

func (s *routerState) proxyInfer(w http.ResponseWriter, r *http.Request, model string, selected routeEndpoint, body []byte, logicalCount int64) {
	if logicalCount <= 0 {
		logicalCount = 1
	}
	endpoint := strings.TrimRight(selected.Endpoint, "/")
	metrics := s.metricsFor(model)
	endpointMetrics := s.metricsForEndpoint(selected.RuntimeID)
	started := time.Now()
	metrics.beginN(started, logicalCount)
	endpointMetrics.beginN(started, logicalCount)
	status := http.StatusBadGateway
	var responseBody []byte
	defer func() {
		elapsed := time.Since(started)
		failed := status >= 500
		metrics.finishN(elapsed, failed, logicalCount)
		endpointMetrics.finishN(elapsed, failed, logicalCount)
		s.recordMonitorSampleN(model, elapsed, failed, logicalCount)
	}()

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint+"/infer", bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "model": model, "endpoint": endpoint})
		return
	}
	defer resp.Body.Close()
	responseBody, err = io.ReadAll(resp.Body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	status = resp.StatusCode
	w.Header().Set("content-type", resp.Header.Get("content-type"))
	if w.Header().Get("content-type") == "" {
		w.Header().Set("content-type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(responseBody)
}

func (s *routerState) handleBatchedInfer(w http.ResponseWriter, r *http.Request, model string, selected routeEndpoint, body []byte) {
	metrics := s.metricsFor(model)
	arrived := time.Now()
	metrics.begin(arrived)
	req := &batchRequest{
		Model:      model,
		Body:       append([]byte(nil), body...),
		ArrivedAt:  arrived,
		ModelStats: metrics,
		Done:       make(chan batchResponse, 1),
	}
	s.batcherFor(selected.RuntimeID).enqueue(s, selected, req)
	select {
	case response := <-req.Done:
		contentType := response.ContentType
		if contentType == "" {
			contentType = "application/json"
		}
		w.Header().Set("content-type", contentType)
		w.WriteHeader(response.Status)
		_, _ = w.Write(response.Body)
	case <-r.Context().Done():
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": r.Context().Err().Error(), "model": model})
	}
}

func (s *routerState) batcherFor(runtimeID string) *endpointBatcher {
	if runtimeID == "" {
		runtimeID = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batchers[runtimeID] == nil {
		s.batchers[runtimeID] = &endpointBatcher{runtimeID: runtimeID}
	}
	return s.batchers[runtimeID]
}

func (b *endpointBatcher) enqueue(s *routerState, endpoint routeEndpoint, req *batchRequest) {
	b.mu.Lock()
	b.queue = append(b.queue, req)
	maxBatch := endpoint.BatchSize
	if maxBatch <= 0 {
		maxBatch = 1
	}
	if len(b.queue) >= maxBatch {
		batch := b.queue
		b.queue = nil
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
		b.mu.Unlock()
		go s.dispatchBatch(endpoint, batch)
		return
	}
	if len(b.queue) == 1 {
		wait := s.visionBatchWait
		if wait <= 0 {
			wait = time.Nanosecond
		}
		b.timer = time.AfterFunc(wait, func() {
			b.flush(s, endpoint)
		})
	}
	b.mu.Unlock()
}

func (b *endpointBatcher) flush(s *routerState, endpoint routeEndpoint) {
	b.mu.Lock()
	batch := b.queue
	b.queue = nil
	b.timer = nil
	b.mu.Unlock()
	if len(batch) > 0 {
		s.dispatchBatch(endpoint, batch)
	}
}

func (b *endpointBatcher) takeQueued() []*batchRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	batch := b.queue
	b.queue = nil
	return batch
}

func (b *endpointBatcher) queued() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

func (s *routerState) drainBatcher(endpoint routeEndpoint) {
	if endpoint.RuntimeID == "" {
		return
	}
	b := s.batcherFor(endpoint.RuntimeID)
	pending := b.takeQueued()
	for _, req := range pending {
		s.redispatchQueuedRequest(req, endpoint.RuntimeID, endpoint)
	}
}

func (s *routerState) redispatchQueuedRequest(req *batchRequest, excludeRuntimeID string, fallback routeEndpoint) {
	endpoint, ok := s.routeForExcluding(req.Model, excludeRuntimeID)
	if ok {
		s.batcherFor(endpoint.RuntimeID).enqueue(s, endpoint, req)
		return
	}
	go s.dispatchBatch(fallback, []*batchRequest{req})
}

func (s *routerState) dispatchBatch(endpoint routeEndpoint, batch []*batchRequest) {
	if len(batch) == 0 {
		return
	}
	serviceStarted := time.Now()
	endpointMetrics := s.metricsForEndpoint(endpoint.RuntimeID)
	for range batch {
		endpointMetrics.begin(serviceStarted)
	}
	payload := map[string]any{}
	if err := json.Unmarshal(batch[0].Body, &payload); err != nil {
		s.finishBatch(endpointMetrics, batch, serviceStarted, http.StatusBadRequest, map[string]any{"error": err.Error(), "model": batch[0].Model}, true)
		return
	}
	payload["batch"] = len(batch)
	payload["logicalRequestCount"] = len(batch)
	raw, _ := json.Marshal(payload)
	runtimeEndpoint := strings.TrimRight(endpoint.Endpoint, "/")
	req, err := http.NewRequest(http.MethodPost, runtimeEndpoint+"/infer", bytes.NewReader(raw))
	if err != nil {
		s.finishBatch(endpointMetrics, batch, serviceStarted, http.StatusBadGateway, map[string]any{"error": err.Error(), "model": batch[0].Model, "endpoint": runtimeEndpoint}, true)
		return
	}
	req.Header.Set("content-type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		s.finishBatch(endpointMetrics, batch, serviceStarted, http.StatusBadGateway, map[string]any{"error": err.Error(), "model": batch[0].Model, "endpoint": runtimeEndpoint}, true)
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.finishBatch(endpointMetrics, batch, serviceStarted, http.StatusBadGateway, map[string]any{"error": err.Error(), "model": batch[0].Model}, true)
		return
	}
	contentType := resp.Header.Get("content-type")
	enriched := enrichBatchResponse(body, map[string]any{
		"routerBatchSize":         len(batch),
		"routerEndpointRuntimeId": endpoint.RuntimeID,
		"routerDispatchAt":        serviceStarted.Format(time.RFC3339Nano),
		"serviceLatencyMs":        round(float64(time.Since(serviceStarted).Microseconds())/1000.0, 6),
	})
	failed := resp.StatusCode >= 500
	serviceLatency := time.Since(serviceStarted)
	for _, item := range batch {
		queueWait := serviceStarted.Sub(item.ArrivedAt)
		e2e := time.Since(item.ArrivedAt)
		responseBody := enrichBatchResponse(enriched, map[string]any{
			"queueWaitMs":  round(float64(queueWait.Microseconds())/1000.0, 6),
			"e2eLatencyMs": round(float64(e2e.Microseconds())/1000.0, 6),
		})
		item.ModelStats.finish(serviceLatency, failed)
		endpointMetrics.finish(serviceLatency, failed)
		s.recordMonitorSample(item.Model, serviceLatency, failed)
		item.Done <- batchResponse{Status: resp.StatusCode, ContentType: contentType, Body: responseBody}
	}
}

func (s *routerState) finishBatch(endpointMetrics *modelMetrics, batch []*batchRequest, serviceStarted time.Time, status int, payload map[string]any, failed bool) {
	body, _ := json.Marshal(payload)
	serviceLatency := time.Since(serviceStarted)
	for _, item := range batch {
		item.ModelStats.finish(serviceLatency, failed)
		endpointMetrics.finish(serviceLatency, failed)
		s.recordMonitorSample(item.Model, serviceLatency, failed)
		item.Done <- batchResponse{Status: status, ContentType: "application/json", Body: body}
	}
}

func driverBatchedRequest(raw []byte) bool {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	if value, ok := payload["driverBatched"].(bool); ok && value {
		return true
	}
	return intNumber(payload["logicalRequestCount"]) > 1
}

func logicalRequestCountFromBody(raw []byte) int64 {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 1
	}
	count := intNumber(payload["logicalRequestCount"])
	if count <= 0 {
		count = intNumber(payload["batch"])
	}
	if count <= 0 {
		return 1
	}
	return int64(count)
}

func enrichBatchResponse(raw []byte, extra map[string]any) []byte {
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		out["rawResponse"] = string(raw)
	}
	for key, value := range extra {
		out[key] = value
	}
	body, _ := json.Marshal(out)
	return body
}

func (s *routerState) handleRouteSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET required"})
		return
	}
	now := time.Now()
	writeJSON(w, http.StatusOK, map[string]any{
		"routes":      s.routeSnapshot(now),
		"generatedAt": now.Format(time.RFC3339Nano),
	})
}

func (s *routerState) handleRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		defer s.mu.RUnlock()
		routes := make([]routeEndpoint, 0)
		for _, endpoints := range s.routes {
			routes = append(routes, endpoints...)
		}
		writeJSON(w, http.StatusOK, map[string]any{"routes": routes})
	case http.MethodPut, http.MethodPost:
		var input routeEndpoint
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if input.Model == "" || input.Endpoint == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model and endpoint are required"})
			return
		}
		input = normalizeEndpoint(input)
		s.mu.Lock()
		s.routes[input.Model] = upsertEndpoints(s.routes[input.Model], input)
		routes := s.copyRoutesLocked()
		s.mu.Unlock()
		if input.Draining || !input.AcceptingNew {
			s.drainBatcher(input)
		}
		if err := s.persistRoutes(routes); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, input)
	case http.MethodDelete:
		model := strings.TrimSpace(r.URL.Query().Get("model"))
		if model == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model query parameter is required"})
			return
		}
		runtimeID := strings.TrimSpace(r.URL.Query().Get("runtimeId"))
		var removed []routeEndpoint
		s.mu.Lock()
		if runtimeID == "" {
			removed = append(removed, s.routes[model]...)
			delete(s.routes, model)
		} else {
			for _, endpoint := range s.routes[model] {
				if endpoint.RuntimeID == runtimeID {
					removed = append(removed, endpoint)
				}
			}
			s.routes[model] = deleteEndpoint(s.routes[model], runtimeID)
			if len(s.routes[model]) == 0 {
				delete(s.routes, model)
			}
		}
		routes := s.copyRoutesLocked()
		s.mu.Unlock()
		for _, endpoint := range removed {
			s.drainBatcher(endpoint)
		}
		if err := s.persistRoutes(routes); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"model": model, "runtimeId": runtimeID, "deleted": true})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET, PUT, POST, or DELETE required"})
	}
}

func (s *routerState) handleMonitorControl(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.monitorSnapshot())
	case http.MethodPost, http.MethodPut:
		var input map[string]any
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		phase := firstNonEmpty(asString(input["phase"]), asString(input["action"]))
		now := time.Now()
		s.mu.Lock()
		switch phase {
		case "start", "begin", "active":
			s.monitor = monitorState{
				PlanName:      asString(input["planName"]),
				Active:        true,
				StartedAt:     now,
				SourceArrival: numberMap(input["sourceArrival"]),
				TargetArrival: numberMap(input["targetArrival"]),
				LatencySLOMs:  latencySLOMap(input),
				Stats:         map[string]*monitorStats{},
			}
		case "finish", "end", "stop", "inactive":
			s.monitor.Active = false
			s.monitor.FinishedAt = now
			if planName := asString(input["planName"]); planName != "" {
				s.monitor.PlanName = planName
			}
		default:
			if planName := asString(input["planName"]); planName != "" {
				s.monitor.PlanName = planName
			}
			if source := numberMap(input["sourceArrival"]); len(source) > 0 {
				s.monitor.SourceArrival = source
			}
			if target := numberMap(input["targetArrival"]); len(target) > 0 {
				s.monitor.TargetArrival = target
			}
			if slo := latencySLOMap(input); len(slo) > 0 {
				s.monitor.LatencySLOMs = slo
			}
			if active, ok := boolValue(input["active"]); ok {
				s.monitor.Active = active
				if active && s.monitor.StartedAt.IsZero() {
					s.monitor.StartedAt = now
				}
				if !active {
					s.monitor.FinishedAt = now
				}
			}
			if s.monitor.Stats == nil {
				s.monitor.Stats = map[string]*monitorStats{}
			}
		}
		snapshot := s.monitorSnapshotLocked()
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, snapshot)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET, PUT, or POST required"})
	}
}

func (s *routerState) routeSnapshot(now time.Time) []map[string]any {
	metricsByModel := s.snapshotMetrics(now)
	metricsByEndpoint := s.snapshotEndpointMetrics(now)
	s.mu.RLock()
	routes := make(map[string][]routeEndpoint, len(s.routes))
	for model, endpoints := range s.routes {
		routes[model] = append([]routeEndpoint(nil), endpoints...)
	}
	s.mu.RUnlock()

	models := make([]string, 0, len(routes))
	for model := range routes {
		models = append(models, model)
	}
	sort.Strings(models)

	out := make([]map[string]any, 0, len(models))
	for _, model := range models {
		endpoints := append([]routeEndpoint(nil), routes[model]...)
		sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].RuntimeID < endpoints[j].RuntimeID })
		for _, endpoint := range endpoints {
			endpointMetrics := metricsByEndpoint[endpoint.RuntimeID]
			row := metricsRow(model, endpointMetrics, s.window)
			row["modelArrivalRate"] = round(float64(len(metricsByModel[model].Arrivals))/s.window.Seconds(), 4)
			row["runtimeId"] = endpoint.RuntimeID
			row["endpoint"] = strings.TrimRight(endpoint.Endpoint, "/")
			row["weight"] = effectiveWeight(endpoint)
			row["capacity"] = endpoint.Capacity
			row["profile"] = endpoint.Profile
			runtimeMetrics := s.runtimeMetrics(endpoint.Endpoint)
			row["batchSize"] = endpoint.BatchSize
			row["driverBatchSize"] = trueBatchSize(endpoint, runtimeMetrics)
			row["gpu"] = endpoint.GPU
			row["slotResource"] = endpoint.SlotResource
			row["deviceResource"] = endpoint.DeviceResource
			row["expectedMigUuid"] = endpoint.ExpectedMIGUUID
			row["active"] = endpoint.Active
			row["acceptingNew"] = endpoint.AcceptingNew
			row["draining"] = endpoint.Draining
			row["endpointRequests"] = endpointMetrics.Requests
			row["endpointInflight"] = endpointMetrics.Inflight
			row["endpointQueued"] = s.endpointQueued(endpoint.RuntimeID)
			row["endpointAvgLatencyMs"] = round(avgLatency(endpointMetrics), 3)
			for key, value := range runtimeMetrics {
				row[key] = value
			}
			if endpointLatency := asFloat(row["endpointAvgLatencyMs"]); endpointLatency > 0 {
				if runtimeLatency := asFloat(row["runtime.runtimeLatencyMs"]); runtimeLatency > 0 {
					row["networkOverheadMs"] = round(math.Max(0, endpointLatency-runtimeLatency), 3)
				}
			}
			out = append(out, row)
		}
	}
	return out
}

func (s *routerState) copyRoutesLocked() map[string][]routeEndpoint {
	out := make(map[string][]routeEndpoint, len(s.routes))
	for model, endpoints := range s.routes {
		out[model] = append([]routeEndpoint(nil), endpoints...)
	}
	return out
}

func (s *routerState) persistRoutes(routes map[string][]routeEndpoint) error {
	if s.kube == nil {
		return nil
	}
	raw, err := json.Marshal(routes)
	if err != nil {
		return err
	}
	body := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      s.store,
			"namespace": s.kube.Namespace(),
			"labels": map[string]any{
				"app.kubernetes.io/name":      "migrant-runtime-router",
				"migrant.io/routing-state":    "true",
				"app.kubernetes.io/component": "runtime-router",
			},
		},
		"data": map[string]any{
			"routes.json": string(raw),
			"updatedAt":   time.Now().Format(time.RFC3339Nano),
		},
	}
	return s.kube.Upsert(configMapPath(s.kube.Namespace(), s.store), body, nil)
}

func (s *routerState) handleDemand(w http.ResponseWriter, _ *http.Request) {
	now := time.Now()
	models := []map[string]any{}
	for model, metrics := range s.snapshotMetrics(now) {
		models = append(models, metricsRow(model, metrics, s.window))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"windowSeconds": s.window.Seconds(),
		"models":        models,
		"generatedAt":   now.Format(time.RFC3339Nano),
	})
}

func (s *routerState) handleSLO(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "GET required"})
		return
	}
	writeJSON(w, http.StatusOK, s.monitorSnapshot())
}

func (s *routerState) handleProfileObservations(w http.ResponseWriter, _ *http.Request) {
	now := time.Now()
	items := []map[string]any{}
	for model, metrics := range s.snapshotMetrics(now) {
		row := metricsRow(model, metrics, s.window)
		row["sampleCount"] = metrics.Requests
		row["confidence"] = confidence(metrics.Requests)
		if endpoint, ok := s.routeFor(model); ok {
			row["runtimeId"] = endpoint.RuntimeID
			row["endpoint"] = endpoint.Endpoint
			row["profile"] = endpoint.Profile
			runtimeMetrics := s.runtimeMetrics(endpoint.Endpoint)
			row["batchSize"] = endpoint.BatchSize
			row["driverBatchSize"] = trueBatchSize(endpoint, runtimeMetrics)
			row["slotResource"] = endpoint.SlotResource
			row["deviceResource"] = endpoint.DeviceResource
			row["expectedMigUuid"] = endpoint.ExpectedMIGUUID
			for key, value := range runtimeMetrics {
				row[key] = value
			}
		}
		items = append(items, row)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"observations": items,
		"generatedAt":  now.Format(time.RFC3339Nano),
	})
}

func (s *routerState) runtimeMetrics(endpoint string) map[string]any {
	ctxClient := http.Client{Timeout: 3 * time.Second}
	resp, err := ctxClient.Get(strings.TrimRight(endpoint, "/") + "/metrics")
	if err != nil {
		return map[string]any{"runtimeMetricsAvailable": false, "runtimeMetricsError": err.Error()}
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return map[string]any{"runtimeMetricsAvailable": false, "runtimeMetricsError": err.Error()}
	}
	out := map[string]any{"runtimeMetricsAvailable": true}
	for _, key := range []string{
		"model", "runtimeId", "runtimeMode", "torchvisionModel", "weightsMode", "device", "imageSize",
		"batchSize", "migUuid", "slotResource", "deviceResource", "expectedMigUuid",
		"avgLatencyMs", "runtimeLatencyMs", "runtimeThroughput", "lastRuntimeLatencyMs",
		"requests", "errors", "loaded", "loadError",
	} {
		if value, ok := payload[key]; ok {
			out["runtime."+key] = value
		}
	}
	return out
}

func (s *routerState) routeGCLoop(interval, timeout time.Duration, failureThreshold int) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if removed, err := s.gcStaleRoutes(timeout, failureThreshold); err != nil {
			log.Printf("runtime-router route GC failed: %v", err)
		} else if removed > 0 {
			log.Printf("runtime-router route GC removed %d stale endpoint(s)", removed)
		}
	}
}

func (s *routerState) gcStaleRoutes(timeout time.Duration, failureThreshold int) (int, error) {
	s.mu.RLock()
	routes := s.copyRoutesLocked()
	s.mu.RUnlock()
	if len(routes) == 0 {
		return 0, nil
	}
	client := http.Client{Timeout: timeout}
	type staleEndpoint struct {
		model     string
		runtimeID string
	}
	stale := []staleEndpoint{}
	for model, endpoints := range routes {
		for _, endpoint := range endpoints {
			if endpoint.RuntimeID == "" || endpoint.Endpoint == "" {
				continue
			}
			resp, err := client.Get(strings.TrimRight(endpoint.Endpoint, "/") + "/healthz")
			if err == nil && resp != nil {
				_ = resp.Body.Close()
			}
			healthy := err == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 500
			if s.routeHealthFailed(endpoint.RuntimeID, healthy, failureThreshold) {
				stale = append(stale, staleEndpoint{model: model, runtimeID: endpoint.RuntimeID})
			}
		}
	}
	if len(stale) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	removed := 0
	for _, item := range stale {
		before := len(s.routes[item.model])
		s.routes[item.model] = deleteEndpoint(s.routes[item.model], item.runtimeID)
		if len(s.routes[item.model]) == 0 {
			delete(s.routes, item.model)
		}
		removed += before - len(s.routes[item.model])
		delete(s.routeGCFailures, item.runtimeID)
	}
	updated := s.copyRoutesLocked()
	s.mu.Unlock()
	if removed == 0 {
		return 0, nil
	}
	return removed, s.persistRoutes(updated)
}

func (s *routerState) routeHealthFailed(runtimeID string, healthy bool, threshold int) bool {
	if threshold < 1 {
		threshold = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routeGCFailures == nil {
		s.routeGCFailures = map[string]int{}
	}
	if healthy {
		delete(s.routeGCFailures, runtimeID)
		return false
	}
	s.routeGCFailures[runtimeID]++
	return s.routeGCFailures[runtimeID] >= threshold
}

func (s *routerState) routeFor(model string) (routeEndpoint, bool) {
	return s.routeForExcluding(model, "")
}

func (s *routerState) routeForExcluding(model, excludeRuntimeID string) (routeEndpoint, bool) {
	s.mu.RLock()
	endpoints := append([]routeEndpoint(nil), s.routes[model]...)
	s.mu.RUnlock()
	if len(endpoints) == 0 {
		return routeEndpoint{}, false
	}
	var best routeEndpoint
	bestScore := math.Inf(1)
	found := false
	for _, endpoint := range endpoints {
		if excludeRuntimeID != "" && endpoint.RuntimeID == excludeRuntimeID {
			continue
		}
		if !endpoint.Active || !endpoint.AcceptingNew || endpoint.Draining {
			continue
		}
		metrics := s.metricsForEndpoint(endpoint.RuntimeID).snapshot(time.Now(), s.window)
		score := float64(metrics.Inflight+int64(s.endpointQueued(endpoint.RuntimeID))) / effectiveWeight(endpoint)
		if !found || score < bestScore || (score == bestScore && endpoint.RuntimeID < best.RuntimeID) {
			best = endpoint
			bestScore = score
			found = true
		}
	}
	if found {
		return best, true
	}
	for _, endpoint := range endpoints {
		if excludeRuntimeID != "" && endpoint.RuntimeID == excludeRuntimeID {
			continue
		}
		if endpoint.Active && !endpoint.Draining {
			return endpoint, true
		}
	}
	return routeEndpoint{}, false
}

func (s *routerState) endpointQueued(runtimeID string) int {
	if runtimeID == "" {
		return 0
	}
	s.mu.RLock()
	b := s.batchers[runtimeID]
	s.mu.RUnlock()
	if b == nil {
		return 0
	}
	return b.queued()
}

func (s *routerState) metricsFor(model string) *modelMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics[model] == nil {
		s.metrics[model] = &modelMetrics{}
	}
	return s.metrics[model]
}

func (s *routerState) metricsForEndpoint(runtimeID string) *modelMetrics {
	if runtimeID == "" {
		runtimeID = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endpointMetrics[runtimeID] == nil {
		s.endpointMetrics[runtimeID] = &modelMetrics{}
	}
	return s.endpointMetrics[runtimeID]
}

func (s *routerState) snapshotMetrics(now time.Time) map[string]modelMetrics {
	s.mu.RLock()
	models := make([]string, 0, len(s.metrics))
	for model := range s.metrics {
		models = append(models, model)
	}
	s.mu.RUnlock()
	out := map[string]modelMetrics{}
	for _, model := range models {
		m := s.metricsFor(model)
		out[model] = m.snapshot(now, s.window)
	}
	return out
}

func (s *routerState) snapshotEndpointMetrics(now time.Time) map[string]modelMetrics {
	s.mu.RLock()
	ids := make([]string, 0, len(s.endpointMetrics))
	for id := range s.endpointMetrics {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	out := map[string]modelMetrics{}
	for _, id := range ids {
		m := s.metricsForEndpoint(id)
		out[id] = m.snapshot(now, s.window)
	}
	return out
}

func (m *modelMetrics) begin(now time.Time) {
	m.beginN(now, 1)
}

func (m *modelMetrics) beginN(now time.Time, count int64) {
	if count <= 0 {
		count = 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Inflight += count
	for i := int64(0); i < count; i++ {
		m.Arrivals = append(m.Arrivals, now)
	}
}

func (m *modelMetrics) finish(latency time.Duration, failed bool) {
	m.finishN(latency, failed, 1)
}

func (m *modelMetrics) finishN(latency time.Duration, failed bool, count int64) {
	if count <= 0 {
		count = 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Inflight -= count
	if m.Inflight < 0 {
		m.Inflight = 0
	}
	m.Requests += count
	if failed {
		m.Errors += count
	}
	latencyMs := float64(latency.Milliseconds())
	m.TotalLatency += latencyMs * float64(count)
	now := time.Now()
	for i := int64(0); i < count; i++ {
		m.Latencies = append(m.Latencies, latencySample{At: now, LatencyMs: latencyMs, Failed: failed})
	}
}

func (m *modelMetrics) snapshot(now time.Time, window time.Duration) modelMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := now.Add(-window)
	kept := m.Arrivals[:0]
	for _, arrival := range m.Arrivals {
		if arrival.After(cutoff) {
			kept = append(kept, arrival)
		}
	}
	m.Arrivals = kept
	keptLatencies := m.Latencies[:0]
	for _, sample := range m.Latencies {
		if sample.At.After(cutoff) {
			keptLatencies = append(keptLatencies, sample)
		}
	}
	m.Latencies = keptLatencies
	cp := *m
	cp.Arrivals = append([]time.Time(nil), kept...)
	cp.Latencies = append([]latencySample(nil), keptLatencies...)
	return cp
}

func metricsRow(model string, metrics modelMetrics, window time.Duration) map[string]any {
	windowRequests := int64(len(metrics.Latencies))
	windowErrors := int64(0)
	errorRate := 0.0
	for _, sample := range metrics.Latencies {
		if sample.Failed {
			windowErrors++
		}
	}
	if windowRequests > 0 {
		errorRate = float64(windowErrors) / float64(windowRequests)
	}
	return map[string]any{
		"model":              model,
		"arrivalRate":        round(float64(len(metrics.Arrivals))/window.Seconds(), 4),
		"requests":           metrics.Requests,
		"errors":             metrics.Errors,
		"windowRequests":     windowRequests,
		"windowErrors":       windowErrors,
		"errorRate":          round(errorRate, 4),
		"inflight":           metrics.Inflight,
		"queued":             0,
		"avgLatencyMs":       round(avgLatency(metrics), 3),
		"p95LatencyMs":       round(percentileLatency(metrics, 0.95), 3),
		"maxLatencyMs":       round(maxLatency(metrics), 3),
		"demandRatePolicy":   "fixed_input_not_observed",
		"demandRateViolated": false,
	}
}

func confidence(samples int64) string {
	switch {
	case samples >= 100:
		return "high"
	case samples >= 20:
		return "medium"
	case samples > 0:
		return "low"
	default:
		return "none"
	}
}

func (s *routerState) recordMonitorSample(model string, latency time.Duration, failed bool) {
	s.recordMonitorSampleN(model, latency, failed, 1)
}

func (s *routerState) recordMonitorSampleN(model string, latency time.Duration, failed bool, count int64) {
	if count <= 0 {
		count = 1
	}
	latencyMs := float64(latency.Milliseconds())
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.monitor.Active {
		return
	}
	if s.monitor.Stats == nil {
		s.monitor.Stats = map[string]*monitorStats{}
	}
	stats := s.monitor.Stats[model]
	if stats == nil {
		stats = &monitorStats{}
		s.monitor.Stats[model] = stats
	}
	stats.Requests += count
	if failed {
		stats.Errors += count
	}
	stats.TotalLatencyMs += latencyMs * float64(count)
	if latencyMs > stats.MaxLatencyMs {
		stats.MaxLatencyMs = latencyMs
	}
	if slo := s.monitor.LatencySLOMs[model]; slo > 0 && latencyMs > slo {
		stats.LatencyViolations += count
		stats.LatencyViolationExcessMs += (latencyMs - slo) * float64(count)
		if stats.FirstViolationAt.IsZero() {
			stats.FirstViolationAt = now
		}
		stats.LastViolationAt = now
	}
}

func (s *routerState) monitorSnapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.monitorSnapshotLocked()
}

func (s *routerState) monitorSnapshotLocked() map[string]any {
	models := map[string]any{}
	for model, stats := range s.monitor.Stats {
		avg := 0.0
		if stats.Requests > 0 {
			avg = stats.TotalLatencyMs / float64(stats.Requests)
		}
		row := map[string]any{
			"requests":                   stats.Requests,
			"errors":                     stats.Errors,
			"avgLatencyMs":               round(avg, 3),
			"maxLatencyMs":               round(stats.MaxLatencyMs, 3),
			"latencySLOMs":               s.monitor.LatencySLOMs[model],
			"latencyViolationCount":      stats.LatencyViolations,
			"latencySLOViolationSeconds": round(stats.LatencyViolationExcessMs/1000.0, 6),
			"latencySLOViolated":         stats.LatencyViolations > 0,
			"demandRate":                 s.monitor.TargetArrival[model],
			"sourceDemandRate":           s.monitor.SourceArrival[model],
			"demandRatePolicy":           "fixed_input_not_observed",
			"demandRateSLOEvaluated":     false,
			"demandRateSLOViolated":      false,
		}
		if !stats.FirstViolationAt.IsZero() {
			row["firstViolationAt"] = stats.FirstViolationAt.Format(time.RFC3339Nano)
			row["lastViolationAt"] = stats.LastViolationAt.Format(time.RFC3339Nano)
			row["latencySLOViolationWallSeconds"] = round(stats.LastViolationAt.Sub(stats.FirstViolationAt).Seconds(), 6)
		}
		models[model] = row
	}
	out := map[string]any{
		"planName":         s.monitor.PlanName,
		"active":           s.monitor.Active,
		"sourceArrival":    s.monitor.SourceArrival,
		"targetArrival":    s.monitor.TargetArrival,
		"latencySLOMs":     s.monitor.LatencySLOMs,
		"models":           models,
		"demandRatePolicy": "fixed_input_not_observed",
		"generatedAt":      time.Now().Format(time.RFC3339Nano),
	}
	if !s.monitor.StartedAt.IsZero() {
		out["startedAt"] = s.monitor.StartedAt.Format(time.RFC3339Nano)
	}
	if !s.monitor.FinishedAt.IsZero() {
		out["finishedAt"] = s.monitor.FinishedAt.Format(time.RFC3339Nano)
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	raw, _ := json.Marshal(payload)
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func envDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return strings.TrimRight(value, "/")
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("invalid %s=%q, using %s", key, value, fallback)
		return fallback
	}
	return parsed
}

func positiveIntEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		log.Printf("invalid %s=%q, using %d", key, value, fallback)
		return fallback
	}
	return parsed
}

func initialRoutes() map[string][]routeEndpoint {
	routes := map[string][]routeEndpoint{}
	for _, item := range []struct {
		model string
		key   string
	}{
		{model: "gpt2", key: "GPT2_ENDPOINT"},
		{model: "resnet50", key: "RESNET50_ENDPOINT"},
		{model: "llama", key: "LLAMA_ENDPOINT"},
	} {
		value := strings.TrimSpace(os.Getenv(item.key))
		if value != "" {
			endpoint := normalizeEndpoint(routeEndpoint{Model: item.model, Endpoint: value})
			routes[item.model] = []routeEndpoint{endpoint}
		}
	}
	return routes
}

func loadStoredRoutes(client *kube.Client, ns, name string) map[string][]routeEndpoint {
	if client == nil {
		return map[string][]routeEndpoint{}
	}
	var cm map[string]any
	status, err := client.Get(configMapPath(ns, name), &cm)
	if err != nil {
		if status != http.StatusNotFound {
			log.Printf("runtime-router route store load failed: %v", err)
		}
		return map[string][]routeEndpoint{}
	}
	raw := strings.TrimSpace(asString(asMap(cm["data"])["routes.json"]))
	if raw == "" {
		return map[string][]routeEndpoint{}
	}
	out := map[string][]routeEndpoint{}
	if err := json.Unmarshal([]byte(raw), &out); err == nil {
		for model, endpoints := range out {
			normalized := []routeEndpoint{}
			for _, endpoint := range endpoints {
				endpoint.Model = firstNonEmpty(endpoint.Model, model)
				normalized = append(normalized, normalizeEndpoint(endpoint))
			}
			out[model] = normalized
		}
		return out
	}
	legacy := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		log.Printf("runtime-router route store decode failed: %v", err)
		return map[string][]routeEndpoint{}
	}
	for model, endpoint := range legacy {
		out[model] = []routeEndpoint{normalizeEndpoint(routeEndpoint{Model: model, Endpoint: endpoint})}
	}
	return out
}

func configMapPath(ns, name string) string {
	return "/api/v1/namespaces/" + ns + "/configmaps/" + name
}

func round(value float64, digits int) float64 {
	pow := math.Pow10(digits)
	return math.Round(value*pow) / pow
}

func avgLatency(metrics modelMetrics) float64 {
	if len(metrics.Latencies) == 0 {
		return 0
	}
	total := 0.0
	for _, sample := range metrics.Latencies {
		total += sample.LatencyMs
	}
	return total / float64(len(metrics.Latencies))
}

func maxLatency(metrics modelMetrics) float64 {
	out := 0.0
	for _, sample := range metrics.Latencies {
		if sample.LatencyMs > out {
			out = sample.LatencyMs
		}
	}
	return out
}

func percentileLatency(metrics modelMetrics, p float64) float64 {
	if len(metrics.Latencies) == 0 {
		return 0
	}
	values := make([]float64, 0, len(metrics.Latencies))
	for _, sample := range metrics.Latencies {
		values = append(values, sample.LatencyMs)
	}
	sort.Float64s(values)
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func normalizeEndpoint(endpoint routeEndpoint) routeEndpoint {
	endpoint.Endpoint = strings.TrimRight(endpoint.Endpoint, "/")
	if endpoint.RuntimeID == "" {
		endpoint.RuntimeID = runtimeIDFromEndpoint(endpoint.Model, endpoint.Endpoint)
	}
	if endpoint.Weight <= 0 {
		endpoint.Weight = endpoint.Capacity
	}
	if endpoint.Weight <= 0 {
		endpoint.Weight = 1
	}
	if !endpoint.Active {
		endpoint.Active = true
	}
	if !endpoint.AcceptingNew && !endpoint.Draining {
		endpoint.AcceptingNew = true
	}
	return endpoint
}

func upsertEndpoints(existing []routeEndpoint, updates ...routeEndpoint) []routeEndpoint {
	out := append([]routeEndpoint(nil), existing...)
	for _, update := range updates {
		update = normalizeEndpoint(update)
		replaced := false
		for idx := range out {
			if out[idx].RuntimeID == update.RuntimeID {
				out[idx] = update
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, update)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuntimeID < out[j].RuntimeID })
	return out
}

func deleteEndpoint(existing []routeEndpoint, runtimeID string) []routeEndpoint {
	out := []routeEndpoint{}
	for _, endpoint := range existing {
		if endpoint.RuntimeID != runtimeID {
			out = append(out, endpoint)
		}
	}
	return out
}

func effectiveWeight(endpoint routeEndpoint) float64 {
	if endpoint.Weight > 0 {
		return endpoint.Weight
	}
	if endpoint.Capacity > 0 {
		return endpoint.Capacity
	}
	return 1
}

func trueBatchSize(endpoint routeEndpoint, runtimeMetrics map[string]any) int {
	if endpoint.BatchSize > 0 {
		return endpoint.BatchSize
	}
	return intNumber(runtimeMetrics["runtime.batchSize"])
}

func isVisionModel(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return false
	}
	if strings.HasPrefix(model, "gpt") || strings.HasPrefix(model, "llama") {
		return false
	}
	if strings.HasSuffix(model, "_image") {
		return true
	}
	switch model {
	case "resnet50", "resnet101", "vgg16", "vit_base", "vit_b_16", "mobilenet_v3_large", "efficientnet_b0", "convnext_tiny":
		return true
	default:
		return false
	}
}

func runtimeIDFromEndpoint(model, endpoint string) string {
	raw := strings.ToLower(model + "-" + endpoint)
	replacer := strings.NewReplacer("http://", "", "https://", "", ":", "-", "/", "-", ".", "-")
	raw = replacer.Replace(raw)
	raw = strings.Trim(raw, "-")
	if raw == "" {
		return "runtime"
	}
	return raw
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func asFloat(v any) float64 {
	value, _ := optionalFloat(v)
	return value
}

func optionalFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		n, _ := x.Float64()
		return n, true
	default:
		return 0, false
	}
}

func intNumber(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case float32:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case json.Number:
		n, _ := x.Int64()
		return int(n)
	case string:
		value, err := strconv.Atoi(strings.TrimSpace(x))
		if err == nil {
			return value
		}
	default:
		return 0
	}
	return 0
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func numberMap(v any) map[string]float64 {
	out := map[string]float64{}
	for key, raw := range asMap(v) {
		if value, ok := optionalFloat(raw); ok {
			out[key] = value
		}
	}
	return out
}

func latencySLOMap(input map[string]any) map[string]float64 {
	for _, key := range []string{"latencySLOMs", "registeredSLOMs"} {
		if values := numberMap(input[key]); len(values) > 0 {
			return values
		}
	}
	slo := asMap(input["slo"])
	out := map[string]float64{}
	for model, raw := range slo {
		row := asMap(raw)
		for _, key := range []string{"latencyMs", "e2eMs", "sloMs", "latencySLOMs"} {
			if value, ok := optionalFloat(row[key]); ok {
				out[model] = value
				break
			}
		}
	}
	return out
}

func boolValue(v any) (bool, bool) {
	if b, ok := v.(bool); ok {
		return b, true
	}
	if s, ok := v.(string); ok {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "true", "1", "yes", "active":
			return true, true
		case "false", "0", "no", "inactive":
			return false, true
		}
	}
	return false, false
}
