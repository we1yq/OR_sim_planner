package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTrueBatchSizePrefersRuntimeMetrics(t *testing.T) {
	endpoint := routeEndpoint{BatchSize: 1}
	if got := trueBatchSize(endpoint, map[string]any{"runtime.batchSize": 32}); got != 32 {
		t.Fatalf("true batch = %d, want runtime-observed 32", got)
	}
	if got := trueBatchSize(endpoint, nil); got != 1 {
		t.Fatalf("true batch fallback = %d, want configured 1", got)
	}
}

func TestRouteHealthRequiresConsecutiveFailures(t *testing.T) {
	state := &routerState{routeGCFailures: map[string]int{}}
	if state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("first health failure must not remove the route")
	}
	if state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("second health failure must not remove the route")
	}
	if !state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("third consecutive health failure must remove the route")
	}
}

func TestRouteHealthSuccessResetsFailureCount(t *testing.T) {
	state := &routerState{routeGCFailures: map[string]int{}}
	state.routeHealthFailed("runtime-a", false, 3)
	state.routeHealthFailed("runtime-a", false, 3)
	if state.routeHealthFailed("runtime-a", true, 3) {
		t.Fatal("successful health check must not remove the route")
	}
	if state.routeHealthFailed("runtime-a", false, 3) {
		t.Fatal("failure count must restart after a successful health check")
	}
}

func newTestRouter(endpoints ...routeEndpoint) *routerState {
	state := &routerState{
		routes:          map[string][]routeEndpoint{},
		metrics:         map[string]*modelMetrics{},
		endpointMetrics: map[string]*modelMetrics{},
		batchers:        map[string]*endpointBatcher{},
		routeGCFailures: map[string]int{},
		window:          time.Minute,
		http:            &http.Client{Timeout: time.Second},
	}
	for _, endpoint := range endpoints {
		state.routes[endpoint.Model] = upsertEndpoints(state.routes[endpoint.Model], endpoint)
	}
	return state
}

func TestRoutePatchKeepsRequestShapeFields(t *testing.T) {
	state := newTestRouter(routeEndpoint{
		Model: "llama_p2048_o64", RuntimeModel: "llama", RequestClass: "p2048/o64", PromptLen: 2048, OutputTokens: 64,
		RuntimeID: "rt-a", Endpoint: "http://a:1", BatchSize: 1, Capacity: 0.655, Active: true, AcceptingNew: true,
	})
	body := `{"model":"llama_p2048_o64","runtimeId":"rt-a","draining":true,"acceptingNew":false}`
	rec := httptest.NewRecorder()
	state.handleRoutes(rec, httptest.NewRequest(http.MethodPatch, "/control/routes", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d: %s", rec.Code, rec.Body.String())
	}
	got := state.routes["llama_p2048_o64"][0]
	if !got.Draining || got.AcceptingNew {
		t.Fatalf("drain not applied: %+v", got)
	}
	if got.RuntimeModel != "llama" || got.RequestClass != "p2048/o64" || got.PromptLen != 2048 || got.OutputTokens != 64 || got.Capacity != 0.655 || got.BatchSize != 1 {
		t.Fatalf("PATCH dropped stored fields: %+v", got)
	}

	rec = httptest.NewRecorder()
	state.handleRoutes(rec, httptest.NewRequest(http.MethodPatch, "/control/routes", strings.NewReader(`{"model":"llama_p2048_o64","runtimeId":"rt-a","batchSize":4}`)))
	if got := state.routes["llama_p2048_o64"][0]; rec.Code != http.StatusOK || got.BatchSize != 4 || !got.Draining || got.PromptLen != 2048 {
		t.Fatalf("batch PATCH = %d %+v", rec.Code, got)
	}
}

func TestRoutePatchUnknownReplicaIsNotFound(t *testing.T) {
	state := newTestRouter(routeEndpoint{Model: "m", RuntimeID: "rt-a", Endpoint: "http://a:1"})
	rec := httptest.NewRecorder()
	state.handleRoutes(rec, httptest.NewRequest(http.MethodPatch, "/control/routes", strings.NewReader(`{"model":"m","runtimeId":"missing","draining":true}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("PATCH of unknown replica = %d, want 404", rec.Code)
	}
	if len(state.routes["m"]) != 1 || state.routes["m"][0].Draining {
		t.Fatalf("PATCH of unknown replica must not change routes: %+v", state.routes["m"])
	}
}

func TestRouteMutationsAdvanceVersionAndStalePersistIsSkipped(t *testing.T) {
	state := newTestRouter()
	state.mu.Lock()
	_, v1 := state.commitRoutesLocked()
	_, v2 := state.commitRoutesLocked()
	state.mu.Unlock()
	if v2 != v1+1 {
		t.Fatalf("versions %d, %d must be consecutive", v1, v2)
	}
	if err := state.persistRoutes(map[string][]routeEndpoint{}, v2); err != nil {
		t.Fatal(err)
	}
	// The older snapshot arrives late; it must not replace the newer one.
	if err := state.persistRoutes(map[string][]routeEndpoint{}, v1); err != nil {
		t.Fatal(err)
	}
	if state.persistedVersion != v2 {
		t.Fatalf("persistedVersion = %d after stale persist, want %d", state.persistedVersion, v2)
	}
}

func TestRouteSnapshotFilterQueriesOnlyThatRuntime(t *testing.T) {
	calls := map[string]int{}
	srv := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[name]++
			_ = json.NewEncoder(w).Encode(map[string]any{})
		}))
	}
	a, b := srv("a"), srv("b")
	defer a.Close()
	defer b.Close()
	state := newTestRouter(
		routeEndpoint{Model: "m", RuntimeID: "rt-a", Endpoint: a.URL},
		routeEndpoint{Model: "m", RuntimeID: "rt-b", Endpoint: b.URL},
	)
	rows := state.routeSnapshot(time.Now(), "rt-b")
	if len(rows) != 1 || rows[0]["runtimeId"] != "rt-b" {
		t.Fatalf("filtered snapshot = %v", rows)
	}
	if calls["a"] != 0 || calls["b"] == 0 {
		t.Fatalf("filtered snapshot queried runtimes %v", calls)
	}
	if rows := state.routeSnapshot(time.Now(), ""); len(rows) != 2 {
		t.Fatalf("unfiltered snapshot rows = %d, want 2", len(rows))
	}
}

func concurrencyProbe(t *testing.T) (*httptest.Server, *int64) {
	var current, peak int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := atomic.AddInt64(&current, 1)
		for {
			old := atomic.LoadInt64(&peak)
			if now <= old || atomic.CompareAndSwapInt64(&peak, old, now) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &peak
}

func TestReplicaRunsOneBatchAtATime(t *testing.T) {
	for _, limit := range []int{1, 0} {
		srv, peak := concurrencyProbe(t)
		state := newTestRouter()
		state.visionPipelineDepth = limit
		endpoint := routeEndpoint{Model: "resnet50_image", RuntimeID: "rt-r", Endpoint: srv.URL, BatchSize: 4}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := &batchRequest{Model: "resnet50_image", Body: []byte(`{"batch":1}`), ArrivedAt: time.Now(), ModelStats: state.metricsFor("resnet50_image"), Done: make(chan batchResponse, 1)}
				state.dispatchBatch(endpoint, []*batchRequest{req})
			}()
		}
		wg.Wait()
		got := atomic.LoadInt64(peak)
		if limit == 1 && got != 1 {
			t.Fatalf("limit 1: runtime saw %d concurrent batches", got)
		}
		if limit == 0 && got < 2 {
			t.Fatalf("limit 0 (unlimited): expected concurrency, saw %d", got)
		}
	}
}

func TestReplicaRunsOneLLMRequestAtATime(t *testing.T) {
	srv, peak := concurrencyProbe(t)
	state := newTestRouter()
	state.maxEndpointConcurrency = 1
	endpoint := routeEndpoint{Model: "gpt2_p64_o64", RuntimeID: "rt-g", Endpoint: srv.URL}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/infer/gpt2_p64_o64", strings.NewReader(`{}`))
			state.proxyInfer(rec, req, "gpt2_p64_o64", endpoint, []byte(`{}`), 1)
			if rec.Code != http.StatusOK {
				t.Errorf("proxy status %d", rec.Code)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(peak); got != 1 {
		t.Fatalf("runtime saw %d concurrent LLM requests, want 1", got)
	}
}

func TestBatchesFillUnderLoadAndRunOneAtATime(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	var current, peak int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := atomic.AddInt64(&current, 1)
		if now > atomic.LoadInt64(&peak) {
			atomic.StoreInt64(&peak, now)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		sizes = append(sizes, int(body["batch"].(float64)))
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	state := newTestRouter()
	state.visionPipelineDepth = 1
	state.visionBatchWait = 5 * time.Millisecond
	endpoint := routeEndpoint{Model: "resnet50_image", RuntimeID: "rt-r", Endpoint: srv.URL, BatchSize: 16}
	reqs := make([]*batchRequest, 64)
	for i := range reqs {
		reqs[i] = &batchRequest{Model: "resnet50_image", Body: []byte(`{"batch":1}`), ArrivedAt: time.Now(), ModelStats: state.metricsFor("resnet50_image"), Done: make(chan batchResponse, 1)}
		state.batcherFor(endpoint.RuntimeID).enqueue(state, endpoint, reqs[i])
	}
	for _, req := range reqs {
		select {
		case <-req.Done:
		case <-time.After(5 * time.Second):
			t.Fatal("request not served")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if atomic.LoadInt64(&peak) != 1 {
		t.Fatalf("peak concurrent batches = %d, want 1", peak)
	}
	total := 0
	for i, size := range sizes {
		total += size
		if i > 0 && size != 16 {
			t.Fatalf("batch %d size %d under load, want 16 (sizes %v)", i, size, sizes)
		}
	}
	if total != 64 {
		t.Fatalf("served %d requests in batches %v, want 64", total, sizes)
	}
}

func TestIdleReplicaSendsPartialBatchAfterWait(t *testing.T) {
	var sizes []int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		sizes = append(sizes, int(body["batch"].(float64)))
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	state := newTestRouter()
	state.visionPipelineDepth = 2
	state.visionBatchWait = 5 * time.Millisecond
	endpoint := routeEndpoint{Model: "vgg16_image", RuntimeID: "rt-v", Endpoint: srv.URL, BatchSize: 16}
	req := &batchRequest{Model: "vgg16_image", Body: []byte(`{"batch":1}`), ArrivedAt: time.Now(), ModelStats: state.metricsFor("vgg16_image"), Done: make(chan batchResponse, 1)}
	state.batcherFor(endpoint.RuntimeID).enqueue(state, endpoint, req)
	select {
	case <-req.Done:
	case <-time.After(2 * time.Second):
		t.Fatal("lone request never dispatched")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 1 || sizes[0] != 1 {
		t.Fatalf("idle batches = %v, want [1]", sizes)
	}
}

// batchProbe records batch sizes and peak concurrency of a runtime whose
// batches take 20 ms.
func batchProbe(t *testing.T) (*httptest.Server, func() ([]int, int64)) {
	var mu sync.Mutex
	var sizes []int
	var current, peak int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := atomic.AddInt64(&current, 1)
		for {
			old := atomic.LoadInt64(&peak)
			if now <= old || atomic.CompareAndSwapInt64(&peak, old, now) {
				break
			}
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		sizes = append(sizes, int(body["batch"].(float64)))
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() ([]int, int64) {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), sizes...), atomic.LoadInt64(&peak)
	}
}

func enqueueAndWait(t *testing.T, state *routerState, endpoint routeEndpoint, n int) {
	t.Helper()
	reqs := make([]*batchRequest, n)
	for i := range reqs {
		reqs[i] = &batchRequest{Model: endpoint.Model, Body: []byte(`{"batch":1}`), ArrivedAt: time.Now(), ModelStats: state.metricsFor(endpoint.Model), Done: make(chan batchResponse, 1)}
		state.batcherFor(endpoint.RuntimeID).enqueue(state, endpoint, reqs[i])
	}
	for _, req := range reqs {
		select {
		case <-req.Done:
		case <-time.After(5 * time.Second):
			t.Fatal("request not served")
		}
	}
}

func TestPipelineKeepsTwoFullBatchesInFlight(t *testing.T) {
	srv, result := batchProbe(t)
	state := newTestRouter()
	state.visionPipelineDepth = 2
	state.visionBatchWait = 5 * time.Millisecond
	endpoint := routeEndpoint{Model: "resnet50_image", RuntimeID: "rt-r", Endpoint: srv.URL, BatchSize: 16}
	enqueueAndWait(t, state, endpoint, 96)
	sizes, peak := result()
	if peak != 2 {
		t.Fatalf("peak batches in flight = %d, want 2", peak)
	}
	for i, size := range sizes {
		if size != 16 {
			t.Fatalf("batch %d size %d, want 16 (sizes %v)", i, size, sizes)
		}
	}
}

func TestPipelineDoesNotSplitSecondBatch(t *testing.T) {
	srv, result := batchProbe(t)
	state := newTestRouter()
	state.visionPipelineDepth = 2
	state.visionBatchWait = 5 * time.Millisecond
	endpoint := routeEndpoint{Model: "vgg16_image", RuntimeID: "rt-v", Endpoint: srv.URL, BatchSize: 16}
	enqueueAndWait(t, state, endpoint, 19)
	sizes, peak := result()
	if peak != 1 || len(sizes) != 2 || sizes[0] != 16 || sizes[1] != 3 {
		t.Fatalf("sizes %v peak %d, want [16 3] one at a time (partial batch only once the replica is idle)", sizes, peak)
	}
}

// A replica's next batch must not wait for the previous batch's responses to
// be handed back: that bookkeeping takes locks shared with arrivals and
// /routes, and holding the pipeline slot through it idled the GPU.
func TestNextBatchStartsBeforeResponsesAreHandedBack(t *testing.T) {
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	state := newTestRouter()
	state.visionPipelineDepth = 1
	endpoint := routeEndpoint{Model: "vgg16_image", RuntimeID: "rt-v", Endpoint: srv.URL, BatchSize: 1}
	stats := state.metricsFor("vgg16_image")
	stats.mu.Lock() // blocks the hand-back of every response (ModelStats.finish)
	b := state.batcherFor(endpoint.RuntimeID)
	for i := 0; i < 3; i++ {
		b.enqueue(state, endpoint, &batchRequest{Model: "vgg16_image", Body: []byte(`{"batch":1}`), ArrivedAt: time.Now(),
			ModelStats: stats, Done: make(chan batchResponse, 1)})
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&calls) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := atomic.LoadInt64(&calls)
	stats.mu.Unlock()
	if got != 3 {
		t.Fatalf("runtime saw %d batches while responses were held back, want 3", got)
	}
}
