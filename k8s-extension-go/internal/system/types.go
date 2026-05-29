package system

type DemandResponse struct {
	WindowSeconds float64       `json:"windowSeconds"`
	GeneratedAt   string        `json:"generatedAt"`
	Models        []DemandModel `json:"models"`
}

type DemandModel struct {
	Model        string  `json:"model"`
	ArrivalRate  float64 `json:"arrivalRate"`
	Requests     int64   `json:"requests"`
	Errors       int64   `json:"errors"`
	ErrorRate    float64 `json:"errorRate"`
	Inflight     int64   `json:"inflight"`
	Queued       int64   `json:"queued"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
}

type StageSpec struct {
	Name          string             `json:"name"`
	TargetArrival map[string]float64 `json:"targetArrival"`
	Models        []ModelRuntimeSpec `json:"models"`
}

type ModelRuntimeSpec struct {
	Model           string `json:"model"`
	BatchSize       int    `json:"batchSize"`
	Node            string `json:"node"`
	HostPort        int    `json:"hostPort"`
	Profile         string `json:"profile"`
	GPU             string `json:"gpu"`
	SlotResource    string `json:"slotResource,omitempty"`
	DeviceResource  string `json:"deviceResource,omitempty"`
	ExpectedMIGUUID string `json:"expectedMigUuid,omitempty"`
}

func DefaultStages() []StageSpec {
	return []StageSpec{
		{
			Name: "stage0",
			TargetArrival: map[string]float64{
				"llama": 0.05, "gpt2": 0.20, "resnet50": 0.30,
			},
			Models: []ModelRuntimeSpec{
				{Model: "llama", BatchSize: 2, Node: "ampere", HostPort: 10681, Profile: "3g", GPU: "ampere-gpu0"},
				{Model: "gpt2", BatchSize: 4, Node: "ampere", HostPort: 10682, Profile: "2g", GPU: "ampere-gpu1"},
				{Model: "resnet50", BatchSize: 8, Node: "rtx1-worker", HostPort: 10683, Profile: "1g", GPU: "rtx1-worker-gpu0"},
			},
		},
		{
			Name: "stage1",
			TargetArrival: map[string]float64{
				"llama": 0.08, "gpt2": 0.30, "resnet50": 0.45,
			},
			Models: []ModelRuntimeSpec{
				{Model: "llama", BatchSize: 2, Node: "ampere", HostPort: 10681, Profile: "4g", GPU: "ampere-gpu0"},
				{Model: "gpt2", BatchSize: 4, Node: "ampere", HostPort: 10682, Profile: "2g", GPU: "ampere-gpu1"},
				{Model: "resnet50", BatchSize: 8, Node: "rtx1-worker", HostPort: 10683, Profile: "1g", GPU: "rtx1-worker-gpu0"},
			},
		},
		{
			Name: "stage2",
			TargetArrival: map[string]float64{
				"llama": 0.04, "gpt2": 0.16, "resnet50": 0.18,
			},
			Models: []ModelRuntimeSpec{
				{Model: "llama", BatchSize: 2, Node: "ampere", HostPort: 10681, Profile: "3g", GPU: "ampere-gpu0"},
				{Model: "gpt2", BatchSize: 4, Node: "ampere", HostPort: 10682, Profile: "1g", GPU: "ampere-gpu1"},
			},
		},
		{
			Name: "stage3",
			TargetArrival: map[string]float64{
				"llama": 0.07, "gpt2": 0.25, "resnet50": 0.35,
			},
			Models: []ModelRuntimeSpec{
				{Model: "llama", BatchSize: 2, Node: "ampere", HostPort: 10681, Profile: "3g", GPU: "ampere-gpu0"},
				{Model: "gpt2", BatchSize: 4, Node: "ampere", HostPort: 10682, Profile: "2g", GPU: "ampere-gpu1"},
				{Model: "resnet50", BatchSize: 8, Node: "rtx1-worker", HostPort: 10683, Profile: "1g", GPU: "rtx1-worker-gpu0"},
			},
		},
	}
}
