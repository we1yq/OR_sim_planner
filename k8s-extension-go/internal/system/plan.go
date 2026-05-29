package system

type PlanningInput struct {
	Source                string             `json:"source"`
	Mode                  string             `json:"mode"`
	Epoch                 string             `json:"epoch"`
	WindowSeconds         int                `json:"windowSeconds"`
	Unit                  string             `json:"unit"`
	ObservedAt            string             `json:"observedAt"`
	TriggerReason         string             `json:"triggerReason"`
	TargetArrival         map[string]float64 `json:"targetArrival"`
	RegisteredSLOMs       map[string]float64 `json:"registeredSLOMs"`
	RequestCount          map[string]int64   `json:"requestCount,omitempty"`
	ProfileCatalogRef     string             `json:"profileCatalogRef"`
	CalibrationOverlayRef string             `json:"calibrationOverlayRef"`
	CurrentAllocationRef  string             `json:"currentAllocationRef"`
	PlacementNodes        []string           `json:"placementNodes,omitempty"`
}

type CurrentAllocation struct {
	GPUs                map[string]CurrentGPU `json:"gpus"`
	AllowedNodes        []string              `json:"allowedNodes,omitempty"`
	LogicalGPUs         []map[string]any      `json:"logicalGpus,omitempty"`
	Metadata            map[string]any        `json:"metadata,omitempty"`
	FreePhysicalGPUPool []string              `json:"freePhysicalGpuPool,omitempty"`
}

type CurrentGPU struct {
	ID              string            `json:"id"`
	Node            string            `json:"node"`
	GPUIndex        int               `json:"gpuIndex"`
	State           string            `json:"state"`
	Cleanliness     string            `json:"cleanliness"`
	RequiredAction  string            `json:"requiredAction,omitempty"`
	MIGDevices      []ObservedMIGSlot `json:"migDevices,omitempty"`
	RuntimeBindings []RuntimeBinding  `json:"runtimeBindings,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

type ObservedMIGSlot struct {
	Start   int    `json:"start,omitempty"`
	End     int    `json:"end,omitempty"`
	Profile string `json:"profile"`
	UUID    string `json:"uuid,omitempty"`
}

type RuntimeBinding struct {
	Model           string `json:"model"`
	BatchSize       int    `json:"batchSize,omitempty"`
	Pod             string `json:"pod,omitempty"`
	Phase           string `json:"phase,omitempty"`
	SlotResource    string `json:"slotResource,omitempty"`
	DeviceResource  string `json:"deviceResource,omitempty"`
	ExpectedMIGUUID string `json:"expectedMigUuid,omitempty"`
}

type TargetAllocationPlan struct {
	Planner        string             `json:"planner"`
	Objective      string             `json:"objective"`
	Allocations    []TargetAllocation `json:"allocations"`
	GPULayouts     []TargetGPULayout  `json:"gpuLayouts"`
	DesiredRuntime []ModelRuntimeSpec `json:"desiredRuntimes"`
}

type TargetAllocation struct {
	Model         string     `json:"model"`
	ArrivalRate   float64    `json:"arrivalRate"`
	SLOMs         float64    `json:"sloMs,omitempty"`
	Mu            float64    `json:"mu,omitempty"`
	ServiceTimeMs float64    `json:"serviceTimeMs,omitempty"`
	BatchSize     int        `json:"batchSize"`
	Node          string     `json:"node"`
	GPU           string     `json:"gpu"`
	Profile       string     `json:"profile"`
	Slot          TargetSlot `json:"slot"`
	SlotResource  string     `json:"slotResource"`
	HostPort      int        `json:"hostPort"`
}

type TargetGPULayout struct {
	Node     string       `json:"node"`
	GPU      string       `json:"gpu"`
	Template string       `json:"template"`
	Slots    []TargetSlot `json:"slots"`
}

type TargetSlot struct {
	Start   int    `json:"start"`
	End     int    `json:"end"`
	Profile string `json:"profile"`
}

type AbstractAction struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	Node          string            `json:"node,omitempty"`
	GPU           string            `json:"gpu,omitempty"`
	GPUIndex      int               `json:"gpuIndex,omitempty"`
	Model         string            `json:"model,omitempty"`
	SlotResource  string            `json:"slotResource,omitempty"`
	Runtime       *ModelRuntimeSpec `json:"runtime,omitempty"`
	Slots         []TargetSlot      `json:"slots,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	Postcondition string            `json:"postcondition,omitempty"`
	DependsOn     []string          `json:"dependsOn,omitempty"`
	Effects       map[string]any    `json:"effects,omitempty"`
}

type ActionDAG struct {
	Format string           `json:"format"`
	Nodes  []AbstractAction `json:"nodes"`
}

type ValidationTargets struct {
	TargetAllocationPlan TargetAllocationPlan `json:"targetAllocationPlan"`
	DesiredRuntimes      []ModelRuntimeSpec   `json:"desiredRuntimes"`
}
