package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Event type constants
const (
	TypeWorkflowPublished = "workflow.published"
	TypeWorkflowStarted   = "workflow.started"
	TypeRunStarted        = "run.started"
	TypeRunEnded          = "run.ended"
	// TypeNodeTriggered is a Level 3 node lifecycle event emitted when an
	// execution unit (and its embedded nodes) is dispatched by an orchestrator
	// such as Zeus. Payload: TriggerNode.
	TypeNodeTriggered = "node.triggered"
	// TypeNodeStarted is a Level 3 node lifecycle event emitted when a node
	// begins execution on a worker. Payload: StartNode.
	TypeNodeStarted = "node.started"
	// TypeNodeEnded is a Level 3 node lifecycle event emitted when a node
	// finishes execution on a worker. Payload: EndNode.
	TypeNodeEnded = "node.ended"
)

// Event is the universal observation event structure.
// Generic and service-agnostic, works for any producer service.
//
// Design Philosophy:
// - Producers emit minimal events: Only IDs (workflow_id, run_id, node_id) and execution data
// - Consumers can enrich events by fetching metadata (names, tags, icons) from catalog services
// - Data field: Type-specific payload as json.RawMessage for flexibility
//
// Field Requirements by Level:
// - Level 1 (Workflow Catalog): Requires client_id, workflow_id (no run_id)
// - Level 2 (Run Lifecycle): Requires client_id, workflow_id, run_id
// - Level 3 (Node Lifecycle): Requires client_id, workflow_id, run_id, node_id
type Event struct {
	// === Routing & Identity ===
	ID        string    `json:"id"`        // Unique ID (for JetStream deduplication)
	Type      string    `json:"type"`      // Event type (e.g., "run.started", "node.ended")
	Version   string    `json:"v"`         // Schema version; consumers should check before parsing Data and skip or log unknown versions
	Timestamp time.Time `json:"timestamp"` // When event occurred

	// === Context (correlation & multi-tenancy) ===
	ClientID   string `json:"client_id"`
	WorkflowID string `json:"workflow_id,omitempty"`
	RunID      string `json:"run_id,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	EnvironmentID string `json:"environment_id,omitempty"` // Tenant environment (empty = default)

	// === Event-Specific Data ===
	// Raw JSON - consumer parses based on Type
	Data json.RawMessage `json:"data,omitempty"`

	// marshalErr captures a json.Marshal failure from WithData. It is private and
	// intentionally excluded from JSON serialisation (json:"-") so a partially
	// constructed event does not reach the NATS publisher with a silently missing
	// Data field. Validate() returns the marshalErr before any field checks so
	// callers always see the root cause rather than a misleading "missing data"
	// error. Consumers never see this field; it is a build-time guard for producers.
	marshalErr error `json:"-"`

	// newID is the ID New generated. While ID still equals it, EmitID derives the ID from the
	// event's content instead.
	newID string `json:"-"`
}

// New creates a new event with a generated ID and timestamp. The generated ID is provisional:
// the observer replaces it with DedupID when it emits the event, so publishing the same event
// twice is stored once (JetStream Nats-Msg-Id dedup). An ID the caller assigns is kept.
func New(eventType string) *Event {
	id := uuid.New().String()
	return &Event{
		ID:        id,
		newID:     id,
		Type:      eventType,
		Version:   "v1",
		Timestamp: time.Now(),
	}
}

// === Fluent Builder Methods ===

// WithClient sets the client_id field.
func (e *Event) WithClient(clientID string) *Event {
	e.ClientID = clientID
	return e
}

// WithWorkflow sets the workflow_id field.
func (e *Event) WithWorkflow(workflowID string) *Event {
	e.WorkflowID = workflowID
	return e
}

// WithRun sets the run_id field.
func (e *Event) WithRun(runID string) *Event {
	e.RunID = runID
	return e
}

// WithNode sets the node_id field.
func (e *Event) WithNode(nodeID string) *Event {
	e.NodeID = nodeID
	return e
}

// WithEnvironment sets the tenant environment_id on the envelope (used for observation subject suffixing).
func (e *Event) WithEnvironment(environmentID string) *Event {
	e.EnvironmentID = environmentID
	return e
}

// WithData sets the data field from any struct/map.
// Marshals to JSON internally.
func (e *Event) WithData(data any) *Event {
	if data == nil {
		e.Data = nil
		e.marshalErr = nil
		return e
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		e.Data = nil
		e.marshalErr = fmt.Errorf("event data marshal error: %w", err)
		return e
	}
	e.Data = bytes
	e.marshalErr = nil
	return e
}

// WithRawData sets data directly from raw JSON bytes.
func (e *Event) WithRawData(data json.RawMessage) *Event {
	e.Data = data
	return e
}

// === Serialization ===

// Bytes serializes the event to JSON.
func (e *Event) Bytes() ([]byte, error) {
	return json.Marshal(e)
}

// ParseData unmarshals the Data field into the provided struct.
func (e *Event) ParseData(dest any) error {
	if e.Data == nil {
		return nil
	}
	return json.Unmarshal(e.Data, dest)
}

// DedupID is the event's publish-time deduplication id: a hash of its type, tenant, workflow,
// run, node and data. Republishing the same event (a producer retry) gives the same id, and
// JetStream stores it once. Events that differ in any of these, for example a node.ended after a
// retried unit with a different outcome, get different ids. The timestamp is left out on purpose:
// a retry stamps a new one.
func (e *Event) DedupID() string {
	h := sha256.New()
	for _, part := range []string{e.Type, e.EnvironmentID, e.ClientID, e.WorkflowID, e.RunID, e.NodeID} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	h.Write(e.Data)
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// EmitID returns the id to publish the event under: DedupID when the ID is empty or still the
// one New generated, otherwise the ID the caller set.
func (e *Event) EmitID() string {
	if e.ID == "" || e.ID == e.newID {
		return e.DedupID()
	}
	return e.ID
}

// === Validation ===

// Validate checks if the event has required fields.
// Validation is level-aware:
// - Level 1 (Workflow Catalog): Only requires client_id and workflow_id (no run_id)
// - Level 2 (Run Lifecycle): Requires client_id, workflow_id, and run_id
// - Level 3 (Node Lifecycle): Requires client_id, workflow_id, run_id, and node_id
func (e *Event) Validate() error {
	if e.ID == "" {
		return ErrMissingEventID
	}
	if e.Type == "" {
		return ErrMissingEventType
	}
	if e.ClientID == "" {
		return ErrMissingClientID
	}
	if e.marshalErr != nil {
		return e.marshalErr
	}

	// Level-based validation
	switch e.Type {
	case TypeWorkflowPublished:
		if e.WorkflowID == "" {
			return ErrMissingWorkflowID
		}

	case TypeWorkflowStarted, TypeRunStarted, TypeRunEnded:
		if e.WorkflowID == "" {
			return ErrMissingWorkflowID
		}
		if e.RunID == "" {
			return ErrMissingRunID
		}

	case TypeNodeTriggered, TypeNodeStarted, TypeNodeEnded:
		if e.WorkflowID == "" {
			return ErrMissingWorkflowID
		}
		if e.RunID == "" {
			return ErrMissingRunID
		}
		if e.NodeID == "" {
			return ErrMissingNodeID
		}
	}

	return nil
}

// === Data Schema Structs ===
// These are documented structs for Data payloads.
// Event producers use these to create type-safe data.
// Event consumers use these to parse data.

// WorkflowPublish is the data payload for workflow.published events.
// client_id and workflow_id are on the event envelope; timestamp is event.Timestamp.
type WorkflowPublish struct {
	Action       string `json:"action"` // "publish" | "unpublish"
	Status       string `json:"status"` // "success" | "failed" | "started"
	StartedAt    int64  `json:"started_at"`
	EndedAt      int64  `json:"ended_at"`
	HasError     bool   `json:"has_error"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type TriggerWorkflow struct {
	WorkflowID   string   `json:"workflow_id"`
	RunID        string   `json:"run_id"`
	ClientID     string   `json:"client_id"`
	ProjectID     string   `json:"project_id,omitempty"`     // For blob path and multi-tenant isolation
	EnvironmentID string   `json:"environment_id,omitempty"` // Tenant environment (empty = default)
	Type          string   `json:"type"`
	Payload      *Payload `json:"payload"`
	StartedAt    int64    `json:"started_at"`
	EndedAt      int64    `json:"ended_at"`
	HasError     bool     `json:"has_error"`
	ErrorMessage string   `json:"error_message,omitempty"`
}

// TriggerInfo provides context about what initiated a run (e.g., http/manual) and payload details.
type TriggerInfo struct {
	Type     string `json:"type"`               // "http" | "manual" | "kafka" | ...
	HasData  bool   `json:"has_data"`           // Whether trigger had any payload
	DataSize int    `json:"data_size"`          // Inline payload size (bytes) when applicable
	BlobURL  string `json:"blob_url,omitempty"` // Blob URL if payload was stored externally
}

// RunStartedData is the data payload for run.started events emitted when execution begins.
// It is distinct from TriggerWorkflow (also sent with run.started) which represents trigger enqueueing.
type RunStartedData struct {
	TotalNodes  int          `json:"total_nodes"`
	ProjectID     string       `json:"project_id,omitempty"`     // For blob path and multi-tenant isolation
	EnvironmentID string       `json:"environment_id,omitempty"` // Tenant environment (empty = default)
	TriggerInfo   *TriggerInfo `json:"trigger_info,omitempty"`
}

// RunEndedData is the data payload for run.ended events.
type RunEndedData struct {
	Status       string `json:"status"`               // "completed" | "failed" | "partial" | ...
	ProjectID     string `json:"project_id,omitempty"`     // For multi-tenant isolation
	EnvironmentID string `json:"environment_id,omitempty"` // Tenant environment (empty = default)
	TotalNodes    int    `json:"total_nodes"`
	SuccessNodes int    `json:"success_nodes"`
	FailedNodes  int    `json:"failed_nodes"`
	SkippedNodes int    `json:"skipped_nodes"`
	QueueLength  int    `json:"queue_length"`

	// CompletedNodeIDs, FailedNodeIDs, SkippedNodeIDs list execution-unit node IDs only
	// (same notion as orchestrator state lists filtered to top-level execution units).
	// Observation consumers can reconcile materialized node rows against this manifest.
	CompletedNodeIDs []string `json:"completed_node_ids,omitempty"`
	FailedNodeIDs    []string `json:"failed_node_ids,omitempty"`
	SkippedNodeIDs   []string `json:"skipped_node_ids,omitempty"`

	// TriggerType is the trigger channel from workflow metadata (e.g. "sync", "http").
	TriggerType string `json:"trigger_type,omitempty"`
	// SyncCorrelationID is workflow_id + "-" + run_id for synchronous triggers; Hermes registers
	// the same value. Optional for non-sync runs.
	SyncCorrelationID string `json:"sync_correlation_id,omitempty"`
	// SyncCompletionProfile distinguishes sync completion paths: "HTTP_RESPONSE", "NODE_IO_DETAIL",
	// or empty for async / legacy. Optional; consumers must treat unknown values as empty.
	SyncCompletionProfile string `json:"sync_completion_profile,omitempty"`
}

type StartWorkflow struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	ClientID   string `json:"client_id"`
	StartedAt  int64  `json:"started_at"`
}

type EndWorkflow struct {
	WorkflowID   string `json:"workflow_id"`
	RunID        string `json:"run_id"`
	ClientID     string `json:"client_id"`
	EndedAt      int64  `json:"ended_at"`
	HasError     bool   `json:"has_error"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// TriggerNode is the data payload for node.triggered events.
// It captures when an execution unit (and optionally its embedded nodes)
// is dispatched by an orchestrator. The identifying fields
// (client_id, workflow_id, run_id, node_id) are also present on the
// Event envelope; they are duplicated here for convenience for consumers.
type TriggerNode struct {
	WorkflowID string   `json:"workflow_id"`
	RunID      string   `json:"run_id"`
	ClientID   string   `json:"client_id"`
	ProjectID     string   `json:"project_id,omitempty"`     // For blob path and multi-tenant isolation
	NodeID        string   `json:"node_id"`
	Label         string   `json:"label,omitempty"` // Human-readable node label from execution plan (e.g. "ESR", "Append to User.csv")
	Type          string   `json:"type"`
	Payload    *Payload `json:"payload"`
	StartedAt  int64    `json:"started_at"`
}

type StartNode struct {
	WorkflowID string   `json:"workflow_id"`
	RunID      string   `json:"run_id"`
	ClientID   string   `json:"client_id"`
	ProjectID     string   `json:"project_id,omitempty"`     // For blob path and multi-tenant isolation
	NodeID        string   `json:"node_id"`
	Label         string   `json:"label,omitempty"` // Human-readable node label from execution plan
	StartedAt     int64    `json:"started_at"`
	Input         *Payload `json:"input"`
}
type EndNode struct {
	WorkflowID   string   `json:"workflow_id"`
	RunID        string   `json:"run_id"`
	ClientID     string   `json:"client_id"`
	NodeID       string   `json:"node_id"`
	Label        string   `json:"label,omitempty"`      // Human-readable node label from execution plan (e.g. "ESR", "Auth Rule")
	StartedAt    int64    `json:"started_at,omitempty"` // Unix ms when node execution started (optional; derived from result timestamp − duration when available)
	EndedAt      int64    `json:"ended_at"`
	Output       *Payload `json:"output"`
	HasError     bool     `json:"has_error"`
	ErrorMessage string   `json:"error_message,omitempty"`

	// Extended payload fields (optional; used by Athena payload processor)
	ProjectID      string              `json:"project_id,omitempty"`      // For blob path and multi-tenant isolation
	ContainsNodes  []string            `json:"contains_nodes,omitempty"`  // Node IDs whose outputs are in this unit result (parent + embedded)
	ExecutionID    string              `json:"execution_id,omitempty"`    // Execution ID of the unit that produced this result
}

type Payload struct {
	// Document is the node's input or output document in the run's files
	// (results/{wf}/{run}/{nodeId}.input.json or .output.json), written once by the producer; monitoring reads it there
	// (raw payloads D9). Empty when the document could not be written.
	Document *FileRef `json:"document,omitempty"`
	// Files lists every file reference inside the document, with its port key and direction.
	Files []PortFile `json:"files,omitempty"`
}

// FileRef is a file in a run's files: the "$file" value raw payloads pass between nodes
// (Icarus pkg/fileref). Path is relative to the results container.
type FileRef struct {
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType,omitempty"`
	FileName    string `json:"fileName,omitempty"`
	Records     *int64 `json:"records,omitempty"`
}

// PortFile is one file reference found in a node's input or output document.
type PortFile struct {
	// Key is where the reference sits in the document, as a JSON pointer ("/payload", "/files/0").
	Key string `json:"key"`
	// Direction is "input" or "output".
	Direction string  `json:"direction"`
	File      FileRef `json:"file"`
}

