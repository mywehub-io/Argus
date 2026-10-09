# pkg/event

Defines all Argus observation event types, payload structs, validation logic, and the NATS
stream/subject contract. This package is the shared contract between producers (Zeus, Elysium,
Icarus) and consumers (Athena).

## Event taxonomy

Events are organized by three levels of hierarchy.

### Level 1 — Workflow catalog

| Type constant | NATS subject | Payload struct | Required fields |
|---|---|---|---|
| `TypeWorkflowPublished` | `OBSERVE.WORKFLOW.PUBLISHED` | `WorkflowPublish` | `client_id`, `workflow_id` |

`WorkflowPublish.Action` is `"publish"` or `"unpublish"`.

### Level 2 — Run lifecycle

| Type constant | NATS subject | Payload struct | Required fields |
|---|---|---|---|
| `TypeRunStarted` | `OBSERVE.WORKFLOW.RUN.STARTED` | `RunStartedData` or `TriggerWorkflow` | `client_id`, `workflow_id`, `run_id` |
| `TypeRunEnded` | `OBSERVE.WORKFLOW.RUN.ENDED` | `RunEndedData` | `client_id`, `workflow_id`, `run_id` |

**Dual use of `run.started`:** Two different payloads share this event type:
- **Trigger enqueue** (pending): producers emit `run.started` with `TriggerWorkflow` to signal a trigger was accepted.
- **Execution begin** (running): orchestrators emit `run.started` with `RunStartedData` to signal execution started.

Consumers distinguish them by attempting to parse `Data` as `TriggerWorkflow` first (it contains `workflow_id`, `run_id`, `client_id`, `type`, `payload`, timestamps); if parsing fails, fall back to `RunStartedData`.

### Level 3 — Node lifecycle

| Type constant | NATS subject | Payload struct | Required fields |
|---|---|---|---|
| `TypeNodeTriggered` | `OBSERVE.WORKFLOW.NODE.TRIGGERED` | `TriggerNode` | `client_id`, `workflow_id`, `run_id`, `node_id` |
| `TypeNodeStarted` | `OBSERVE.WORKFLOW.NODE.STARTED` | `StartNode` | `client_id`, `workflow_id`, `run_id`, `node_id` |
| `TypeNodeEnded` | `OBSERVE.WORKFLOW.NODE.ENDED` | `EndNode` | `client_id`, `workflow_id`, `run_id`, `node_id` |

Nodes that never ran (e.g. downstream embedded nodes after an upstream failure) must **not**
emit `node.ended` — omit rather than send a synthetic terminal event.

## Event struct

```go
type Event struct {
    ID        string          // Unique ID for JetStream deduplication (auto-generated if empty)
    Type      string          // Event type constant (e.g. "run.started")
    Version   string          // Schema version; always "v1" — consumers must check before parsing Data
    Timestamp time.Time       // When the event occurred

    ClientID   string         // Tenant owning this event (required on every event)
    WorkflowID string         // Workflow identifier (required for Level 2+)
    RunID      string         // Run identifier (required for Level 3)
    NodeID     string         // Node/plugin identifier (required for Level 3)

    Data json.RawMessage      // Type-specific payload; parse using the struct for the matching Type
}
```

(`pkg/event/event.go:41`)

## Payload data structs

### `WorkflowPublish`

```go
type WorkflowPublish struct {
    Action       string // "publish" | "unpublish"
    Status       string // "success" | "failed" | "started"
    StartedAt    int64  // Unix ms
    EndedAt      int64  // Unix ms
    HasError     bool
    ErrorMessage string
}
```

### `RunStartedData`

```go
type RunStartedData struct {
    TotalNodes  int
    ProjectID   string       // For multi-tenant blob path isolation
    TriggerInfo *TriggerInfo // How the run was triggered
}
```

### `RunEndedData`

```go
type RunEndedData struct {
    Status            string   // "completed" | "failed" | "partial"
    ProjectID         string
    TotalNodes        int
    SuccessNodes      int
    FailedNodes       int
    SkippedNodes      int
    QueueLength       int
    CompletedNodeIDs  []string // execution-unit node IDs that completed
    FailedNodeIDs     []string
    SkippedNodeIDs    []string
    TriggerType       string   // e.g. "sync" | "http"
    SyncCorrelationID string   // workflow_id + "-" + run_id for sync triggers
}
```

`SyncCorrelationID` lets Hermes match a `run.ended` event to the caller waiting for a
synchronous response.

### `StartNode`

```go
type StartNode struct {
    WorkflowID string
    RunID      string
    ClientID   string
    ProjectID  string
    NodeID     string
    Label      string   // Human-readable label from the execution plan
    StartedAt  int64    // Unix ms
    Input      *Payload // Resolved input (a reference to its document)
}
```

### `EndNode`

```go
type EndNode struct {
    WorkflowID    string
    RunID         string
    ClientID      string
    NodeID        string
    Label         string
    StartedAt     int64
    EndedAt       int64    // Unix ms
    Output        *Payload // Node output (a reference to its document)
    HasError      bool
    ErrorMessage  string
    ProjectID     string
    ContainsNodes []string // Node IDs in this execution unit (parent + embedded)
    ExecutionID   string
}
```

### `Payload`, `FileRef` and `PortFile`

```go
type Payload struct {
    Document *FileRef   // the node's input or output document, in the run's files
    Files    []PortFile // every file reference inside the document
}

type FileRef struct {
    Path        string
    Size        int64
    ContentType string
    FileName    string
    Records     *int64
}

type PortFile struct {
    Key       string  // JSON pointer of the reference in the document
    Direction string  // "input" or "output"
    File      FileRef
}
```

A payload never carries data inline. `Document` is empty only when the producer could not write
the document.

## NATS contract

All constants are defined in `pkg/event/subjects.go`:

| Constant | Value |
|---|---|
| `StreamName` | `"OBSERVATION"` |
| `SubjectPrefix` | `"OBSERVE"` |
| `SubjectPatternAll` | `"OBSERVE.>"` |

Use `SubjectPatternAll` to consume all observation events from the stream.
Use `SubjectForEventType(evt.Type)` to get the specific subject for a known event type;
returns empty string for unknown types.

## Building an event

```go
evt := event.New(event.TypeNodeEnded).
    WithClient("org_123").
    WithWorkflow("wf_abc").
    WithRun("run_xyz").
    WithNode("node_1").
    WithData(&event.EndNode{
        WorkflowID:   "wf_abc",
        RunID:        "run_xyz",
        ClientID:     "org_123",
        NodeID:       "node_1",
        EndedAt:      time.Now().UnixMilli(),
        HasError:     false,
        Output:       &event.Payload{Document: &event.FileRef{Path: "results/wf_abc/run_xyz/node_1/output.json", Size: 16}},
    })
```

`event.New` auto-generates `ID` (UUID), sets `Version` to `"v1"`, and sets `Timestamp` to
`time.Now()`. (`pkg/event/event.go:64`)

## Consuming events

```go
var evt event.Event
if err := json.Unmarshal(msg.Data, &evt); err != nil { ... }

switch evt.Type {
case event.TypeRunEnded:
    var data event.RunEndedData
    evt.ParseData(&data)
    // use data.Status, data.CompletedNodeIDs, etc.
case event.TypeNodeEnded:
    var data event.EndNode
    evt.ParseData(&data)
}
```

## Validation errors

`Event.Validate()` returns a sentinel error when required fields are missing.

| Sentinel | When returned |
|---|---|
| `ErrMissingEventID` | `ID` is empty |
| `ErrMissingEventType` | `Type` is empty |
| `ErrMissingClientID` | `ClientID` is empty (all events) |
| `ErrMissingWorkflowID` | `WorkflowID` empty for Level 1, 2, or 3 event |
| `ErrMissingRunID` | `RunID` empty for Level 2 or Level 3 event |
| `ErrMissingNodeID` | `NodeID` empty for Level 3 event |

(`pkg/event/errors.go`, `pkg/event/event.go:146`)

## See also

- [pkg/observer](../observer/README.md) — how to emit events
- [pkg/emitter](../emitter/README.md) — payload size threshold and blob upload
- [Argus README](../../README.md) — architecture overview
