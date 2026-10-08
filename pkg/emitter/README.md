# pkg/emitter

High-level emitters for Argus node lifecycle events.

## Purpose

`emitter` sits between your business logic and `pkg/observer`. It writes a node's input or output
as a document in the run's own files, sends the document's reference, and wraps the full
emit-a-node-event flow into two concrete types: `ArgusNodeEndEmitter` and
`ArgusNodeStartEmitter`. An event never carries the payload itself: monitoring reads the document
from the run's files.

## `DocumentWriter`

```go
type DocumentWriter interface {
    WriteDocument(ctx context.Context, target DocumentTarget, data []byte) (event.FileRef, error)
}

type DocumentTarget struct {
    WorkflowID string
    RunID      string
    NodeID     string
    Direction  string // DirectionInput or DirectionOutput
}
```

The producer implements `DocumentWriter` over its own blob store and returns the reference of the
file it wrote (Elysium writes `results/{workflow}/{run}/{node}/input.json` and `output.json`).
(`pkg/emitter/documents.go`)

## `ScanFiles`

```go
func ScanFiles(data []byte, direction string) []event.PortFile
```

Returns every file reference (`{"$file":{...}}`, alone or as an item of a files list) in a JSON
document, keyed by its JSON pointer. The emitters attach the result to the payload as `Files`, so
a consumer can list a node's files without reading the document.

## `ArgusNodeEndEmitter`

```go
func NewArgusNodeEndEmitter(obs observer.Observer, documents DocumentWriter, logger *zap.Logger) *ArgusNodeEndEmitter
```

Marshals the output, writes it as the node's output document, builds the `node.ended` event and
calls `observer.Emit`.

```go
type NodeEndEmitParams struct {
    ClientID      string
    ProjectID     string
    WorkflowID    string
    RunID         string
    NodeID        string
    Label         string
    Output        interface{}   // marshaled to JSON
    HasError      bool
    ErrorMessage  string
    ContainsNodes []string
}
```

**Best effort:** if the document cannot be written the event is still sent, with an empty
`Payload`, so the end of the node is not lost to monitoring. A nil receiver, a nil observer or a
nil writer is a no-op.

**Required fields:** `ClientID`, `WorkflowID`, `RunID`, `NodeID` must all be non-empty. Empty
required fields skip emission silently (DEBUG log).

## `ArgusNodeStartEmitter`

```go
func NewArgusNodeStartEmitter(obs observer.Observer, documents DocumentWriter, logger *zap.Logger) *ArgusNodeStartEmitter
```

```go
type NodeStartEmitParams struct {
    ClientID   string
    ProjectID  string
    WorkflowID string
    RunID      string
    NodeID     string
    Label      string
    Input      []byte  // empty → no-op
}
```

Writes the resolved input as the node's input document and emits a `node.started` event. Empty
`Label` defaults to `NodeID`.

## Typical usage

```go
nodeEndEmitter := emitter.NewArgusNodeEndEmitter(obs, myDocumentWriter, logger)

err := nodeEndEmitter.EmitNodeEnd(ctx, emitter.NodeEndEmitParams{
    ClientID:   "org_123",
    ProjectID:  "proj_abc",
    WorkflowID: "wf_xyz",
    RunID:      "run_1",
    NodeID:     "node_2",
    Label:      "Transform Data",
    Output:     myOutputStruct,
    HasError:   false,
})
// error is best-effort; log and continue
```

## See also

- [pkg/event](../event/README.md) — `Payload`, `FileRef`, event type constants
- [pkg/observer](../observer/README.md) — how to emit events over NATS
- [Argus README](../../README.md) — architecture overview
