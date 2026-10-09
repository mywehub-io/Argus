# pkg/observer

Synchronous, thread-safe wrapper over NATS JetStream for emitting Argus observation events.

## Purpose

`observer` turns a raw NATS JetStream context into a safe, reusable interface for emitting
[Argus events](../event/README.md). By default every `Emit` call publishes synchronously and
returns the actual delivery result, so callers know immediately whether the event reached the
broker. In async mode (`WithAsync`) `Emit` queues the event and returns at once, so a slow
OBSERVATION stream never slows the caller.

## Quick start

```go
import (
    "context"
    "github.com/nats-io/nats.go"
    "github.com/wehubfusion/Argus/pkg/observer"
    "go.uber.org/zap"
)

nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()
logger, _ := zap.NewProduction()

obs, err := observer.NewObserver(js, observer.DefaultOptions(), logger)
if err != nil {
    // handle startup failure
}
defer obs.Close(context.Background())
```

## Observer interface

```go
type Observer interface {
    Emit(ctx context.Context, evt *event.Event) error
    Close(ctx context.Context) error
}
```

### `Emit(ctx, evt)`

Validates the event, serializes it to JSON, and publishes it to the NATS subject that
corresponds to `evt.Type` (via `event.SubjectForEventType`). Returns synchronously with the
delivery result.

Auto-populates `Timestamp` and `Version` if absent. The publish id (`Nats-Msg-Id`) is
`evt.EmitID()`: `evt.DedupID()`, a hash of the type, tenant, workflow, run, node and data, unless
the caller set `ID` itself (the ID `event.New` generates does not count). Publishing the same event
twice is therefore stored once, while a retried unit's `node.ended` with a different outcome is a
different event (`pkg/event/event.go`).

In async mode `Emit` returns `nil` once the event is validated, serialised and queued. A full queue
drops the event; drops and publish failures are counted (`observer.StatsOf`) and logged as
`Argus observation events lost` at most once per `DropLogInterval` (`pkg/observer/async.go`).

Error return values:

| Error | Meaning |
|---|---|
| `nil` | Event delivered to the broker |
| `ErrObserverClosed` | `Close()` was called before this `Emit` |
| wrapped error | Validation, serialization, or NATS publish failure |

(`pkg/observer/observer.go:88`)

### `Close(ctx)`

Marks the observer as closed. Subsequent `Emit` calls return `ErrObserverClosed`. In async mode it
then waits for queued events to be published, until `ctx` ends or, when `ctx` has no deadline,
`CloseFlushTimeout`, and returns the context error if events were left. Safe to call multiple
times — idempotent via `sync.Once`.

## `NewObserver`

```go
func NewObserver(js natsclient.JetStreamContext, opts Options, logger *zap.Logger) (Observer, error)
```

Creates the observer and the underlying NATS stream (create-or-update semantics). Fails if
`js` is nil. Uses `zap.NewProduction()` if `logger` is nil.

**Nil-safe pattern:** when observation is disabled, pass `nil` for the observer and guard
every call site:

```go
if obs != nil {
    obs.Emit(ctx, evt)
}
```

## Options

All fields have defaults; call `DefaultOptions()` to start.

| Field | Default | Description |
|---|---|---|
| `StreamName` | `"OBSERVATION"` | JetStream stream that receives all observation events |
| `StreamMaxAge` | 30 days | How long the stream retains messages |
| `StreamMaxMsgs` | 1 000 000 | Maximum messages stored in the stream |
| `PublishTimeout` | 5 s | Context deadline added to each NATS publish call |
| `AsyncQueueSize` | 0 (sync) | Above 0, async mode with a queue of this many events (`WithAsync`) |
| `AsyncWorkers` | 4 | Goroutines publishing queued events. Events can reach the stream out of order |
| `DropLogInterval` | 30 s | Least time between two "events lost" log lines |
| `CloseFlushTimeout` | 5 s | How long `Close` waits for the queue when `ctx` has no deadline |

(`pkg/observer/options.go`)

Fluent builder:

```go
opts := observer.DefaultOptions().
    WithStreamName("MY_STREAM").
    WithStreamMaxAge(7 * 24 * time.Hour).
    WithPublishTimeout(10 * time.Second)
```

## Thread safety

`Observer` is safe for concurrent use from multiple goroutines. The `isClosed` flag is
guarded by a `sync.RWMutex`; the `closeOnce` ensures the close action runs exactly once.
(`pkg/observer/observer.go:32-39`)

## Observability

Every successful publish logs at INFO:

```
Argus observation event published
  event_type=<type>  workflow_id=<id>  run_id=<id>  node_id=<id>  dedupe_msg_id=<id>
```

Use `dedupe_msg_id` to correlate emission order with Athena consumption and detect races.

## Error sentinel

```go
var ErrObserverClosed = errors.New("observer is closed")
```

(`pkg/observer/errors.go`)

## See also

- [pkg/event](../event/README.md) — event types, payload structs, NATS subjects
- [pkg/emitter](../emitter/README.md) — payload size threshold and blob upload helpers
- [Argus README](../../README.md) — quick-start and architecture overview
