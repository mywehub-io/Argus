# Migration guide

## Migrating to v0.3.0

v0.3.0 introduces three breaking changes. Producers that emit node-level events and consumers
that handle them both require updates.

### 1. Plugin event types removed; node event types added

**What changed:**

`TypePluginStarted`, `TypePluginEnded`, and all `OBSERVE.WORKFLOW.PLUGIN.*` subjects are
removed. They are replaced by:

| Old | New |
|---|---|
| `TypePluginStarted` / `OBSERVE.WORKFLOW.PLUGIN.STARTED` | `TypeNodeStarted` / `OBSERVE.WORKFLOW.NODE.STARTED` |
| `TypePluginEnded` / `OBSERVE.WORKFLOW.PLUGIN.ENDED` | `TypeNodeEnded` / `OBSERVE.WORKFLOW.NODE.ENDED` |
| — | `TypeNodeTriggered` / `OBSERVE.WORKFLOW.NODE.TRIGGERED` (new) |

**Producers:** replace all references to the old type constants and update the payload
structs from the old plugin structs to `StartNode` / `EndNode`.

**Consumers (Athena):** update subject subscriptions to `OBSERVE.WORKFLOW.NODE.>` and
switch the switch-case to `event.TypeNodeStarted` / `event.TypeNodeEnded`.

**NATS stream:** the `OBSERVATION` stream's subject filter must include `OBSERVE.WORKFLOW.NODE.>`.
Old messages under `OBSERVE.WORKFLOW.PLUGIN.*` remain in the stream until retention expires;
consumers that have already migrated will simply not receive them.

### 2. `EmitNodeEnd` / `EmitNodeStart` interface rename

**What changed:** the concrete emitter types were renamed for clarity.

| Before | After |
|---|---|
| `NodeEndEmitter` (old interface and struct name) | `NodeEndEmitter` (interface), `ArgusNodeEndEmitter` (concrete) |
| — | `NodeStartEmitter` (interface), `ArgusNodeStartEmitter` (concrete) |

**Update call sites:**

```go
// Before
emitter := pkg.NewNodeEndEmitter(obs, uploader, logger)

// After
emitter := emitter.NewArgusNodeEndEmitter(obs, uploader, logger) // v0.3; from the raw payloads release the second argument is an emitter.DocumentWriter
```

If you typed the variable as the interface, the change is limited to the constructor call.

### 3. `ProjectID` is now required in all node payloads

`StartNode`, `EndNode`, `RunStartedData`, and `RunEndedData` now carry a `ProjectID string`
field, used for multi-tenant isolation.

**Producers:** populate `ProjectID` in `NodeEndEmitParams` and `NodeStartEmitParams`.

---

## Migrating to v0.2.0

v0.2.0 reorganised the NATS subject hierarchy. The changes are limited to subject strings;
the `Event` struct and all payload types are backward-compatible.

| Before | After |
|---|---|
| `OBSERVE.RUN.STARTED` | `OBSERVE.WORKFLOW.RUN.STARTED` |
| `OBSERVE.RUN.ENDED` | `OBSERVE.WORKFLOW.RUN.ENDED` |

Update any hardcoded subject strings to use the constants from `pkg/event/subjects.go`.
Do not hardcode subject strings; always reference the constants to avoid future drift.
