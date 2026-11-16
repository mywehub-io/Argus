# Changelog

All notable changes to the Argus SDK are documented here.

## Tagging convention

Each entry is tagged `` `public:` `` or `` `internal:` ``:

- `` `public:` `` — customer- or developer-visible change: new API surface, breaking
  removals, observable behaviour changes. Include in external release notes.
- `` `internal:` `` — infrastructure or implementation detail: internal telemetry,
  private refactors, dependency bumps. Omit from external release notes.

## [Unreleased]

### Added: verify-only observer

- `public:` `observer.Options.VerifyOnly` (`WithVerifyOnly`): the observer only looks the
  observation stream up. It never creates it, never changes its size cap, and does not recreate it
  when a publish finds it gone. `NewObserver` succeeds while the stream is missing; events are
  refused until the platform creates it. Off by default, so existing callers are unchanged. For
  customer runners under NATS operator mode, which have no right to create or update streams.

### Document lane: a full lane respects the caller's context

- `public:` When the lane is full and a submission runs on the caller's goroutine, its waits for a
  write slot and for its run's previous event now end with the caller's context. The emitter then
  returns the context's error at once, writes nothing and publishes nothing: that event is lost to
  monitoring, never published out of order. The run's later events still wait for the earlier
  ones. Queued submissions are unaffected (their context never ends).
- `public:` `LaneHooks.Abandoned(err)` reports such a submission.

### Document lane: node documents written off the unit's path

- `public:` `emitter.NewLane(opts, logger)` and `emitter.WithLane(ctx, lane)`. With a lane on the
  context, `EmitNodeStart` and `EmitNodeEnd` return at once; the lane writes the node's document
  (up to `Concurrency` at a time) and publishes the event once the document is written and the
  run's earlier events are published. Runs are ordered independently, so one slow write never
  holds back another run. Without a lane on the context the emitters behave as before.
- `public:` Events carry the time they were submitted: `StartedAt`, `EndedAt` and the event
  timestamp are stamped when the emitter is called, not when the event is published.
- `public:` A lane holds at most `MaxQueuedBytes` (64 MiB) and `MaxQueuedJobs` (16,384
  documents, whatever their size, so tiny or lazily built documents cannot pile up as goroutines);
  past either a submission runs on the caller's goroutine, after its run's earlier events. Each write is bounded by `WriteTimeout`
  (30 s), and a failed write still publishes the event without the document. `Drain(ctx)` waits
  for everything submitted; documents still queued when a process is killed are lost.
  `LaneHooks` report queued, inline, written and published for metrics.
- `public:` `Lane.Submit(ctx, emitter.Document{...})` is the same path for a caller that builds
  its own event (a connector step). `Document.Build` produces the document on the lane, so one
  that takes a read to assemble costs the caller nothing; a document built lazily does not count
  towards `MaxQueuedBytes`.

## [0.5.0] — 2026-10-09

v0.6.0 and v0.7.0 were tagged without changelog entries.

### Breaking: payloads are references (raw payloads hard cut)

- `public:` `event.Payload` carries `Document` and `Files` only. `InlineData`, `BlobReference`
  (field and type), `BlobRef`, `PayloadInfo` and `EndNode.ConsumerInputs` are removed. A consumer
  that reads events published before the release has to decode the old fields itself.
- `public:` `pkg/emitter`: `PreparePayload`, `PayloadOptions`, `DefaultMaxInlineBytes`,
  `PathContext`, `BuildMonitoringPath` and `BuildMonitoringPathWithSuffix`, `BlobUploader`,
  `AzureBlobUploader`, the CSV and XLSX produce blob special case and `WithDocuments` are removed.
  `NewArgusNodeEndEmitter` and `NewArgusNodeStartEmitter` take an `emitter.DocumentWriter` as
  their second argument and write the node's document through it. If the document cannot be
  written the event is still sent with an empty payload.
- `internal:` The module no longer depends on the Azure storage SDK.

## [0.4.1] — 2026-05-10

### Added

- `public:` `RunEndedData` gains optional `SyncCompletionProfile string`
  (`json:"sync_completion_profile,omitempty"`). Wire values: `HTTP_RESPONSE`,
  `NODE_IO_DETAIL`, or empty (async / legacy). Existing consumers do not need to
  be updated. (`pkg/event/event.go`)

## [0.4.0] — 2026-05-03

### Added

- `public:` **CSV/XLSX produce artifact support in `EmitNodeEnd`**: when a node output
  matches the Elysium produce envelope (`action: "produce"`, `fileExtension: ".csv"` or
  `".xlsx"`, base64 `encoded` field), the raw decoded bytes are uploaded directly to a
  `.csv` or `.xlsx` blob path. Monitoring downloads are now native files rather than
  JSON wrappers. (`pkg/emitter/emitter.go`, `pkg/emitter/producer_blob.go`)

### Changed

- `internal:` `BuildMonitoringPath` now delegates to
  `BuildMonitoringPathWithSuffix(pathCtx, ".json")`. The new
  `BuildMonitoringPathWithSuffix` accepts an explicit extension (`.json`, `.csv`, or
  `.xlsx`). Extensions outside that set fall back to `.json`.

## [0.3.1] — 2026-03-31

### Added

- `internal:` `Observer.Emit` now logs a structured INFO message on every successful
  publish: `event_type`, `workflow_id`, `run_id`, `node_id`, `dedupe_msg_id`. Use
  `dedupe_msg_id` to correlate emission order with Athena consumption.

### Changed

- `public:` `RunEndedData` gains five optional fields: `CompletedNodeIDs []string`,
  `FailedNodeIDs []string`, `SkippedNodeIDs []string`, `TriggerType string`,
  `SyncCorrelationID string`. Existing consumers do not need to be updated; omitting
  these fields in existing `run.ended` events is valid. (`pkg/event/event.go`)

### Fixed

- `internal:` NATS publisher error handling: wrapped errors now preserve the original
  cause through the call stack. (`72f7746`)

## [0.3.0] — 2026-03-24

**Breaking changes.** See [docs/migration-guide.md](docs/migration-guide.md).

### Added

- `public:` `pkg/emitter` package: `PreparePayload`, `PathContext`, `PayloadOptions`,
  `BlobUploader` interface, `AzureBlobUploader`, `ArgusNodeEndEmitter`,
  `ArgusNodeStartEmitter`.
- `public:` `TypeNodeTriggered` (`OBSERVE.WORKFLOW.NODE.TRIGGERED`) event type and
  subject.
- `public:` `TypeNodeStarted` (`OBSERVE.WORKFLOW.NODE.STARTED`) event type and subject.
- `public:` `TypeNodeEnded` (`OBSERVE.WORKFLOW.NODE.ENDED`) event type and subject.
- `public:` `StartNode` payload struct with optional `Label` field for human-readable
  node names.
- `public:` `EndNode` gains `ConsumerInputs map[string]*Payload`, `ExecutionID string`,
  `ContainsNodes []string`.
- `public:` `Payload.BlobReference` (`BlobReference{URL, Size}`) for payloads above the
  500 KB threshold.
- `public:` `ProjectID string` field on `RunStartedData`, `RunEndedData`, `StartNode`,
  `EndNode` for multi-tenant blob path isolation.

### Removed (breaking)

- `public:` Legacy plugin event types (`TypePluginStarted`, `TypePluginEnded`, and
  related subjects `OBSERVE.WORKFLOW.PLUGIN.*`) — removed in favour of the new `node.*`
  event hierarchy.
- `public:` `HealthStatus` field from `WorkflowPublishedData`.

### Changed (breaking)

- `public:` `NodeEndEmitter` interface renamed to `NodeEndEmitter`; `NodeStartEmitter`
  added. The concrete types are now `ArgusNodeEndEmitter` / `ArgusNodeStartEmitter`.

## [0.2.0] — 2026-02-16

### Changed (breaking)

- `public:` NATS subject hierarchy reorganised: all subjects now live under
  `OBSERVE.WORKFLOW.*` (previously a mix of `OBSERVE.*` and `OBSERVE.WORKFLOW.*`).
- `internal:` `pkg/event/subjects.go` is now the single source of truth for all subject
  constants. `internal/nats/subjects.go` was deleted.
- `public:` Event data structures revised; `WorkflowPublish.Action` is now `"publish"`
  or `"unpublish"` (previously different values).

## [0.1.0] — 2026-01-22

Initial release.

- `public:` `pkg/event`: `Event` struct, `TypeWorkflowPublished`, `TypeRunStarted`,
  `TypeRunEnded` constants, `WorkflowPublish`, `RunStartedData`, `RunEndedData`
  payloads.
- `public:` `pkg/observer`: `Observer` interface, `NewObserver`, `Options`,
  `DefaultOptions()`, `ErrObserverClosed`.
- `internal:` `pkg/emitter`: not yet present.
- `public:` `tests/`: `MockJetStream` and observer/event test suite.
