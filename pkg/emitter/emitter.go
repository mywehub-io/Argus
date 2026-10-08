package emitter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wehubfusion/Argus/pkg/event"
	"github.com/wehubfusion/Argus/pkg/observer"
	"go.uber.org/zap"
)

// PathContext describes where a payload belongs in monitoring blob storage.
// Used to build deterministic blob paths for node output and input events.
type PathContext struct {
	ClientID   string
	ProjectID  string
	WorkflowID string
	RunID      string
	NodeID     string
	// IsInput indicates whether this payload is a node input (true) or output (false).
	IsInput bool
}

// BlobUploader uploads bytes to blob storage and returns the resulting URL.
// Use NewAzureBlobUploader for Azure Blob Storage, or inject a custom implementation
// (e.g. an adapter for Icarus storage.BlobStorageClient).
type BlobUploader interface {
	Upload(ctx context.Context, path string, data []byte, metadata map[string]string) (url string, size int64, err error)
}

// DefaultMaxInlineBytes is the payload size threshold (500 KB) above which
// PreparePayload offloads data to Azure Blob Storage and returns a BlobReference
// instead of inline bytes.
//
// Cross-package sync requirement: this constant MUST stay in sync with:
//   - Icarus pkg/resolver: its inline threshold for field-mapping blob downloads.
//   - Athena: its inline threshold for storing node payloads in Elasticsearch.
//
// If you change this value, update ALL THREE locations in the same PR and add a
// test that verifies the values are equal. A mismatch causes Athena to store
// BlobReferences when Argus emitted inline data (or vice versa), producing
// broken payload drill-down in the UI.
const DefaultMaxInlineBytes = 512000

// PayloadOptions configures PreparePayload behavior.
type PayloadOptions struct {
	// MaxInlineBytes is the size threshold in bytes. Payloads larger than this
	// are uploaded to blob storage. Defaults to DefaultMaxInlineBytes when zero.
	MaxInlineBytes int
}

// PreparePayload applies the size threshold and returns either inline data or
// a blob reference for node output and input events. If data exceeds MaxInlineBytes
// and uploader is provided, it uploads to the monitoring path and returns a
// BlobReference. Otherwise returns InlineData.
//
// Use for both lifecycle payload directions; set PathContext.IsInput
// accordingly for correct blob path suffixes.
//
// Returns nil, nil when data is empty. Returns an error only when upload fails;
// callers may fall back to inline with logging.
func PreparePayload(ctx context.Context, data []byte, pathCtx PathContext, uploader BlobUploader, opts *PayloadOptions) (*event.Payload, error) {
	if len(data) == 0 {
		return nil, nil
	}

	maxInline := DefaultMaxInlineBytes
	if opts != nil && opts.MaxInlineBytes > 0 {
		maxInline = opts.MaxInlineBytes
	}

	// Below threshold or no uploader: return inline
	if len(data) <= maxInline || uploader == nil {
		return &event.Payload{
			InlineData:    data,
			BlobReference: nil,
		}, nil
	}

	blobPath := BuildMonitoringPath(pathCtx)
	if blobPath == "" {
		return &event.Payload{
			InlineData:    data,
			BlobReference: nil,
		}, fmt.Errorf("emitter: missing identifiers for blob path (client_id/project_id/workflow_id/run_id/node_id)")
	}

	metadata := map[string]string{
		"client_id":   pathCtx.ClientID,
		"project_id":  pathCtx.ProjectID,
		"workflow_id": pathCtx.WorkflowID,
		"run_id":      pathCtx.RunID,
		"node_id":     pathCtx.NodeID,
		"direction":   "output",
	}
	if pathCtx.IsInput {
		metadata["direction"] = "input"
	}

	url, size, err := uploader.Upload(ctx, blobPath, data, metadata)
	if err != nil {
		return &event.Payload{
			InlineData:    data,
			BlobReference: nil,
		}, fmt.Errorf("emitter: failed to upload monitoring blob: %w", err)
	}

	return &event.Payload{
		InlineData: nil,
		BlobReference: &event.BlobReference{
			URL:  url,
			Size: size,
		},
	}, nil
}

// BuildMonitoringPath returns the blob path for monitoring payloads.
// Mirrors Athena's path convention. Returns empty string if required
// identifiers are missing. For output: {node_id}.json; for input: {node_id}-input.json.
func BuildMonitoringPath(pathCtx PathContext) string {
	return BuildMonitoringPathWithSuffix(pathCtx, ".json")
}

// NodeEndEmitter emits node.ended observation events with output payload.
// Implementations are best-effort and must not panic.
type NodeEndEmitter interface {
	EmitNodeEnd(ctx context.Context, params NodeEndEmitParams) error
}

// NodeEndEmitParams contains all data needed to emit terminal node state.
type NodeEndEmitParams struct {
	ClientID      string
	ProjectID     string
	WorkflowID    string
	RunID         string
	NodeID        string
	Label         string
	Output        interface{}
	HasError      bool
	ErrorMessage  string
	ContainsNodes []string
}

// ArgusNodeEndEmitter implements NodeEndEmitter using Argus observer.
type ArgusNodeEndEmitter struct {
	observer  observer.Observer
	uploader  BlobUploader
	logger    *zap.Logger
	documents DocumentWriter
}

// WithDocuments makes the emitter write each output as the node's output document and send its
// reference (raw payloads D9) instead of uploading a monitoring copy. A failed write falls back
// to the inline or uploaded path. Returns e.
func (e *ArgusNodeEndEmitter) WithDocuments(w DocumentWriter) *ArgusNodeEndEmitter {
	e.documents = w
	return e
}

// NewArgusNodeEndEmitter creates an emitter. observer and uploader may be nil; emission will no-op or fall back to inline.
func NewArgusNodeEndEmitter(
	obs observer.Observer,
	uploader BlobUploader,
	logger *zap.Logger,
) *ArgusNodeEndEmitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ArgusNodeEndEmitter{
		observer: obs,
		uploader: uploader,
		logger:   logger,
	}
}

// EmitNodeEnd emits a node.ended event with output payload. Best-effort; logs errors, never panics.
//
// When the marshaled output matches the CSV/XLSX produce envelope (JSON object with
// action "produce", fileExtension ".csv" or ".xlsx", and base64 "encoded" that passes
// sanity checks), the payload is always uploaded to blob as raw bytes at
// monitoring/.../{node_id}.csv or .xlsx — never inline — so monitoring downloads are
// native files. If the uploader is nil or upload fails, falls back to PreparePayload
// (size threshold + .json path) like other outputs.
func (e *ArgusNodeEndEmitter) EmitNodeEnd(ctx context.Context, params NodeEndEmitParams) error {
	if e == nil || e.observer == nil {
		return nil
	}
	if params.ClientID == "" || params.WorkflowID == "" || params.RunID == "" || params.NodeID == "" {
		e.logger.Debug("skipping node.ended emit due to missing context",
			zap.String("workflow_id", params.WorkflowID),
			zap.String("run_id", params.RunID),
			zap.String("node_id", params.NodeID),
		)
		return nil
	}

	var payload *event.Payload
	// Marshal output as-is (no label wrapping)
	jsonBytes, err := json.Marshal(params.Output)
	if err != nil {
		e.logger.Error("failed to marshal node output for observation",
			zap.String("node_id", params.NodeID),
			zap.Error(err),
		)
		return err
	}

	pathCtx := PathContext{
		ClientID:   params.ClientID,
		ProjectID:  params.ProjectID,
		WorkflowID: params.WorkflowID,
		RunID:      params.RunID,
		NodeID:     params.NodeID,
		IsInput:    false,
	}

	if e.documents != nil {
		doc, err := documentPayload(ctx, e.documents, DocumentTarget{WorkflowID: params.WorkflowID, RunID: params.RunID,
			NodeID: params.NodeID, Direction: DirectionOutput}, jsonBytes)
		if err == nil {
			payload = doc
		} else {
			e.logger.Warn("node output document not written, sending the output the old way",
				zap.String("node_id", params.NodeID), zap.Error(err))
		}
	}

	rawProduce, dotExt, isProduceFile := produceRawArtifactFromOutputJSON(jsonBytes)

	var prepErr error
	switch {
	case payload != nil:
		// Sent as its document.
	case isProduceFile && e.uploader != nil:
		blobPath := BuildMonitoringPathWithSuffix(pathCtx, dotExt)
		if blobPath == "" {
			e.logger.Warn("produce file blob path empty (missing ids), using PreparePayload",
				zap.String("node_id", params.NodeID))
			payload, prepErr = PreparePayload(ctx, jsonBytes, pathCtx, e.uploader, nil)
		} else {
			md := map[string]string{
				"client_id":   params.ClientID,
				"project_id":  params.ProjectID,
				"workflow_id": params.WorkflowID,
				"run_id":      params.RunID,
				"node_id":     params.NodeID,
				"direction":   "output",
			}
			if dotExt == ".csv" {
				md["content_type"] = "text/csv; charset=utf-8"
				md["artifact"] = "csv_produce"
			} else {
				md["content_type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
				md["artifact"] = "xlsx_produce"
			}
			url, size, uploadErr := e.uploader.Upload(ctx, blobPath, rawProduce, md)
			if uploadErr != nil {
				e.logger.Warn("produce file monitoring blob upload failed, using PreparePayload",
					zap.String("node_id", params.NodeID),
					zap.Error(uploadErr),
				)
				payload, prepErr = PreparePayload(ctx, jsonBytes, pathCtx, e.uploader, nil)
			} else {
				payload = &event.Payload{
					InlineData: nil,
					BlobReference: &event.BlobReference{
						URL:  url,
						Size: size,
					},
				}
			}
		}
		if prepErr != nil {
			e.logger.Warn("PreparePayload failed after produce branch, using inline fallback",
				zap.String("node_id", params.NodeID),
				zap.Error(prepErr),
			)
			payload = &event.Payload{
				InlineData:    jsonBytes,
				BlobReference: nil,
			}
		}
	default:
		if isProduceFile && e.uploader == nil {
			e.logger.Warn("produce file output needs uploader for raw monitoring blob; using PreparePayload",
				zap.String("node_id", params.NodeID))
		}
		payload, prepErr = PreparePayload(ctx, jsonBytes, pathCtx, e.uploader, nil)
		if prepErr != nil {
			e.logger.Warn("PreparePayload failed, using inline fallback",
				zap.String("node_id", params.NodeID),
				zap.Error(prepErr),
			)
			payload = &event.Payload{
				InlineData:    jsonBytes,
				BlobReference: nil,
			}
		}
	}

	if payload == nil {
		return nil
	}
	if payload.Document == nil {
		payload.Files = ScanFiles(jsonBytes, DirectionOutput)
	}

	evt := event.New(event.TypeNodeEnded).
		WithClient(params.ClientID).
		WithWorkflow(params.WorkflowID).
		WithRun(params.RunID).
		WithNode(params.NodeID).
		WithData(&event.EndNode{
			WorkflowID:    params.WorkflowID,
			RunID:         params.RunID,
			ClientID:      params.ClientID,
			ProjectID:     params.ProjectID,
			NodeID:        params.NodeID,
			Label:         params.Label,
			EndedAt:       time.Now().UnixMilli(),
			Output:        payload,
			HasError:      params.HasError,
			ErrorMessage:  params.ErrorMessage,
			ContainsNodes: params.ContainsNodes,
		})

	if err := e.observer.Emit(ctx, evt); err != nil {
		e.logger.Error("failed to emit node.ended observation event",
			zap.String("workflow_id", params.WorkflowID),
			zap.String("run_id", params.RunID),
			zap.String("node_id", params.NodeID),
			zap.Error(err),
		)
		return err
	}

	return nil
}

// NodeStartEmitter emits node.started observation events with resolved payload.
// Implementations are best-effort and must not panic.
type NodeStartEmitter interface {
	EmitNodeStart(ctx context.Context, params NodeStartEmitParams) error
}

// NodeStartEmitParams contains all data needed to emit a node.started event.
type NodeStartEmitParams struct {
	ClientID      string
	ProjectID     string
	WorkflowID    string
	RunID      string
	NodeID     string
	Label      string // Human-readable node label (e.g. from execution plan)
	Input      []byte
}

// ArgusNodeStartEmitter implements NodeStartEmitter using Argus observer.
type ArgusNodeStartEmitter struct {
	observer  observer.Observer
	uploader  BlobUploader
	logger    *zap.Logger
	documents DocumentWriter
}

// WithDocuments makes the emitter write each input as the node's input document and send its
// reference (raw payloads D9). A failed write falls back to the inline or uploaded path. Returns e.
func (e *ArgusNodeStartEmitter) WithDocuments(w DocumentWriter) *ArgusNodeStartEmitter {
	e.documents = w
	return e
}

// NewArgusNodeStartEmitter creates an emitter. observer and uploader may be nil; emission will no-op or fall back to inline.
func NewArgusNodeStartEmitter(
	obs observer.Observer,
	uploader BlobUploader,
	logger *zap.Logger,
) *ArgusNodeStartEmitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ArgusNodeStartEmitter{
		observer: obs,
		uploader: uploader,
		logger:   logger,
	}
}

// EmitNodeStart emits a node.started event with resolved payload. Best-effort; logs errors, never panics.
func (e *ArgusNodeStartEmitter) EmitNodeStart(ctx context.Context, params NodeStartEmitParams) error {
	if e == nil || e.observer == nil {
		return nil
	}
	if params.ClientID == "" || params.WorkflowID == "" || params.RunID == "" || params.NodeID == "" {
		e.logger.Debug("skipping node.started emit due to missing context",
			zap.String("workflow_id", params.WorkflowID),
			zap.String("run_id", params.RunID),
			zap.String("node_id", params.NodeID),
		)
		return nil
	}
	if len(params.Input) == 0 {
		return nil
	}

	pathCtx := PathContext{
		ClientID:   params.ClientID,
		ProjectID:  params.ProjectID,
		WorkflowID: params.WorkflowID,
		RunID:      params.RunID,
		NodeID:     params.NodeID,
		IsInput:    true,
	}

	var payload *event.Payload
	if e.documents != nil {
		doc, err := documentPayload(ctx, e.documents, DocumentTarget{WorkflowID: params.WorkflowID, RunID: params.RunID,
			NodeID: params.NodeID, Direction: DirectionInput}, params.Input)
		if err == nil {
			payload = doc
		} else {
			e.logger.Warn("node input document not written, sending the input the old way",
				zap.String("node_id", params.NodeID), zap.Error(err))
		}
	}
	var prepErr error
	if payload == nil {
		payload, prepErr = PreparePayload(ctx, params.Input, pathCtx, e.uploader, nil)
		if payload != nil && prepErr == nil {
			payload.Files = ScanFiles(params.Input, DirectionInput)
		}
	}
	if prepErr != nil {
		e.logger.Warn("PreparePayload failed for node.started input payload, using inline fallback",
			zap.String("node_id", params.NodeID),
			zap.Error(prepErr),
		)
		payload = &event.Payload{
			InlineData:    params.Input,
			BlobReference: nil,
		}
	}

	if payload == nil {
		return nil
	}

	label := params.Label
	if label == "" {
		label = params.NodeID
	}
	evt := event.New(event.TypeNodeStarted).
		WithClient(params.ClientID).
		WithWorkflow(params.WorkflowID).
		WithRun(params.RunID).
		WithNode(params.NodeID).
		WithData(&event.StartNode{
			WorkflowID:    params.WorkflowID,
			RunID:         params.RunID,
			ClientID:      params.ClientID,
			ProjectID:     params.ProjectID,
			NodeID:        params.NodeID,
			Label:      label,
			StartedAt:  time.Now().UnixMilli(),
			Input:      payload,
		})

	if err := e.observer.Emit(ctx, evt); err != nil {
		e.logger.Error("failed to emit node.started observation event",
			zap.String("workflow_id", params.WorkflowID),
			zap.String("run_id", params.RunID),
			zap.String("node_id", params.NodeID),
			zap.Error(err),
		)
		return err
	}
	return nil
}
