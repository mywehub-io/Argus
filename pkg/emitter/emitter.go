package emitter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wehubfusion/Argus/pkg/event"
	"github.com/wehubfusion/Argus/pkg/observer"
	"go.uber.org/zap"
)

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
	documents DocumentWriter
	logger    *zap.Logger
}

// NewArgusNodeEndEmitter creates an emitter that writes each output as the node's output document
// in the run's files and sends its reference (raw payloads D9). observer or documents may be nil;
// emission then no-ops.
func NewArgusNodeEndEmitter(
	obs observer.Observer,
	documents DocumentWriter,
	logger *zap.Logger,
) *ArgusNodeEndEmitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ArgusNodeEndEmitter{
		observer:  obs,
		documents: documents,
		logger:    logger,
	}
}

// EmitNodeEnd emits a node.ended event whose output is the node's output document. Best-effort;
// logs errors, never panics. When the document cannot be written the event is still sent, with no
// output, so the node's end is not lost to monitoring.
func (e *ArgusNodeEndEmitter) EmitNodeEnd(ctx context.Context, params NodeEndEmitParams) error {
	if e == nil || e.observer == nil || e.documents == nil {
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

	// Marshal output as-is (no label wrapping)
	jsonBytes, err := json.Marshal(params.Output)
	if err != nil {
		e.logger.Error("failed to marshal node output for observation",
			zap.String("node_id", params.NodeID),
			zap.Error(err),
		)
		return err
	}

	payload, err := documentPayload(ctx, e.documents, DocumentTarget{WorkflowID: params.WorkflowID, RunID: params.RunID,
		NodeID: params.NodeID, Direction: DirectionOutput}, jsonBytes)
	if err != nil {
		e.logger.Warn("node output document not written, sending node.ended without output",
			zap.String("node_id", params.NodeID), zap.Error(err))
		payload = &event.Payload{}
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
	ClientID   string
	ProjectID  string
	WorkflowID string
	RunID      string
	NodeID     string
	Label      string // Human-readable node label (e.g. from execution plan)
	Input      []byte
}

// ArgusNodeStartEmitter implements NodeStartEmitter using Argus observer.
type ArgusNodeStartEmitter struct {
	observer  observer.Observer
	documents DocumentWriter
	logger    *zap.Logger
}

// NewArgusNodeStartEmitter creates an emitter that writes each input as the node's input document
// in the run's files and sends its reference (raw payloads D9). observer or documents may be nil;
// emission then no-ops.
func NewArgusNodeStartEmitter(
	obs observer.Observer,
	documents DocumentWriter,
	logger *zap.Logger,
) *ArgusNodeStartEmitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ArgusNodeStartEmitter{
		observer:  obs,
		documents: documents,
		logger:    logger,
	}
}

// EmitNodeStart emits a node.started event whose input is the node's input document. Best-effort;
// logs errors, never panics. When the document cannot be written the event is still sent, with no
// input.
func (e *ArgusNodeStartEmitter) EmitNodeStart(ctx context.Context, params NodeStartEmitParams) error {
	if e == nil || e.observer == nil || e.documents == nil {
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

	payload, err := documentPayload(ctx, e.documents, DocumentTarget{WorkflowID: params.WorkflowID, RunID: params.RunID,
		NodeID: params.NodeID, Direction: DirectionInput}, params.Input)
	if err != nil {
		e.logger.Warn("node input document not written, sending node.started without input",
			zap.String("node_id", params.NodeID), zap.Error(err))
		payload = &event.Payload{}
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
			WorkflowID: params.WorkflowID,
			RunID:      params.RunID,
			ClientID:   params.ClientID,
			ProjectID:  params.ProjectID,
			NodeID:     params.NodeID,
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
