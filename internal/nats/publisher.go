package nats

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Argus/pkg/event"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// PublisherConfig holds configuration for the NATS publisher.
type PublisherConfig struct {
	StreamName     string
	StreamMaxAge   time.Duration
	StreamMaxMsgs  int64
	StreamMaxBytes int64
	PublishTimeout time.Duration
}

// Publisher handles publishing messages to NATS JetStream.
// All methods are safe for concurrent use.
type Publisher struct {
	js     nats.JetStreamContext
	config PublisherConfig
	logger *zap.Logger
}

// NewPublisher creates a new Publisher and ensures the target stream exists.
func NewPublisher(js nats.JetStreamContext, config PublisherConfig, logger *zap.Logger) (*Publisher, error) {
	if js == nil {
		return nil, fmt.Errorf("jetstream context cannot be nil")
	}
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	p := &Publisher{js: js, config: config, logger: logger}
	if err := p.ensureStream(); err != nil {
		return nil, fmt.Errorf("failed to ensure observation stream: %w", err)
	}
	return p, nil
}

// ensureStream creates the observation stream if it does not already exist, and brings an
// existing one to the configured size cap (it used to be left as created, with no cap at all).
func (p *Publisher) ensureStream() error {
	streamInfo, err := p.js.StreamInfo(p.config.StreamName)
	if err != nil && err != nats.ErrStreamNotFound {
		return fmt.Errorf("failed to check stream info: %w", err)
	}

	if streamInfo != nil {
		p.logger.Info("Observation stream already exists",
			zap.String("stream", p.config.StreamName),
			zap.Uint64("messages", streamInfo.State.Msgs))
		return p.ensureSizeCap(streamInfo.Config)
	}

	streamConfig := &nats.StreamConfig{
		Name:     p.config.StreamName,
		Subjects: []string{event.SubjectPatternAll},
		Storage:  nats.FileStorage,
		MaxAge:   p.config.StreamMaxAge,
		MaxMsgs:  p.config.StreamMaxMsgs,
		MaxBytes: p.config.StreamMaxBytes,
		// Above a limit the oldest events go: events are kept by limits, not removed on ack, so
		// refusing new ones would stop monitoring once the stream filled.
		Discard:  nats.DiscardOld,
		Replicas: 1,
	}
	if _, err := p.js.AddStream(streamConfig); err != nil {
		return fmt.Errorf("failed to create observation stream: %w", err)
	}

	p.logger.Info("Created observation stream",
		zap.String("stream", p.config.StreamName),
		zap.Strings("subjects", streamConfig.Subjects),
		zap.Duration("max_age", streamConfig.MaxAge),
		zap.Int64("max_msgs", streamConfig.MaxMsgs),
		zap.Int64("max_bytes", streamConfig.MaxBytes))
	return nil
}

// ensureSizeCap updates an existing stream whose size cap differs from the configured one. Only
// MaxBytes and Discard change. Lowering the cap below the stream's size drops its oldest events.
func (p *Publisher) ensureSizeCap(cfg nats.StreamConfig) error {
	if p.config.StreamMaxBytes <= 0 || (cfg.MaxBytes == p.config.StreamMaxBytes && cfg.Discard == nats.DiscardOld) {
		return nil
	}
	previous := cfg.MaxBytes
	cfg.MaxBytes = p.config.StreamMaxBytes
	cfg.Discard = nats.DiscardOld
	if _, err := p.js.UpdateStream(&cfg); err != nil {
		return fmt.Errorf("failed to set the observation stream's size cap: %w", err)
	}
	p.logger.Info("Updated the observation stream's size cap",
		zap.String("stream", p.config.StreamName),
		zap.Int64("from", previous),
		zap.Int64("to", cfg.MaxBytes))
	return nil
}

// Publish publishes a message to JetStream with exactly-once deduplication support.
//
// It blocks until the server acknowledges receipt, the caller's context is cancelled,
// or PublishTimeout elapses — whichever comes first. No goroutines are spawned;
// backpressure is applied directly to the caller.
//
// nats.Context(publishCtx) passes the context into the JetStream ack-wait select
// loop (RequestMsgWithContext), so cancellation is handled natively by the NATS
// client rather than via a wrapper goroutine.
//
// The caller's span (if any) is injected as a W3C traceparent into the message header, and the
// publish itself is wrapped in its own SpanKindProducer span — so a consumer that extracts the
// header continues the emitting service's trace instead of starting a disconnected root.
func (p *Publisher) Publish(ctx context.Context, subject, msgID string, data []byte) (err error) {
	if subject == "" {
		return fmt.Errorf("publisher: subject cannot be empty")
	}
	if msgID == "" {
		return fmt.Errorf("publisher: message ID cannot be empty")
	}
	if len(data) == 0 {
		return fmt.Errorf("publisher: data cannot be empty")
	}

	ctx, span := otel.Tracer("argus/nats-publisher").Start(ctx, subject, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", subject),
		attribute.String("messaging.message.id", msgID),
	))
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
	}()

	// Fast-path: reject immediately if the caller's context is already done.
	if err = ctx.Err(); err != nil {
		return fmt.Errorf("publisher: context already done: %w", err)
	}

	// Apply per-publish timeout as a tighter bound on the caller's context.
	// nats.Context(publishCtx) is mutually exclusive with nats.AckWait, so
	// we do NOT also pass an AckWait option.
	publishCtx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
	defer cancel()

	msg := &nats.Msg{Subject: subject, Data: data, Header: nats.Header{}}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))

	ack, err := p.js.PublishMsg(msg,
		nats.MsgId(msgID),
		nats.Context(publishCtx),
	)
	if err != nil {
		return fmt.Errorf("publisher: publish failed: %w", err)
	}

	p.logger.Debug("Published message",
		zap.String("msg_id", msgID),
		zap.String("subject", subject),
		zap.String("stream", ack.Stream),
		zap.Uint64("sequence", ack.Sequence))

	return nil
}
