// Package observer provides an interface for emitting observation events to NATS JetStream.
// By default Emit publishes directly to NATS and returns the actual delivery result, so callers
// know immediately whether the event reached the broker. In async mode (Options.AsyncQueueSize)
// Emit queues the event and returns at once, so a slow monitoring stream never slows the caller.
// Use NewObserver to create an observer.
// The default stream name matches event.StreamName.
package observer

import (
	"context"
	"fmt"
	"sync"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/wehubfusion/Argus/internal/nats"
	"github.com/wehubfusion/Argus/pkg/event"
	"go.uber.org/zap"
)

// Observer provides a synchronous, thread-safe interface for emitting observation events.
// Each Emit call publishes directly to NATS JetStream; the return value reflects actual delivery.
type Observer interface {
	// Emit validates and publishes an observation event to NATS JetStream.
	// Returns ErrObserverClosed if the observer has been closed.
	// Returns a wrapped error if validation, serialization, or publish fails.
	Emit(ctx context.Context, evt *event.Event) error

	// Close marks the observer as closed. Subsequent Emit calls return ErrObserverClosed.
	// Safe to call multiple times (idempotent).
	Close(ctx context.Context) error
}

// observer implements the Observer interface with synchronous publishing.
// It is safe for concurrent use: multiple goroutines may call Emit concurrently.
// mu guards isClosed for read access in Emit; closeOnce ensures Close() runs
// its body at most once, making Close() safe to call from multiple goroutines
// or multiple times (idempotent). The sync.RWMutex allows concurrent Emit
// calls (read-lock) to proceed without blocking each other, while Close
// (write-lock) drains in-progress Emit calls before marking the observer
// closed.
type observer struct {
	publisher publisher
	options   Options
	logger    *zap.Logger
	// closeOnce ensures the close logic (marking isClosed, logging) runs exactly
	// once even if Close() is called concurrently from multiple goroutines.
	closeOnce sync.Once
	// mu guards isClosed. Emit acquires a read lock; Close acquires a write lock
	// so it waits for all in-progress Emit calls to finish before closing.
	mu       sync.RWMutex
	isClosed bool

	// async is set in async mode (Options.AsyncQueueSize > 0).
	async *asyncQueue
}

// publisher is what the observer publishes through; *nats.Publisher in production.
type publisher interface {
	Publish(ctx context.Context, subject, msgID string, data []byte) error
}

// NewObserver creates a new Observer instance.
func NewObserver(js natsclient.JetStreamContext, opts Options, logger *zap.Logger) (Observer, error) {
	if js == nil {
		return nil, fmt.Errorf("jetstream context cannot be nil")
	}
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	// Apply defaults
	if opts.StreamName == "" {
		opts = opts.WithStreamName(DefaultOptions().StreamName)
	}
	if opts.StreamMaxAge == 0 {
		opts = opts.WithStreamMaxAge(DefaultOptions().StreamMaxAge)
	}
	if opts.StreamMaxMsgs == 0 {
		opts = opts.WithStreamMaxMsgs(DefaultOptions().StreamMaxMsgs)
	}
	if opts.PublishTimeout == 0 {
		opts = opts.WithPublishTimeout(DefaultOptions().PublishTimeout)
	}

	publisherConfig := nats.PublisherConfig{
		StreamName:     opts.StreamName,
		StreamMaxAge:   opts.StreamMaxAge,
		StreamMaxMsgs:  opts.StreamMaxMsgs,
		PublishTimeout: opts.PublishTimeout,
	}
	publisher, err := nats.NewPublisher(js, publisherConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	logger.Info("Observer created",
		zap.String("stream_name", opts.StreamName))

	o := &observer{
		publisher: publisher,
		options:   opts,
		logger:    logger,
	}
	if opts.AsyncQueueSize > 0 {
		o.async = newAsyncQueue(o, opts)
	}
	return o, nil
}

// Emit validates and publishes an observation event synchronously.
// Auto-populates ID, Timestamp, and Version if absent.
func (o *observer) Emit(ctx context.Context, evt *event.Event) error {
	o.mu.RLock()
	closed := o.isClosed
	o.mu.RUnlock()

	if closed {
		return ErrObserverClosed
	}

	// Auto-populate required fields
	// The publish-time dedup id (Nats-Msg-Id): derived from the event's content unless the
	// caller set its own.
	evt.ID = evt.EmitID()
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	if evt.Version == "" {
		evt.Version = "v1"
	}

	return o.publishEvent(ctx, evt)
}

// publishEvent validates, serializes, and publishes a single event.
// Accepts the caller's context so publish honours both the context deadline and PublishTimeout.
func (o *observer) publishEvent(ctx context.Context, evt *event.Event) error {
	if evt == nil {
		return fmt.Errorf("observer: nil event")
	}

	if err := evt.Validate(); err != nil {
		return fmt.Errorf("observer: invalid event: %w", err)
	}

	subject := event.SubjectForEventTypeWithEnv(evt.Type, evt.EnvironmentID)
	if subject == "" {
		return fmt.Errorf("observer: unknown event type %q", evt.Type)
	}

	data, err := evt.Bytes()
	if err != nil {
		return fmt.Errorf("observer: failed to serialize event: %w", err)
	}

	if o.async != nil {
		o.async.enqueue(ctx, queued{subject: subject, id: evt.ID, data: data, eventType: evt.Type})
		return nil
	}

	if err := o.publisher.Publish(ctx, subject, evt.ID, data); err != nil {
		return fmt.Errorf("observer: failed to publish event: %w", err)
	}

	// Debug, not Info: one line per event (several per unit) was most of a service's log volume.
	o.logger.Debug("Argus observation event published",
		zap.String("event_type", evt.Type),
		zap.String("workflow_id", evt.WorkflowID),
		zap.String("run_id", evt.RunID),
		zap.String("node_id", evt.NodeID),
		zap.String("subject", subject),
		zap.String("dedupe_msg_id", evt.ID))

	return nil
}

// Close marks the observer as closed. In async mode it then waits for queued events to be
// published, until ctx ends or, with no deadline on ctx, CloseFlushTimeout. Idempotent; safe to
// call multiple times.
func (o *observer) Close(ctx context.Context) error {
	var err error
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.isClosed = true
		if o.async != nil {
			o.async.closeQueue()
		}
		o.mu.Unlock()
		if o.async != nil {
			err = o.async.flush(ctx, o.options.CloseFlushTimeout)
		}
		o.logger.Info("Observer closed")
	})
	return err
}
