package observer

import (
	"time"

	"github.com/wehubfusion/Argus/pkg/event"
)

// Options configures the Observer behavior
type Options struct {
	// StreamName is the JetStream stream name for observation events (default: event.StreamName)
	StreamName string

	// StreamMaxAge is the maximum age for messages in the stream
	// Default: 30 days
	StreamMaxAge time.Duration

	// StreamMaxMsgs is the maximum number of messages in the stream
	// Default: 1000000
	StreamMaxMsgs int64

	// StreamMaxBytes caps the stream's size on disk; above it the oldest events are dropped. An
	// existing stream is brought to this value at start. Without it OBSERVATION grew to 14 GB and,
	// with the other streams, filled the NATS disk (UAT incident 08/10/2026, C2).
	// Default: 4 GiB
	StreamMaxBytes int64

	// PublishTimeout is the timeout for publishing events to JetStream
	// Default: 5 seconds
	PublishTimeout time.Duration

	// AsyncQueueSize turns on async mode when above 0: Emit validates and serialises the event,
	// queues it and returns without waiting for the broker. When the queue is full the event is
	// dropped and counted; Emit never blocks. 0 (default) publishes synchronously, as before.
	AsyncQueueSize int

	// AsyncWorkers is how many goroutines publish queued events. Default: 4.
	AsyncWorkers int

	// DropLogInterval is the least time between two log lines about dropped or failed events.
	// Default: 30 seconds.
	DropLogInterval time.Duration

	// CloseFlushTimeout bounds how long Close waits for queued events to be published when the
	// caller's context has no deadline. Default: 5 seconds.
	CloseFlushTimeout time.Duration
}

// DefaultOptions returns default options for the Observer
func DefaultOptions() Options {
	return Options{
		StreamName:        event.StreamName,
		StreamMaxAge:      30 * 24 * time.Hour, // 30 days
		StreamMaxMsgs:     1000000,
		StreamMaxBytes:    DefaultStreamMaxBytes,
		PublishTimeout:    5 * time.Second,
		AsyncWorkers:      4,
		DropLogInterval:   30 * time.Second,
		CloseFlushTimeout: 5 * time.Second,
	}
}

// WithAsync turns on async mode with a queue of queueSize events (see AsyncQueueSize).
func (o Options) WithAsync(queueSize int) Options {
	o.AsyncQueueSize = queueSize
	return o
}

// WithStreamName sets the stream name
func (o Options) WithStreamName(name string) Options {
	o.StreamName = name
	return o
}

// WithStreamMaxAge sets the stream max age
func (o Options) WithStreamMaxAge(age time.Duration) Options {
	o.StreamMaxAge = age
	return o
}

// DefaultStreamMaxBytes is the default StreamMaxBytes, 4 GiB.
const DefaultStreamMaxBytes int64 = 4 << 30

// WithStreamMaxBytes sets the stream's size cap in bytes.
func (o Options) WithStreamMaxBytes(maxBytes int64) Options {
	o.StreamMaxBytes = maxBytes
	return o
}

// WithStreamMaxMsgs sets the stream max messages
func (o Options) WithStreamMaxMsgs(maxMsgs int64) Options {
	o.StreamMaxMsgs = maxMsgs
	return o
}

// WithPublishTimeout sets the publish timeout
func (o Options) WithPublishTimeout(timeout time.Duration) Options {
	o.PublishTimeout = timeout
	return o
}
