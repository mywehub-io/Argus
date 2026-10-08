package observer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// queued is one serialised event waiting to be published.
type queued struct {
	ctx       context.Context
	subject   string
	id        string
	data      []byte
	eventType string
}

// asyncQueue publishes events on background workers so Emit never waits for the broker. A full
// queue drops the event: monitoring must never slow a workflow (raw payloads phase 15).
type asyncQueue struct {
	o       *observer
	ch      chan queued
	wg      sync.WaitGroup
	dropped atomic.Uint64
	failed  atomic.Uint64

	logMu      sync.Mutex
	lastLog    time.Time
	loggedDrop uint64
	loggedFail uint64
}

func newAsyncQueue(o *observer, opts Options) *asyncQueue {
	workers := opts.AsyncWorkers
	if workers <= 0 {
		workers = DefaultOptions().AsyncWorkers
	}
	q := &asyncQueue{o: o, ch: make(chan queued, opts.AsyncQueueSize)}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.run()
	}
	return q
}

// enqueue queues e without blocking. The observer's read lock is held for the send, so Close
// (which takes the write lock to close the channel) cannot close it underneath.
func (q *asyncQueue) enqueue(ctx context.Context, e queued) {
	// The publish happens after the caller has moved on: keep its trace, drop its cancellation.
	e.ctx = context.WithoutCancel(ctx)
	q.o.mu.RLock()
	defer q.o.mu.RUnlock()
	if q.o.isClosed {
		q.dropped.Add(1)
		q.maybeLog()
		return
	}
	select {
	case q.ch <- e:
	default:
		q.dropped.Add(1)
		q.maybeLog()
	}
}

func (q *asyncQueue) run() {
	defer q.wg.Done()
	for e := range q.ch {
		if err := q.o.publisher.Publish(e.ctx, e.subject, e.id, e.data); err != nil {
			q.failed.Add(1)
			q.maybeLog()
			q.o.logger.Debug("Argus observation event not published",
				zap.String("event_type", e.eventType), zap.String("dedupe_msg_id", e.id), zap.Error(err))
		}
	}
}

// maybeLog reports drops and failures since the last report, at most once per DropLogInterval.
func (q *asyncQueue) maybeLog() {
	interval := q.o.options.DropLogInterval
	if interval <= 0 {
		interval = DefaultOptions().DropLogInterval
	}
	q.logMu.Lock()
	defer q.logMu.Unlock()
	if time.Since(q.lastLog) < interval {
		return
	}
	dropped, failed := q.dropped.Load(), q.failed.Load()
	q.o.logger.Warn("Argus observation events lost",
		zap.Uint64("dropped_queue_full", dropped-q.loggedDrop),
		zap.Uint64("publish_failed", failed-q.loggedFail),
		zap.Uint64("dropped_total", dropped),
		zap.Uint64("failed_total", failed),
		zap.Int("queue_size", cap(q.ch)))
	q.lastLog = time.Now()
	q.loggedDrop, q.loggedFail = dropped, failed
}

// closeQueue stops new events; the caller holds the observer's write lock.
func (q *asyncQueue) closeQueue() { close(q.ch) }

// flush waits for the workers to publish what is queued, until ctx ends or, when ctx has no
// deadline, timeout passes. It returns ctx's error, or context.DeadlineExceeded, if events were
// still queued.
func (q *asyncQueue) flush(ctx context.Context, timeout time.Duration) error {
	if _, ok := ctx.Deadline(); !ok && timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		q.o.logger.Warn("Argus observer closed before every queued event was published",
			zap.Int("still_queued", len(q.ch)))
		return ctx.Err()
	}
}

// Stats counts events async mode did not deliver.
type Stats struct {
	// Dropped is events dropped because the queue was full, or emitted after Close.
	Dropped uint64
	// Failed is events whose publish returned an error.
	Failed uint64
}

// StatsOf returns the async counters of an Observer made by NewObserver; zero in sync mode.
func StatsOf(obs Observer) Stats {
	o, ok := obs.(*observer)
	if !ok || o.async == nil {
		return Stats{}
	}
	return Stats{Dropped: o.async.dropped.Load(), Failed: o.async.failed.Load()}
}
