package emitter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wehubfusion/Argus/pkg/event"
	"go.uber.org/zap"
)

// Lane defaults.
const (
	DefaultLaneConcurrency          = 8
	DefaultLaneMaxQueuedBytes int64 = 64 << 20
	DefaultLaneMaxQueuedJobs        = 16384
	DefaultLaneWriteTimeout         = 30 * time.Second
)

// LaneOptions configures NewLane. Zero values take the defaults.
type LaneOptions struct {
	// Concurrency is how many documents are written at once, across every run.
	Concurrency int
	// MaxQueuedBytes bounds the document bytes held in the lane. A submission that would pass it
	// is written and published on the caller's goroutine instead, after the run's earlier events.
	MaxQueuedBytes int64
	// MaxQueuedJobs bounds how many documents the lane holds, whatever their size: each is a
	// goroutine until it is published, and a document built on the lane (Document.Build) counts
	// towards no byte bound. A submission that would pass it runs on the caller's goroutine, as
	// for MaxQueuedBytes.
	MaxQueuedJobs int
	// WriteTimeout bounds one document write, so a stuck request cannot hold back its run.
	WriteTimeout time.Duration
	// Hooks report what the lane does, for metrics. Every hook is optional.
	Hooks LaneHooks
}

// LaneHooks are called as the lane works. They must be cheap and must not block.
type LaneHooks struct {
	// Queued is called when a document is accepted, with its size.
	Queued func(bytes int)
	// Inline is called when a submission ran on the caller's goroutine because the lane was full.
	Inline func()
	// Written is called after each document write, with how long it took and its error.
	Written func(took time.Duration, err error)
	// Published is called after each event is handed to the observer, with the time from
	// submission and the observer's error.
	Published func(sinceSubmit time.Duration, err error)
	// Abandoned is called when a submission that ran on the caller's goroutine (the lane was
	// full) gave up because the caller's context ended: its event was not published.
	Abandoned func(err error)
}

// Lane writes node documents and publishes their events off the unit's critical path.
//
// Without a lane, EmitNodeStart and EmitNodeEnd write the node's document to storage and only
// then return, so a unit waits for every one of its documents in turn. With a lane on the context
// (WithLane), they hand the document to the lane and return at once. The lane writes documents
// concurrently, up to Concurrency at a time, and publishes each event only once its document is
// written and every earlier event of the same run has been published. Monitoring therefore never
// receives a reference to a document that does not exist yet, and a run's events keep the order
// they were submitted in. Runs do not wait for each other.
//
// Each event carries the time it was submitted, not the time it was published.
//
// The lane holds at most MaxQueuedBytes and MaxQueuedJobs; a submission past either is done on
// the caller's goroutine, still after its run's earlier events, so memory stays bounded and
// nothing is dropped.
//
// A document that cannot be written still has its event published, without the document, as the
// synchronous path does. Documents queued when the process is killed are lost; Drain at shutdown
// publishes what is left.
type Lane struct {
	opts   LaneOptions
	logger *zap.Logger
	sem    chan struct{}

	mu         sync.Mutex
	runs       map[string]chan struct{} // per run: closed when its last submitted event is published
	queued     int64                    // bytes of the documents queued
	queuedJobs int                      // documents queued, whatever their size
	wg         sync.WaitGroup
}

// NewLane returns a lane. Each document is written with the DocumentWriter of the emitter that
// submitted it.
func NewLane(opts LaneOptions, logger *zap.Logger) *Lane {
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultLaneConcurrency
	}
	if opts.MaxQueuedBytes <= 0 {
		opts.MaxQueuedBytes = DefaultLaneMaxQueuedBytes
	}
	if opts.MaxQueuedJobs <= 0 {
		opts.MaxQueuedJobs = DefaultLaneMaxQueuedJobs
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = DefaultLaneWriteTimeout
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Lane{
		opts:   opts,
		logger: logger,
		sem:    make(chan struct{}, opts.Concurrency),
		runs:   map[string]chan struct{}{},
	}
}

// QueuedBytes reports the document bytes held in the lane.
func (l *Lane) QueuedBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queued
}

// QueuedJobs reports how many documents the lane holds.
func (l *Lane) QueuedJobs() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queuedJobs
}

// Drain waits until every submitted event has been published, or ctx ends. It returns an error
// naming how many were still pending when ctx ended. The lane stays usable after Drain.
func (l *Lane) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		pending := len(l.runs)
		l.mu.Unlock()
		return fmt.Errorf("emitter: lane drain: %d runs still had events pending: %w", pending, ctx.Err())
	}
}

// laneJob is one document and the event that references it.
type laneJob struct {
	writer DocumentWriter
	target DocumentTarget
	data   []byte
	// build, when set, produces the document on the lane instead of data.
	build     func(ctx context.Context) ([]byte, error)
	submitted time.Time
	// send builds the event around the document's payload and hands it to the observer.
	send func(ctx context.Context, payload *event.Payload) error
}

// Document is one node document and the event that references it, for a caller that builds its
// own event: a connector step's node.started and node.ended carry fields the node emitters do not.
type Document struct {
	// Writer writes the document; Target names it.
	Writer DocumentWriter
	Target DocumentTarget
	// Data is the document. When it is nil, Build is called on the lane instead, so a document
	// that takes a read to assemble costs the unit nothing. Build returning nil and no error
	// means there is no document: the event is sent without one and nothing is written.
	Data  []byte
	Build func(ctx context.Context) ([]byte, error)
	// Send builds the event around the document's payload and hands it to the observer. The
	// payload is empty when the document could not be built or written. Send runs after the
	// run's earlier events have been published.
	Send func(ctx context.Context, payload *event.Payload) error
}

// Submit hands a document and its event to the lane, as the node emitters do with a lane on the
// context. It returns once the document is queued, or, when the lane is full, once its event has
// been published on the caller's goroutine. The submission time is the caller's to stamp on the
// event it builds in Send.
func (l *Lane) Submit(ctx context.Context, d Document) error {
	if d.Send == nil || (d.Data == nil && d.Build == nil) {
		return fmt.Errorf("emitter: lane submit needs a document (Data or Build) and Send")
	}
	return l.submit(ctx, laneJob{writer: d.Writer, target: d.Target, data: d.Data, build: d.Build, submitted: time.Now(), send: d.Send})
}

// submit hands a job to the lane. It returns once the job is queued, or, when the lane is full,
// once the job's event has been published on this goroutine.
func (l *Lane) submit(ctx context.Context, job laneJob) error {
	n := int64(len(job.data))
	run := job.target.RunID

	l.mu.Lock()
	prev := l.runs[run]
	done := make(chan struct{})
	l.runs[run] = done
	l.wg.Add(1)
	queue := l.queued+n <= l.opts.MaxQueuedBytes && l.queuedJobs < l.opts.MaxQueuedJobs
	if queue {
		l.queued += n
		l.queuedJobs++
	}
	l.mu.Unlock()

	if !queue {
		// Full: do the work here, still after the run's earlier events.
		if h := l.opts.Hooks.Inline; h != nil {
			h()
		}
		return l.run(ctx, job, prev, done, 0, false)
	}
	if h := l.opts.Hooks.Queued; h != nil {
		h(int(n))
	}
	// The caller may reuse its buffer once this returns (a node's input is the unit's resolved
	// payload), so the lane keeps its own copy.
	job.data = append([]byte(nil), job.data...)
	go func() { _ = l.run(context.WithoutCancel(ctx), job, prev, done, n, true) }()
	return nil
}

// run writes the job's document, waits for the run's previous event, publishes its own and
// releases the run's slot. queuedBytes and queued are what submit reserved for a queued job; an
// inline job reserved nothing.
//
// A queued job runs under a context that never ends (submit detaches it), so only an inline job,
// running under its caller's context, can give up. It gives up while waiting for a write slot,
// for the run's previous event, or once its context has ended by the time it would publish. It
// then publishes nothing: its event is lost to monitoring, but the run's order is kept.
func (l *Lane) run(ctx context.Context, job laneJob, prev, done chan struct{}, queuedBytes int64, queued bool) error {
	// finish releases what the job holds and lets the run's next job publish. A job that gives up
	// calls it only once the run's previous job has finished, never earlier: closing done early
	// would let the next job publish ahead of an event still queued before this one.
	finish := func() {
		l.mu.Lock()
		l.queued -= queuedBytes
		if queued {
			l.queuedJobs--
		}
		if l.runs[job.target.RunID] == done {
			delete(l.runs, job.target.RunID)
		}
		l.mu.Unlock()
		close(done)
		l.wg.Done()
	}

	payload, ok := l.write(ctx, job)
	if !ok {
		return l.abandon(ctx, job, prev, finish)
	}
	if prev != nil {
		select {
		case <-prev:
		case <-ctx.Done():
			return l.abandon(ctx, job, prev, finish)
		}
	}
	if ctx.Err() != nil {
		return l.abandon(ctx, job, nil, finish)
	}
	defer finish()
	err := job.send(ctx, payload)
	if h := l.opts.Hooks.Published; h != nil {
		h(time.Since(job.submitted), err)
	}
	return err
}

// abandon records that a job running on its caller's goroutine gave up because the caller's
// context ended, and returns at once with that context's error. Its slot in the run is released
// once prev (the run's previous job, if still pending) has finished, so the run's order holds.
func (l *Lane) abandon(ctx context.Context, job laneJob, prev chan struct{}, finish func()) error {
	err := ctx.Err()
	if h := l.opts.Hooks.Abandoned; h != nil {
		h(err)
	}
	l.logger.Warn("node event not sent: the lane was full and the caller's context ended while it waited",
		zap.String("run_id", job.target.RunID),
		zap.String("node_id", job.target.NodeID),
		zap.String("direction", job.target.Direction),
		zap.Error(err))
	if prev == nil {
		finish()
	} else {
		go func() {
			<-prev
			finish()
		}()
	}
	return err
}

// write writes the job's document within the lane's concurrency and timeout. A failure is logged
// and yields an empty payload, so the event is still published. ok is false when ctx ended while
// the job waited for a write slot: nothing was written.
func (l *Lane) write(ctx context.Context, job laneJob) (payload *event.Payload, ok bool) {
	select {
	case l.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, false
	}
	defer func() { <-l.sem }()

	wctx, cancel := context.WithTimeout(ctx, l.opts.WriteTimeout)
	defer cancel()
	start := time.Now()
	data, err := job.data, error(nil)
	if job.build != nil {
		data, err = job.build(wctx)
	}
	if err == nil && data == nil && job.build != nil {
		// Build had no document to give (a step with no input): the event goes without one.
		return &event.Payload{}, true
	}
	if err == nil {
		payload, err = documentPayload(wctx, job.writer, job.target, data)
	}
	if h := l.opts.Hooks.Written; h != nil {
		h(time.Since(start), err)
	}
	if err != nil {
		l.logger.Warn("node document not written, sending its event without it",
			zap.String("run_id", job.target.RunID),
			zap.String("node_id", job.target.NodeID),
			zap.String("direction", job.target.Direction),
			zap.Error(err))
		return &event.Payload{}, true
	}
	return payload, true
}

type laneKey struct{}

// WithLane returns ctx carrying lane. The node emitters use it when they find it, so code that
// runs below an API it cannot change (an embedded runtime given the unit's context) uses the
// lane without being told.
func WithLane(ctx context.Context, lane *Lane) context.Context {
	if lane == nil {
		return ctx
	}
	return context.WithValue(ctx, laneKey{}, lane)
}

// LaneFrom returns the lane WithLane put on ctx, or nil.
func LaneFrom(ctx context.Context) *Lane {
	if ctx == nil {
		return nil
	}
	l, _ := ctx.Value(laneKey{}).(*Lane)
	return l
}
