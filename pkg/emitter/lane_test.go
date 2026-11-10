package emitter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wehubfusion/Argus/pkg/event"
)

// slowWriter writes documents after a per-node delay, optionally failing some, and records when
// each write finished.
type slowWriter struct {
	mu       sync.Mutex
	delay    map[string]time.Duration // by node id
	fail     map[string]bool
	inFlight int
	peak     int
	written  map[string]time.Time
}

func newSlowWriter() *slowWriter {
	return &slowWriter{delay: map[string]time.Duration{}, fail: map[string]bool{}, written: map[string]time.Time{}}
}

func (w *slowWriter) WriteDocument(ctx context.Context, t DocumentTarget, data []byte) (event.FileRef, error) {
	w.mu.Lock()
	d := w.delay[t.NodeID]
	fail := w.fail[t.NodeID]
	w.inFlight++
	if w.inFlight > w.peak {
		w.peak = w.inFlight
	}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.inFlight--
		w.mu.Unlock()
	}()
	select {
	case <-time.After(d):
	case <-ctx.Done():
		return event.FileRef{}, ctx.Err()
	}
	if fail {
		return event.FileRef{}, errors.New("storage refused")
	}
	w.mu.Lock()
	w.written[t.NodeID+"."+t.Direction] = time.Now()
	w.mu.Unlock()
	return event.FileRef{Path: "results/" + t.WorkflowID + "/" + t.RunID + "/" + t.NodeID + "." + t.Direction + ".json", Size: int64(len(data))}, nil
}

// orderObserver records events in the order they are published.
type orderObserver struct {
	mu     sync.Mutex
	events []*event.Event
	at     []time.Time
}

func (o *orderObserver) Emit(_ context.Context, e *event.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
	o.at = append(o.at, time.Now())
	return nil
}
func (o *orderObserver) Close(context.Context) error { return nil }

func (o *orderObserver) nodes(run string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for _, e := range o.events {
		if e.RunID == run {
			out = append(out, e.NodeID+":"+e.Type)
		}
	}
	return out
}

func endData(t *testing.T, e *event.Event) event.EndNode {
	t.Helper()
	var d event.EndNode
	if err := json.Unmarshal(e.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func startData(t *testing.T, e *event.Event) event.StartNode {
	t.Helper()
	var d event.StartNode
	if err := json.Unmarshal(e.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func startParams(run, node string) NodeStartEmitParams {
	return NodeStartEmitParams{ClientID: "c", WorkflowID: "wf", RunID: run, NodeID: node, Input: []byte(`{"in":1}`)}
}

func endParams(run, node string) NodeEndEmitParams {
	return NodeEndEmitParams{ClientID: "c", WorkflowID: "wf", RunID: run, NodeID: node, Output: map[string]int{"out": 1}}
}

func TestLaneKeepsARunsOrderUnderRandomWriteDelays(t *testing.T) {
	w := newSlowWriter()
	obs := &orderObserver{}
	start := NewArgusNodeStartEmitter(obs, w, nil)
	end := NewArgusNodeEndEmitter(obs, w, nil)
	lane := NewLane(LaneOptions{Concurrency: 16}, nil)
	ctx := WithLane(context.Background(), lane)

	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20; i++ {
		w.delay[fmt.Sprintf("n%02d", i)] = time.Duration(rng.Intn(20)) * time.Millisecond
	}
	var want []string
	for i := 0; i < 20; i++ {
		node := fmt.Sprintf("n%02d", i)
		if err := start.EmitNodeStart(ctx, startParams("r1", node)); err != nil {
			t.Fatal(err)
		}
		if err := end.EmitNodeEnd(ctx, endParams("r1", node)); err != nil {
			t.Fatal(err)
		}
		want = append(want, node+":"+event.TypeNodeStarted, node+":"+event.TypeNodeEnded)
	}
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := obs.nodes("r1")
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("published order\n got %v\nwant %v", got, want)
	}
	if w.peak < 2 {
		t.Fatalf("peak concurrent writes = %d; documents were not written concurrently", w.peak)
	}
}

func TestLanePublishesOnlyAfterTheDocumentIsWritten(t *testing.T) {
	w := newSlowWriter()
	w.delay["n1"] = 30 * time.Millisecond
	obs := &orderObserver{}
	end := NewArgusNodeEndEmitter(obs, w, nil)
	lane := NewLane(LaneOptions{}, nil)

	began := time.Now()
	if err := end.EmitNodeEnd(WithLane(context.Background(), lane), endParams("r1", "n1")); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(began); took > 15*time.Millisecond {
		t.Fatalf("EmitNodeEnd took %v with a lane; it waited for the write", took)
	}
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(obs.events) != 1 || obs.at[0].Before(w.written["n1.output"]) {
		t.Fatal("event published before its document was written")
	}
	out := endData(t, obs.events[0]).Output
	if out == nil || out.Document == nil || !strings.HasSuffix(out.Document.Path, "n1.output.json") {
		t.Fatalf("event payload = %+v, want the document reference", out)
	}
}

func TestLaneDoesNotHoldOneRunBehindAnother(t *testing.T) {
	w := newSlowWriter()
	w.delay["slow"] = 200 * time.Millisecond
	obs := &orderObserver{}
	end := NewArgusNodeEndEmitter(obs, w, nil)
	lane := NewLane(LaneOptions{}, nil)
	ctx := WithLane(context.Background(), lane)

	_ = end.EmitNodeEnd(ctx, endParams("slow-run", "slow"))
	_ = end.EmitNodeEnd(ctx, endParams("fast-run", "fast"))
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) && len(obs.nodes("fast-run")) == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	if len(obs.nodes("fast-run")) == 0 {
		t.Fatal("a fast run's event waited behind another run's slow write")
	}
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLaneStampsTheSubmissionTime(t *testing.T) {
	w := newSlowWriter()
	w.delay["n1"] = 50 * time.Millisecond
	obs := &orderObserver{}
	start := NewArgusNodeStartEmitter(obs, w, nil)
	lane := NewLane(LaneOptions{}, nil)

	submitted := time.Now()
	_ = start.EmitNodeStart(WithLane(context.Background(), lane), startParams("r1", "n1"))
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	e := obs.events[0]
	if d := e.Timestamp.Sub(submitted); d < 0 || d > 20*time.Millisecond {
		t.Fatalf("event timestamp is %v after submission; want the submission time", d)
	}
	if got := startData(t, e).StartedAt; got != e.Timestamp.UnixMilli() {
		t.Fatalf("StartedAt = %d, timestamp = %d", got, e.Timestamp.UnixMilli())
	}
}

func TestLaneSendsTheEventWhenTheWriteFails(t *testing.T) {
	w := newSlowWriter()
	w.fail["n1"] = true
	obs := &orderObserver{}
	end := NewArgusNodeEndEmitter(obs, w, nil)
	lane := NewLane(LaneOptions{}, nil)
	_ = end.EmitNodeEnd(WithLane(context.Background(), lane), endParams("r1", "n1"))
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(obs.events) != 1 {
		t.Fatalf("events = %d, want 1", len(obs.events))
	}
	if out := endData(t, obs.events[0]).Output; out == nil || out.Document != nil {
		t.Fatalf("output = %+v, want an empty payload", out)
	}
}

func TestLaneRunsInlineWhenFull(t *testing.T) {
	w := newSlowWriter()
	w.delay["n1"] = 40 * time.Millisecond
	w.delay["n2"] = 10 * time.Millisecond
	obs := &orderObserver{}
	start := NewArgusNodeStartEmitter(obs, w, nil)
	inline := 0
	lane := NewLane(LaneOptions{MaxQueuedBytes: 10, Hooks: LaneHooks{Inline: func() { inline++ }}}, nil)
	ctx := WithLane(context.Background(), lane)

	_ = start.EmitNodeStart(ctx, startParams("r1", "n1")) // 8 bytes: queued
	began := time.Now()
	_ = start.EmitNodeStart(ctx, startParams("r1", "n2")) // 8 more passes 10: inline
	if took := time.Since(began); took < 10*time.Millisecond {
		t.Fatalf("an inline submission returned after %v, before its own write", took)
	}
	if inline != 1 {
		t.Fatalf("inline = %d, want 1", inline)
	}
	if got := obs.nodes("r1"); strings.Join(got, ",") != "n1:"+event.TypeNodeStarted+",n2:"+event.TypeNodeStarted {
		t.Fatalf("order = %v; an inline submission overtook the run's queued event", got)
	}
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lane.QueuedBytes() != 0 {
		t.Fatalf("queued bytes after drain = %d", lane.QueuedBytes())
	}
}

func TestLaneDrainHonoursItsContext(t *testing.T) {
	w := newSlowWriter()
	w.delay["n1"] = time.Second
	end := NewArgusNodeEndEmitter(&orderObserver{}, w, nil)
	lane := NewLane(LaneOptions{}, nil)
	_ = end.EmitNodeEnd(WithLane(context.Background(), lane), endParams("r1", "n1"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lane.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "1 runs") {
		t.Fatalf("Drain = %v", err)
	}
}

func TestLaneWritesSurviveTheCallersCancellation(t *testing.T) {
	w := newSlowWriter()
	w.delay["n1"] = 20 * time.Millisecond
	obs := &orderObserver{}
	end := NewArgusNodeEndEmitter(obs, w, nil)
	lane := NewLane(LaneOptions{}, nil)
	ctx, cancel := context.WithCancel(WithLane(context.Background(), lane))
	_ = end.EmitNodeEnd(ctx, endParams("r1", "n1"))
	cancel() // the unit ends; its documents must still be written
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out := endData(t, obs.events[0]).Output; out == nil || out.Document == nil {
		t.Fatal("the document was abandoned when the caller's context ended")
	}
}

func TestEmittersStaySynchronousWithoutALane(t *testing.T) {
	w := newSlowWriter()
	w.delay["n1"] = 20 * time.Millisecond
	obs := &orderObserver{}
	end := NewArgusNodeEndEmitter(obs, w, nil)
	if err := end.EmitNodeEnd(context.Background(), endParams("r1", "n1")); err != nil {
		t.Fatal(err)
	}
	if len(obs.events) != 1 {
		t.Fatal("without a lane the event was not published before EmitNodeEnd returned")
	}
}

// A caller that builds its own events (a connector step) submits them directly; a document built
// on the lane costs the caller nothing, and its events keep their order with the run's others.
func TestLaneSubmitBuildsTheDocumentOnTheLane(t *testing.T) {
	w := newSlowWriter()
	obs := &orderObserver{}
	lane := NewLane(LaneOptions{}, nil)
	end := NewArgusNodeEndEmitter(obs, w, nil)
	ctx := WithLane(context.Background(), lane)

	built := make(chan struct{})
	send := func(node string) func(context.Context, *event.Payload) error {
		return func(ctx context.Context, p *event.Payload) error {
			e := event.New(event.TypeNodeEnded).WithClient("c").WithWorkflow("wf").WithRun("r1").WithNode(node).
				WithData(&event.EndNode{WorkflowID: "wf", RunID: "r1", ClientID: "c", NodeID: node, Output: p})
			return obs.Emit(ctx, e)
		}
	}
	began := time.Now()
	err := lane.Submit(ctx, Document{
		Writer: w,
		Target: DocumentTarget{WorkflowID: "wf", RunID: "r1", NodeID: "step", Direction: DirectionOutput},
		Build: func(context.Context) ([]byte, error) {
			time.Sleep(30 * time.Millisecond) // a read to assemble the document
			close(built)
			return []byte(`{"ok":true}`), nil
		},
		Send: send("step"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(began); took > 15*time.Millisecond {
		t.Fatalf("Submit took %v; the document was built on the caller", took)
	}
	_ = end.EmitNodeEnd(ctx, endParams("r1", "after"))
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-built
	if got := obs.nodes("r1"); strings.Join(got, ",") != "step:"+event.TypeNodeEnded+",after:"+event.TypeNodeEnded {
		t.Fatalf("order = %v", got)
	}
	if out := endData(t, obs.events[0]).Output; out == nil || out.Document == nil {
		t.Fatalf("submitted event has no document: %+v", out)
	}
}

func TestLaneSubmitSendsWithoutTheDocumentWhenBuildFails(t *testing.T) {
	obs := &orderObserver{}
	lane := NewLane(LaneOptions{}, nil)
	var got *event.Payload
	err := lane.Submit(context.Background(), Document{
		Writer: newSlowWriter(),
		Target: DocumentTarget{WorkflowID: "wf", RunID: "r1", NodeID: "step", Direction: DirectionInput},
		Build:  func(context.Context) ([]byte, error) { return nil, errors.New("archive unreadable") },
		Send: func(ctx context.Context, p *event.Payload) error {
			got = p
			return obs.Emit(ctx, event.New(event.TypeNodeStarted))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Document != nil {
		t.Fatalf("payload = %+v, want an empty one", got)
	}
	var none *event.Payload
	w := newSlowWriter()
	_ = lane.Submit(context.Background(), Document{
		Writer: w,
		Target: DocumentTarget{WorkflowID: "wf", RunID: "r2", NodeID: "step", Direction: DirectionInput},
		Build:  func(context.Context) ([]byte, error) { return nil, nil },
		Send:   func(_ context.Context, p *event.Payload) error { none = p; return nil },
	})
	if err := lane.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if none == nil || none.Document != nil || len(w.written) != 0 {
		t.Fatalf("no-document build: payload %+v, writes %d", none, len(w.written))
	}
	if err := lane.Submit(context.Background(), Document{}); err == nil {
		t.Fatal("Submit accepted a document with nothing to write")
	}
}
