package observer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wehubfusion/Argus/pkg/event"
	"go.uber.org/zap"
)

type fakePublisher struct {
	mu    sync.Mutex
	gate  chan struct{} // when set, Publish waits on it
	ids   []string
	delay time.Duration
}

func (f *fakePublisher) Publish(ctx context.Context, subject, msgID string, data []byte) error {
	if f.gate != nil {
		<-f.gate
	}
	time.Sleep(f.delay)
	f.mu.Lock()
	f.ids = append(f.ids, msgID)
	f.mu.Unlock()
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ids)
}

func newTestObserver(p publisher, opts Options) *observer {
	o := &observer{publisher: p, options: opts, logger: zap.NewNop()}
	if opts.AsyncQueueSize > 0 {
		o.async = newAsyncQueue(o, opts)
	}
	return o
}

func nodeEvent(node string) *event.Event {
	return event.New(event.TypeNodeStarted).WithClient("c").WithWorkflow("w").WithRun("r").WithNode(node)
}

// A stalled broker never slows Emit; events queued while it stalls arrive once it resumes.
func TestAsyncEmitNeverBlocks(t *testing.T) {
	p := &fakePublisher{gate: make(chan struct{})}
	o := newTestObserver(p, DefaultOptions().WithAsync(100))
	start := time.Now()
	for i := 0; i < 50; i++ {
		if err := o.Emit(context.Background(), nodeEvent(string(rune('a'+i%26))+string(rune('a'+i/26)))); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("50 emits against a stalled broker took %v", d)
	}
	close(p.gate)
	if err := o.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.count() != 50 {
		t.Fatalf("published %d of 50 after resume", p.count())
	}
}

// A full queue drops and counts; Emit still returns at once.
func TestAsyncFullQueueDrops(t *testing.T) {
	p := &fakePublisher{gate: make(chan struct{})}
	opts := DefaultOptions().WithAsync(2)
	opts.AsyncWorkers = 1
	o := newTestObserver(p, opts)
	for i := 0; i < 10; i++ {
		_ = o.Emit(context.Background(), nodeEvent(string(rune('a'+i))))
	}
	st := StatsOf(o)
	// One event is held by the worker, two wait in the queue, the rest are dropped.
	if st.Dropped < 6 || st.Dropped > 7 {
		t.Fatalf("dropped %d, want 6 or 7", st.Dropped)
	}
	close(p.gate)
	_ = o.Close(context.Background())
	if got := p.count() + int(StatsOf(o).Dropped); got != 10 {
		t.Fatalf("published + dropped = %d, want 10", got)
	}
}

// Close returns the context error when the queue cannot drain in time, and never hangs.
func TestAsyncCloseHonoursItsDeadline(t *testing.T) {
	p := &fakePublisher{gate: make(chan struct{})}
	o := newTestObserver(p, DefaultOptions().WithAsync(10))
	_ = o.Emit(context.Background(), nodeEvent("n"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := o.Close(ctx); err == nil {
		t.Fatal("want a deadline error with a stalled broker")
	}
	close(p.gate)
	if err := o.Emit(context.Background(), nodeEvent("late")); err != ErrObserverClosed {
		t.Fatalf("emit after close: %v", err)
	}
}

// The publish id is derived from the content: the same event twice gives one id, a different
// outcome another, and an id the caller sets is kept.
func TestEmitIDs(t *testing.T) {
	p := &fakePublisher{}
	o := newTestObserver(p, DefaultOptions())
	a := nodeEvent("n").WithData(map[string]string{"status": "failed"})
	b := nodeEvent("n").WithData(map[string]string{"status": "failed"})
	c := nodeEvent("n").WithData(map[string]string{"status": "success"})
	d := nodeEvent("n")
	d.ID = "caller-id"
	for _, e := range []*event.Event{a, b, c, d} {
		if err := o.Emit(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	if p.ids[0] != p.ids[1] {
		t.Fatal("the same event must publish under the same id")
	}
	if p.ids[0] == p.ids[2] {
		t.Fatal("a different outcome must publish under a different id")
	}
	if p.ids[3] != "caller-id" {
		t.Fatalf("a caller id must be kept, got %q", p.ids[3])
	}
}
