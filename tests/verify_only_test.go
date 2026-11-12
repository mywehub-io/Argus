package tests

import (
	"context"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/wehubfusion/Argus/internal/nats"
	"github.com/wehubfusion/Argus/pkg/observer"
	"go.uber.org/zap"
)

func verifyOnlyConfig() nats.PublisherConfig {
	return nats.PublisherConfig{
		StreamName:     "OBSERVATION",
		StreamMaxAge:   time.Hour,
		StreamMaxMsgs:  1000,
		StreamMaxBytes: 8 << 20,
		PublishTimeout: time.Second,
		VerifyOnly:     true,
	}
}

// A verify-only publisher starts without the stream, never creates it, refuses events while it
// is missing, and publishes once the platform has created it.
func TestVerifyOnly_MissingStreamWaitsForPlatform(t *testing.T) {
	js := NewMockJetStream()
	js.RequireStream()

	pub, err := nats.NewPublisher(js, verifyOnlyConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("NewPublisher refused to start without the stream: %v", err)
	}
	if len(js.GetStreams()) != 0 {
		t.Fatal("a verify-only publisher created the stream at start")
	}

	ctx := context.Background()
	if err := pub.Publish(ctx, "OBSERVE.WORKFLOW.NODE.ENDED.r1", "m1", []byte("{}")); err == nil {
		t.Fatal("published with no stream")
	}
	if len(js.GetStreams()) != 0 {
		t.Fatal("a verify-only publisher created the stream on a failed publish")
	}

	// The platform creates the stream.
	if _, err := js.AddStream(&natsclient.StreamConfig{Name: "OBSERVATION", Subjects: []string{"OBSERVE.>"}}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, "OBSERVE.WORKFLOW.NODE.ENDED.r1", "m2", []byte("{}")); err != nil {
		t.Fatalf("publish after the platform created the stream: %v", err)
	}
}

// A verify-only publisher leaves an existing stream's size cap as it is.
func TestVerifyOnly_ExistingStreamIsNotUpdated(t *testing.T) {
	js := NewMockJetStream()
	if _, err := js.AddStream(&natsclient.StreamConfig{Name: "OBSERVATION", Subjects: []string{"OBSERVE.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := nats.NewPublisher(js, verifyOnlyConfig(), zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if js.Updates() != 0 {
		t.Fatalf("UpdateStream called %d times", js.Updates())
	}
}

// Off, the default, still creates and caps.
func TestVerifyOnly_OffStillCreatesAndCaps(t *testing.T) {
	js := NewMockJetStream()
	cfg := verifyOnlyConfig()
	cfg.VerifyOnly = false
	if _, err := nats.NewPublisher(js, cfg, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if _, ok := js.GetStreams()["OBSERVATION"]; !ok {
		t.Fatal("stream not created")
	}

	capped := NewMockJetStream()
	if _, err := capped.AddStream(&natsclient.StreamConfig{Name: "OBSERVATION", Subjects: []string{"OBSERVE.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := nats.NewPublisher(capped, cfg, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if capped.Updates() != 1 {
		t.Fatalf("UpdateStream called %d times, want 1", capped.Updates())
	}
}

// The observer option reaches the publisher: NewObserver starts without the stream.
func TestVerifyOnly_ObserverOption(t *testing.T) {
	js := NewMockJetStream()
	obs, err := observer.NewObserver(js, observer.DefaultOptions().WithVerifyOnly(true), zap.NewNop())
	if err != nil {
		t.Fatalf("NewObserver: %v", err)
	}
	defer obs.Close(context.Background())
	if len(js.GetStreams()) != 0 {
		t.Fatal("a verify-only observer created the stream")
	}
}
