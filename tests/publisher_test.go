package tests

import (
	"context"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/wehubfusion/Argus/internal/nats"
	"go.uber.org/zap"
)

func TestNewPublisher_Success(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if pub == nil {
		t.Fatal("Expected publisher to be created")
	}

	// Verify stream was created
	streams := mockJS.GetStreams()
	if _, exists := streams["TEST_STREAM"]; !exists {
		t.Error("Expected stream to be created")
	}
}

func TestNewPublisher_NilJetStream(t *testing.T) {
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(nil, config, logger)
	if err == nil {
		t.Fatal("Expected error for nil JetStream context")
	}
	if pub != nil {
		t.Fatal("Expected publisher to be nil")
	}
}

func TestNewPublisher_StreamAlreadyExists(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	// Create stream first
	streamConfig := &natsclient.StreamConfig{
		Name:     "EXISTING_STREAM",
		Subjects: []string{"OBSERVE.>"},
		Storage:  natsclient.FileStorage,
		MaxAge:   24 * time.Hour,
		MaxMsgs:  1000,
		Replicas: 1,
	}
	_, err := mockJS.AddStream(streamConfig)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Create publisher with same stream name
	config := nats.PublisherConfig{
		StreamName:     "EXISTING_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Expected no error when stream exists, got: %v", err)
	}
	if pub == nil {
		t.Fatal("Expected publisher to be created")
	}
}

func TestPublisher_Publish_Success(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	subject := "OBSERVE.RUN.STARTED"
	msgID := "msg_123"
	data := []byte(`{"type":"run.started","id":"msg_123"}`)

	err = pub.Publish(context.Background(), subject, msgID, data)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify message was published
	messages := mockJS.GetPublishedMessages()
	if len(messages) == 0 {
		t.Error("Expected message to be published")
	}
	if len(messages) > 0 {
		if messages[0].Subject != subject {
			t.Errorf("Expected subject %s, got %s", subject, messages[0].Subject)
		}
		if string(messages[0].Data) != string(data) {
			t.Errorf("Expected data %s, got %s", string(data), string(messages[0].Data))
		}
	}
}

func TestPublisher_Publish_VerifyAck(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	subject := "OBSERVE.RUN.STARTED"
	msgID := "msg_456"
	data := []byte(`{"type":"run.started","id":"msg_456"}`)

	err = pub.Publish(context.Background(), subject, msgID, data)
	if err != nil {
		t.Fatalf("Expected no error on successful publish with ack, got: %v", err)
	}

	// Verify PubAck was received and processed
	// The mock returns a PubAck with Stream="MOCK" and Sequence=1
	// If the code properly handles the ack, it should succeed
	messages := mockJS.GetPublishedMessages()
	if len(messages) != 1 {
		t.Errorf("Expected 1 published message, got %d", len(messages))
	}
}

func TestPublisher_Publish_EmptySubject(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	err = pub.Publish(context.Background(), "", "msg_123", []byte("data"))
	if err == nil {
		t.Fatal("Expected error for empty subject")
	}
}

func TestPublisher_Publish_EmptyMsgID(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	err = pub.Publish(context.Background(), "OBSERVE.RUN.STARTED", "", []byte("data"))
	if err == nil {
		t.Fatal("Expected error for empty msgID")
	}
}

func TestPublisher_Publish_EmptyData(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	err = pub.Publish(context.Background(), "OBSERVE.RUN.STARTED", "msg_123", nil)
	if err == nil {
		t.Fatal("Expected error for empty data")
	}
}

func TestPublisher_Publish_ContextTimeout(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "TEST_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 1 * time.Nanosecond, // Very short timeout
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	// Create context that will timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait to ensure timeout
	time.Sleep(10 * time.Millisecond)

	err = pub.Publish(ctx, "OBSERVE.RUN.STARTED", "msg_123", []byte("data"))
	if err == nil {
		t.Error("Expected error for context timeout")
	}
}

func TestPublisher_EnsureStream_CreatesStream(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	config := nats.PublisherConfig{
		StreamName:     "NEW_STREAM",
		StreamMaxAge:   24 * time.Hour,
		StreamMaxMsgs:  1000,
		PublishTimeout: 5 * time.Second,
	}

	_, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	// Verify stream was created
	streams := mockJS.GetStreams()
	streamInfo, exists := streams["NEW_STREAM"]
	if !exists {
		t.Fatal("Expected stream to be created")
	}

	// Verify stream configuration
	if streamInfo.Config.Name != "NEW_STREAM" {
		t.Errorf("Expected stream name 'NEW_STREAM', got %s", streamInfo.Config.Name)
	}
	if streamInfo.Config.MaxAge != 24*time.Hour {
		t.Errorf("Expected max age 24h, got %v", streamInfo.Config.MaxAge)
	}
	if streamInfo.Config.MaxMsgs != 1000 {
		t.Errorf("Expected max msgs 1000, got %d", streamInfo.Config.MaxMsgs)
	}
}

func TestPublisher_EnsureStream_UsesExistingStream(t *testing.T) {
	mockJS := NewMockJetStream()
	logger := zap.NewNop()

	// Create stream first
	streamConfig := &natsclient.StreamConfig{
		Name:     "EXISTING_STREAM",
		Subjects: []string{"OBSERVE.>"},
		Storage:  natsclient.FileStorage,
		MaxAge:   24 * time.Hour,
		MaxMsgs:  1000,
		Replicas: 1,
	}
	_, err := mockJS.AddStream(streamConfig)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Create publisher - should use existing stream
	config := nats.PublisherConfig{
		StreamName:     "EXISTING_STREAM",
		StreamMaxAge:   48 * time.Hour, // Different config
		StreamMaxMsgs:  2000,           // Different config
		PublishTimeout: 5 * time.Second,
	}

	pub, err := nats.NewPublisher(mockJS, config, logger)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if pub == nil {
		t.Fatal("Expected publisher to be created")
	}

	// Verify stream still exists (wasn't recreated)
	streams := mockJS.GetStreams()
	if _, exists := streams["EXISTING_STREAM"]; !exists {
		t.Fatal("Expected stream to still exist")
	}
}

// The observation stream gets a size cap that drops its oldest events: a new stream is created
// with it, an existing stream without it is updated once, and a stream already capped is left
// alone (UAT incident 08/10/2026, C2).
func TestPublisherCapsTheStreamSize(t *testing.T) {
	logger := zap.NewNop()
	config := nats.PublisherConfig{StreamName: "OBS", StreamMaxAge: time.Hour, StreamMaxMsgs: 10, StreamMaxBytes: 4 << 30, PublishTimeout: time.Second}

	fresh := NewMockJetStream()
	if _, err := nats.NewPublisher(fresh, config, logger); err != nil {
		t.Fatal(err)
	}
	if cfg := fresh.GetStreams()["OBS"].Config; cfg.MaxBytes != 4<<30 || cfg.Discard != natsclient.DiscardOld {
		t.Fatalf("new stream: max_bytes %d discard %v", cfg.MaxBytes, cfg.Discard)
	}

	old := NewMockJetStream()
	if _, err := old.AddStream(&natsclient.StreamConfig{Name: "OBS", Subjects: []string{"OBSERVE.>"}, MaxAge: 30 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := nats.NewPublisher(old, config, logger); err != nil {
		t.Fatal(err)
	}
	cfg := old.GetStreams()["OBS"].Config
	if old.Updates() != 1 || cfg.MaxBytes != 4<<30 || cfg.Discard != natsclient.DiscardOld || cfg.MaxAge != 30*24*time.Hour {
		t.Fatalf("existing stream: updates %d, max_bytes %d, discard %v, max_age %v (other settings must stay)", old.Updates(), cfg.MaxBytes, cfg.Discard, cfg.MaxAge)
	}
	if _, err := nats.NewPublisher(old, config, logger); err != nil {
		t.Fatal(err)
	}
	if old.Updates() != 1 {
		t.Fatalf("an already capped stream was updated again (%d updates)", old.Updates())
	}
}
