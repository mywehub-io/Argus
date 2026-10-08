package tests

import (
	"reflect"
	"sync"

	nats "github.com/nats-io/nats.go"
)

// PublishedMessage represents a message that was published to the mock
type PublishedMessage struct {
	Subject string
	Data    []byte
	MsgID   string
}

// MockJetStream is a mock implementation of nats.JetStreamContext for unit testing
type MockJetStream struct {
	mu            sync.Mutex
	streams       map[string]*nats.StreamInfo
	publishedMsgs []PublishedMessage
	publishErr    error // if non-nil, Publish returns this error
	updates       int   // UpdateStream calls
}

// SetPublishError configures the mock to return err from future Publish calls.
// Pass nil to reset to normal (success) behaviour.
func (m *MockJetStream) SetPublishError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishErr = err
}

// NewMockJetStream creates a new mock JetStream context
func NewMockJetStream() *MockJetStream {
	return &MockJetStream{
		streams:       make(map[string]*nats.StreamInfo),
		publishedMsgs: make([]PublishedMessage, 0),
	}
}

// StreamInfo returns stream information or ErrStreamNotFound
func (m *MockJetStream) StreamInfo(streamName string, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if info, exists := m.streams[streamName]; exists {
		return info, nil
	}
	return nil, nats.ErrStreamNotFound
}

// UpdateStream replaces an existing stream's config and counts the call.
func (m *MockJetStream) UpdateStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.streams[cfg.Name]
	if !exists {
		return nil, nats.ErrStreamNotFound
	}
	info.Config = *cfg
	m.updates++
	return info, nil
}

// Updates is how many times UpdateStream was called.
func (m *MockJetStream) Updates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updates
}

// AddStream creates a new stream
func (m *MockJetStream) AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	info := &nats.StreamInfo{
		Config: *cfg,
		State: nats.StreamState{
			Msgs:      0,
			Bytes:     0,
			FirstSeq:  1,
			LastSeq:   0,
			Consumers: 0,
		},
	}
	m.streams[cfg.Name] = info
	return info, nil
}

// Publish publishes a message and captures it for verification
func (m *MockJetStream) Publish(subject string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.publishErr != nil {
		return nil, m.publishErr
	}

	// Extract MsgId from options using reflection
	// The nats.MsgId() function returns a PubOpt that is a function
	// We need to extract the captured value from the closure
	msgID := extractMsgIDFromOpts(opts)
	
	msg := PublishedMessage{
		Subject: subject,
		Data:    data,
		MsgID:   msgID,
	}
	m.publishedMsgs = append(m.publishedMsgs, msg)
	
	return &nats.PubAck{
		Stream:   "MOCK",
		Sequence: uint64(len(m.publishedMsgs)),
	}, nil
}

// extractMsgIDFromOpts attempts to extract MsgId from PubOpts using reflection
func extractMsgIDFromOpts(opts []nats.PubOpt) string {
	for _, opt := range opts {
		optValue := reflect.ValueOf(opt)
		if optValue.Kind() != reflect.Func {
			continue
		}
		
		// The nats.MsgId function creates a closure that captures the message ID
		// We can try to extract it by inspecting the function's closure
		// This is a workaround since we can't directly access closure variables
		
		// Try to call the function with a mock PubOptBuilder to extract the value
		// Since we can't easily do this, we'll use a different approach:
		// We'll create a test helper that wraps the publish and tracks MsgId separately
		// For now, we'll return empty and verify messages by their content/order
		
		// Alternative: Use a custom PubOpt that we can inspect
		// But since the real code uses nats.MsgId(), we need to work with that
	}
	
	// Since extracting MsgId from the closure is difficult, we'll verify
	// messages by checking their order and content in tests
	return ""
}

// GetPublishedMessages returns all published messages
func (m *MockJetStream) GetPublishedMessages() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	result := make([]PublishedMessage, len(m.publishedMsgs))
	copy(result, m.publishedMsgs)
	return result
}

// ClearPublishedMessages clears the published messages list
func (m *MockJetStream) ClearPublishedMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedMsgs = make([]PublishedMessage, 0)
}

// GetStreams returns all created streams
func (m *MockJetStream) GetStreams() map[string]*nats.StreamInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	result := make(map[string]*nats.StreamInfo)
	for k, v := range m.streams {
		result[k] = v
	}
	return result
}

// AccountInfo is required by nats.JetStreamContext interface
func (m *MockJetStream) AccountInfo(opts ...nats.JSOpt) (*nats.AccountInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	return &nats.AccountInfo{
		Tier: nats.Tier{
			Memory:    0,
			Store:     0,
			Streams:   int(len(m.streams)),
			Consumers: 0,
		},
		API: nats.APIStats{
			Total:  0,
			Errors: 0,
		},
	}, nil
}

// ConsumerInfo is required by nats.JetStreamContext interface
func (m *MockJetStream) ConsumerInfo(stream, consumer string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, nats.ErrConsumerNotFound
}

// AddConsumer is required by nats.JetStreamContext interface
func (m *MockJetStream) AddConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, nats.ErrStreamNotFound
}

// DeleteConsumer is required by nats.JetStreamContext interface
func (m *MockJetStream) DeleteConsumer(stream, consumer string, opts ...nats.JSOpt) error {
	return nats.ErrConsumerNotFound
}

// ConsumerNames is required by nats.JetStreamContext interface
func (m *MockJetStream) ConsumerNames(stream string, opts ...nats.JSOpt) <-chan string {
	ch := make(chan string)
	close(ch)
	return ch
}

// Consumers is required by nats.JetStreamContext interface
func (m *MockJetStream) Consumers(stream string, opts ...nats.JSOpt) <-chan *nats.ConsumerInfo {
	ch := make(chan *nats.ConsumerInfo)
	close(ch)
	return ch
}

// ConsumersInfo is required by nats.JetStreamContext interface
func (m *MockJetStream) ConsumersInfo(stream string, opts ...nats.JSOpt) <-chan *nats.ConsumerInfo {
	ch := make(chan *nats.ConsumerInfo)
	close(ch)
	return ch
}

// ConsumerInfoContext is required by nats.JetStreamContext interface
func (m *MockJetStream) ConsumerInfoContext(ctx nats.JetStreamContext, stream, consumer string, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, nats.ErrConsumerNotFound
}

// Streams is required by nats.JetStreamContext interface
func (m *MockJetStream) Streams(opts ...nats.JSOpt) <-chan *nats.StreamInfo {
	ch := make(chan *nats.StreamInfo)
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, info := range m.streams {
			ch <- info
		}
		close(ch)
	}()
	return ch
}

// StreamNames is required by nats.JetStreamContext interface
func (m *MockJetStream) StreamNames(opts ...nats.JSOpt) <-chan string {
	ch := make(chan string)
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for name := range m.streams {
			ch <- name
		}
		close(ch)
	}()
	return ch
}

// StreamInfoContext is required by nats.JetStreamContext interface
func (m *MockJetStream) StreamInfoContext(ctx nats.JetStreamContext, stream string, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	return m.StreamInfo(stream, opts...)
}

// StreamNameBySubject is required by nats.JetStreamContext interface
func (m *MockJetStream) StreamNameBySubject(subject string, opts ...nats.JSOpt) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Try to find a stream that matches the subject pattern
	for name, info := range m.streams {
		for _, subjPattern := range info.Config.Subjects {
			// Simple pattern matching - in real implementation this would be more sophisticated
			if subjPattern == subject || (len(subjPattern) > 0 && subjPattern[len(subjPattern)-1] == '>' && len(subject) >= len(subjPattern)-1 && subject[:len(subjPattern)-1] == subjPattern[:len(subjPattern)-1]) {
				return name, nil
			}
		}
	}
	return "", nats.ErrStreamNotFound
}

// GetMsg is required by nats.JetStreamContext interface
func (m *MockJetStream) GetMsg(name string, seq uint64, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	return nil, nats.ErrMsgNotFound
}

// GetLastMsg is required by nats.JetStreamContext interface
func (m *MockJetStream) GetLastMsg(name, subject string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	return nil, nats.ErrMsgNotFound
}

// DeleteMsg is required by nats.JetStreamContext interface
func (m *MockJetStream) DeleteMsg(name string, seq uint64, opts ...nats.JSOpt) error {
	return nil
}

// SecureDeleteMsg is required by nats.JetStreamContext interface
func (m *MockJetStream) SecureDeleteMsg(name string, seq uint64, opts ...nats.JSOpt) error {
	return nil
}

// UpdateConsumer is required by nats.JetStreamContext interface
func (m *MockJetStream) UpdateConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, nats.ErrConsumerNotFound
}

// StreamsInfo is required by nats.JetStreamContext interface
func (m *MockJetStream) StreamsInfo(opts ...nats.JSOpt) <-chan *nats.StreamInfo {
	return m.Streams(opts...)
}

// KeyValueStoreNames is required by nats.JetStreamContext interface
func (m *MockJetStream) KeyValueStoreNames() <-chan string {
	ch := make(chan string)
	close(ch)
	return ch
}

// KeyValueStores is required by nats.JetStreamContext interface
func (m *MockJetStream) KeyValueStores() <-chan nats.KeyValueStatus {
	ch := make(chan nats.KeyValueStatus)
	close(ch)
	return ch
}

// ObjectStoreNames is required by nats.JetStreamContext interface
func (m *MockJetStream) ObjectStoreNames(opts ...nats.ObjectOpt) <-chan string {
	ch := make(chan string)
	close(ch)
	return ch
}

// ObjectStores is required by nats.JetStreamContext interface
func (m *MockJetStream) ObjectStores(opts ...nats.ObjectOpt) <-chan nats.ObjectStoreStatus {
	ch := make(chan nats.ObjectStoreStatus)
	close(ch)
	return ch
}

// DeleteStream is required by nats.JetStreamContext interface
func (m *MockJetStream) DeleteStream(name string, opts ...nats.JSOpt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.streams, name)
	return nil
}

// PurgeStream is required by nats.JetStreamContext interface
func (m *MockJetStream) PurgeStream(name string, opts ...nats.JSOpt) error {
	return nil
}

// PublishAsync is required by nats.JetStreamContext interface
func (m *MockJetStream) PublishAsync(subject string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	ack, err := m.Publish(subject, data, opts...)
	if err != nil {
		return nil, err
	}
	return &mockPubAckFuture{ack: ack}, nil
}

// PublishMsg is required by nats.JetStreamContext interface
func (m *MockJetStream) PublishMsg(msg *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return m.Publish(msg.Subject, msg.Data, opts...)
}

// PublishMsgAsync is required by nats.JetStreamContext interface
func (m *MockJetStream) PublishMsgAsync(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return m.PublishAsync(msg.Subject, msg.Data, opts...)
}

// PublishAsyncPending is required by nats.JetStreamContext interface
func (m *MockJetStream) PublishAsyncPending() int {
	return 0
}

// PublishAsyncComplete is required by nats.JetStreamContext interface
func (m *MockJetStream) PublishAsyncComplete() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Subscribe is required by nats.JetStreamContext interface
func (m *MockJetStream) Subscribe(subject string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// SubscribeSync is required by nats.JetStreamContext interface
func (m *MockJetStream) SubscribeSync(subject string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// ChanSubscribe is required by nats.JetStreamContext interface
func (m *MockJetStream) ChanSubscribe(subject string, ch chan *nats.Msg, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// ChanQueueSubscribe is required by nats.JetStreamContext interface
func (m *MockJetStream) ChanQueueSubscribe(subject, queue string, ch chan *nats.Msg, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// QueueSubscribe is required by nats.JetStreamContext interface
func (m *MockJetStream) QueueSubscribe(subject, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// QueueSubscribeSync is required by nats.JetStreamContext interface
func (m *MockJetStream) QueueSubscribeSync(subject, queue string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// PullSubscribe is required by nats.JetStreamContext interface
func (m *MockJetStream) PullSubscribe(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return nil, nil
}

// KeyValue is required by nats.JetStreamContext interface
func (m *MockJetStream) KeyValue(bucket string) (nats.KeyValue, error) {
	return nil, nil
}

// CreateKeyValue is required by nats.JetStreamContext interface
func (m *MockJetStream) CreateKeyValue(cfg *nats.KeyValueConfig) (nats.KeyValue, error) {
	return nil, nil
}

// DeleteKeyValue is required by nats.JetStreamContext interface
func (m *MockJetStream) DeleteKeyValue(bucket string) error {
	return nil
}

// ObjectStore is required by nats.JetStreamContext interface
func (m *MockJetStream) ObjectStore(bucket string) (nats.ObjectStore, error) {
	return nil, nil
}

// CreateObjectStore is required by nats.JetStreamContext interface
func (m *MockJetStream) CreateObjectStore(cfg *nats.ObjectStoreConfig) (nats.ObjectStore, error) {
	return nil, nil
}

// DeleteObjectStore is required by nats.JetStreamContext interface
func (m *MockJetStream) DeleteObjectStore(bucket string) error {
	return nil
}

type mockPubAckFuture struct {
	ack *nats.PubAck
	err error
}

func (f *mockPubAckFuture) Ok() <-chan *nats.PubAck {
	ch := make(chan *nats.PubAck, 1)
	ch <- f.ack
	close(ch)
	return ch
}

func (f *mockPubAckFuture) Err() <-chan error {
	ch := make(chan error, 1)
	if f.err != nil {
		ch <- f.err
	}
	close(ch)
	return ch
}

func (f *mockPubAckFuture) Msg() *nats.Msg {
	return nil
}

// CleanupPublisher is required by nats.JetStream interface
func (m *MockJetStream) CleanupPublisher() {
	// No-op for mock
}
