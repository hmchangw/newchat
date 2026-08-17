package roomkeysender

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
)

// Publisher abstracts NATS publishing so the sender is testable.
// *nats.Conn satisfies this interface directly.
type Publisher interface {
	Publish(subject string, data []byte) error
}

// Sender publishes room key events to user NATS subjects.
type Sender struct {
	pub     Publisher
	metrics natsmetrics.Publisher
}

// Option configures an optional Sender behavior.
type Option func(*Sender)

// WithMetrics records a bounded publish attempt per send. Without it the
// zero-value natsmetrics.Publisher is inert, so an un-instrumented caller keeps
// its current behavior.
func WithMetrics(metrics natsmetrics.Publisher) Option {
	return func(s *Sender) { s.metrics = metrics }
}

// NewSender creates a Sender backed by the given publisher.
func NewSender(pub Publisher, opts ...Option) *Sender {
	s := &Sender{pub: pub}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Marshal stamps the event Timestamp and serializes it once into a payload that
// can be fanned out to many accounts via SendData without re-marshaling per
// recipient. The event is accepted by value; Marshal must not mutate the
// caller's struct.
//
//nolint:gocritic // hugeParam: by-value is intentional for immutability; the copy cost is acceptable.
func (s *Sender) Marshal(evt model.RoomKeyEvent) ([]byte, error) {
	evt.Timestamp = time.Now().UTC().UnixMilli()
	// #nosec G117 -- RoomKeyEvent.PrivateKey is the intended payload: room-key distribution to the authorized account over its auth-callout-gated per-user subject, not a leak
	data, err := json.Marshal(evt)
	if err != nil {
		return nil, fmt.Errorf("marshal room key event: %w", err)
	}
	return data, nil
}

// SendData publishes an already-marshaled payload (from Marshal) to the room key
// update subject for the given user account.
func (s *Sender) SendData(account string, data []byte) error {
	return s.SendDataContext(context.Background(), account, data)
}

// SendDataContext publishes an already-marshaled payload and correlates the
// publish metric with the caller's context.
func (s *Sender) SendDataContext(ctx context.Context, account string, data []byte) error {
	subj := subject.RoomKeyUpdate(account)
	err := s.pub.Publish(subj, data)
	destination, operation := natsmetrics.PublishLabelsFromSubject(subj)
	s.metrics.Attempt(ctx, destination, operation, err)
	if err != nil {
		return fmt.Errorf("publish room key event: %w", err)
	}
	return nil
}

// Send publishes evt to the room key update subject for the given user account.
// The event is accepted by value; Send stamps its own Timestamp before publishing.
// The value copy is intentional: Send must not mutate the caller's struct.
//
//nolint:gocritic // hugeParam: by-value is intentional for immutability; the copy cost is acceptable.
func (s *Sender) Send(account string, evt model.RoomKeyEvent) error {
	data, err := s.Marshal(evt)
	if err != nil {
		return err
	}
	return s.SendData(account, data)
}
