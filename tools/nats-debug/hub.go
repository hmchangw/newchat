package main

import "time"

// Message represents a NATS message received on a subscribed subject.
type Message struct {
	ID        string    `json:"id"`
	Subject   string    `json:"subject"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// Subscription represents an active NATS subscription on the dest server.
type Subscription struct {
	ID      string `json:"id"`
	Subject string `json:"subject"`
}

// DebugHeaders are the optional X-Debug headers stamped on an outbound message.
// The zero value emits no headers, leaving the message byte-identical to a plain
// publish/request.
type DebugHeaders struct {
	// Level is the canonical X-Debug token ("" | "flow" | "debug" | "trace").
	// Empty emits no X-Debug header.
	Level string
	// Payload sets X-Debug-Payload: 1 when true.
	Payload bool
}

// ConnectionStatus holds the current connection state for all three NATS connections.
type ConnectionStatus struct {
	SourceConnected  bool   `json:"sourceConnected"`
	DestConnected    bool   `json:"destConnected"`
	RequestConnected bool   `json:"requestConnected"`
	SourceURL        string `json:"sourceURL,omitempty"`
	DestURL          string `json:"destURL,omitempty"`
	RequestURL       string `json:"requestURL,omitempty"`
}

// Hub manages connections to NATS servers, subscriptions, publishing, SSE fan-out,
// and request/reply interactions.
//
//go:generate mockgen -destination=mock_hub_test.go -package=main . Hub
type Hub interface {
	// Connect establishes connections to the source and dest NATS servers. It
	// disconnects any existing source/dest connections first.
	Connect(sourceURL, destURL string) error

	// Disconnect closes the source and dest NATS connections and removes all subscriptions.
	Disconnect()

	// Subscribe adds a NATS subscription on the dest server. Returns the new
	// Subscription with its assigned ID.
	Subscribe(subject string) (Subscription, error)

	// Unsubscribe removes a subscription by ID. Returns an error if not found.
	Unsubscribe(id string) error

	// Publish sends a message payload to the given subject on the source server,
	// stamping the optional X-Debug headers carried by dbg.
	Publish(subject, payload string, dbg DebugHeaders) error

	// Status returns the current connection status for all three servers.
	Status() ConnectionStatus

	// Subscriptions returns a snapshot of all active subscriptions.
	Subscriptions() []Subscription

	// RegisterSSEClient adds a channel that will receive all incoming messages.
	// Returns a unique client ID used to unregister later.
	RegisterSSEClient(ch chan<- Message) string

	// UnregisterSSEClient removes the SSE client with the given ID.
	UnregisterSSEClient(id string)

	// ConnectRequest establishes a NATS connection used exclusively for request/reply.
	// Closes any existing request connection first.
	ConnectRequest(url string) error

	// DisconnectRequest closes the request/reply NATS connection.
	DisconnectRequest()

	// Request sends a NATS request on the given subject and waits up to timeoutMs
	// milliseconds for a reply, stamping the optional X-Debug headers carried by
	// dbg. Returns the reply payload as a string.
	Request(subject, payload string, timeoutMs int, dbg DebugHeaders) (string, error)
}
