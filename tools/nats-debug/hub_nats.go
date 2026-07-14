package main

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// debugHeader builds the NATS header carrying the optional X-Debug intent for an
// outbound message. It returns nil when nothing is requested, so the message is
// byte-identical to a plain publish/request. Header names are reused from
// pkg/natsutil to stay bound to the same wire contract as the services.
func debugHeader(dbg DebugHeaders) nats.Header {
	h := nats.Header{}
	if dbg.Level != "" {
		h.Set(natsutil.DebugHeader, dbg.Level)
	}
	if dbg.Payload {
		h.Set(natsutil.DebugPayloadHeader, "1")
	}
	if len(h) == 0 {
		return nil
	}
	return h
}

type natsSub struct {
	id      string
	subject string
	sub     *nats.Subscription
}

type natsHub struct {
	mu          sync.RWMutex
	sourceConn  *nats.Conn
	destConn    *nats.Conn
	requestConn *nats.Conn
	sourceURL   string
	destURL     string
	requestURL  string
	credsFile   string
	subs        map[string]*natsSub
	clients     map[string]chan<- Message
}

// newNATSHub builds a hub. When credsFile is non-empty it is applied as the
// user credentials (JWT + NKey) on every NATS connection the hub opens.
func newNATSHub(credsFile string) *natsHub {
	return &natsHub{
		credsFile: credsFile,
		subs:      make(map[string]*natsSub),
		clients:   make(map[string]chan<- Message),
	}
}

// buildConnectOptions returns the connection options for a debug connection
// named name. The debug tool never auto-reconnects (MaxReconnects(0)). When
// credsFile is non-empty the connection authenticates with those credentials.
func buildConnectOptions(name, credsFile string) []nats.Option {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(0),
	}
	if credsFile != "" {
		opts = append(opts, nats.UserCredentials(credsFile))
	}
	return opts
}

func (h *natsHub) Connect(sourceURL, destURL string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.disconnectLocked()

	srcConn, err := nats.Connect(sourceURL, buildConnectOptions("nats-debug-source", h.credsFile)...)
	if err != nil {
		return fmt.Errorf("connect to source NATS %s: %w", sourceURL, err)
	}

	dstConn, err := nats.Connect(destURL, buildConnectOptions("nats-debug-dest", h.credsFile)...)
	if err != nil {
		srcConn.Close()
		return fmt.Errorf("connect to dest NATS %s: %w", destURL, err)
	}

	h.sourceConn = srcConn
	h.destConn = dstConn
	h.sourceURL = sourceURL
	h.destURL = destURL

	slog.Info("connected to NATS servers", "source", sourceURL, "dest", destURL)
	return nil
}

func (h *natsHub) Disconnect() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnectLocked()
}

// disconnectLocked closes connections and clears state. Caller must hold mu.
func (h *natsHub) disconnectLocked() {
	for id, ns := range h.subs {
		if err := ns.sub.Unsubscribe(); err != nil {
			slog.Warn("unsubscribe on disconnect", "id", id, "error", err)
		}
	}
	h.subs = make(map[string]*natsSub)

	if h.sourceConn != nil {
		h.sourceConn.Close()
		h.sourceConn = nil
	}
	if h.destConn != nil {
		h.destConn.Close()
		h.destConn = nil
	}
	if h.requestConn != nil {
		h.requestConn.Close()
		h.requestConn = nil
	}
	h.sourceURL = ""
	h.destURL = ""
	h.requestURL = ""
}

func (h *natsHub) Subscribe(subject string) (Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.destConn == nil {
		return Subscription{}, fmt.Errorf("not connected to dest NATS")
	}

	id := idgen.GenerateID()
	sub, err := h.destConn.Subscribe(subject, func(msg *nats.Msg) {
		h.broadcast(Message{
			ID:        idgen.GenerateID(),
			Subject:   msg.Subject,
			Payload:   string(msg.Data),
			Timestamp: time.Now().UTC(),
		})
	})
	if err != nil {
		return Subscription{}, fmt.Errorf("subscribe to %s: %w", subject, err)
	}

	h.subs[id] = &natsSub{id: id, subject: subject, sub: sub}
	slog.Info("subscribed", "id", id, "subject", subject)
	return Subscription{ID: id, Subject: subject}, nil
}

func (h *natsHub) Unsubscribe(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ns, ok := h.subs[id]
	if !ok {
		return fmt.Errorf("subscription %s not found", id)
	}
	if err := ns.sub.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe %s: %w", id, err)
	}
	delete(h.subs, id)
	slog.Info("unsubscribed", "id", id, "subject", ns.subject)
	return nil
}

func (h *natsHub) Publish(subject, payload string, dbg DebugHeaders) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.sourceConn == nil {
		return fmt.Errorf("not connected to source NATS")
	}
	msg := &nats.Msg{Subject: subject, Data: []byte(payload), Header: debugHeader(dbg)}
	if err := h.sourceConn.PublishMsg(msg); err != nil {
		return fmt.Errorf("publish to %s: %w", subject, err)
	}
	slog.Info("published", "subject", subject)
	return nil
}

func (h *natsHub) Status() ConnectionStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return ConnectionStatus{
		SourceConnected:  h.sourceConn != nil && h.sourceConn.IsConnected(),
		DestConnected:    h.destConn != nil && h.destConn.IsConnected(),
		RequestConnected: h.requestConn != nil && h.requestConn.IsConnected(),
		SourceURL:        h.sourceURL,
		DestURL:          h.destURL,
		RequestURL:       h.requestURL,
	}
}

func (h *natsHub) Subscriptions() []Subscription {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]Subscription, 0, len(h.subs))
	for _, ns := range h.subs {
		out = append(out, Subscription{ID: ns.id, Subject: ns.subject})
	}
	return out
}

func (h *natsHub) RegisterSSEClient(ch chan<- Message) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := idgen.GenerateID()
	h.clients[id] = ch
	return id
}

func (h *natsHub) UnregisterSSEClient(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, id)
}

func (h *natsHub) ConnectRequest(url string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.requestConn != nil {
		h.requestConn.Close()
		h.requestConn = nil
		h.requestURL = ""
	}

	conn, err := nats.Connect(url, buildConnectOptions("nats-debug-request", h.credsFile)...)
	if err != nil {
		return fmt.Errorf("connect to request NATS %s: %w", url, err)
	}

	h.requestConn = conn
	h.requestURL = url
	slog.Info("connected to request NATS", "url", url)
	return nil
}

func (h *natsHub) DisconnectRequest() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.requestConn != nil {
		h.requestConn.Close()
		h.requestConn = nil
	}
	h.requestURL = ""
	slog.Info("disconnected from request NATS")
}

func (h *natsHub) Request(subject, payload string, timeoutMs int, dbg DebugHeaders) (string, error) {
	h.mu.RLock()
	conn := h.requestConn
	h.mu.RUnlock()

	if conn == nil {
		return "", fmt.Errorf("not connected to request NATS")
	}

	req := &nats.Msg{Subject: subject, Data: []byte(payload), Header: debugHeader(dbg)}
	msg, err := conn.RequestMsg(req, time.Duration(timeoutMs)*time.Millisecond)
	if err != nil {
		return "", err
	}
	return string(msg.Data), nil
}

// broadcast sends a message to all registered SSE clients without blocking.
func (h *natsHub) broadcast(msg Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.clients {
		select {
		case ch <- msg:
		default:
			// drop message if client channel is full
		}
	}
}
