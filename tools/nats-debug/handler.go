package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// sseKeepAlive is the interval between SSE keep-alive pings, which also refresh
// the session so an actively-watched feed is not swept as idle.
const sseKeepAlive = 25 * time.Second

type handler struct {
	sessions *sessionManager
}

func newHandler(sessions *sessionManager) *handler {
	return &handler{sessions: sessions}
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

type connectRequest struct {
	SourceURL string `json:"sourceURL"`
	DestURL   string `json:"destURL"`
}

func (h *handler) connect(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	var req connectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.SourceURL == "" || req.DestURL == "" {
		http.Error(w, "sourceURL and destURL are required", http.StatusBadRequest)
		return
	}
	if err := sess.hub.Connect(req.SourceURL, req.DestURL); err != nil {
		slog.Error("connect to NATS failed", "error", err)
		http.Error(w, fmt.Sprintf("connection failed: %s", err.Error()), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, sess.hub.Status())
}

func (h *handler) disconnect(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	sess.hub.Disconnect()
	writeJSON(w, http.StatusOK, map[string]string{"status": "disconnected"})
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	writeJSON(w, http.StatusOK, sess.hub.Status())
}

type subscribeRequest struct {
	Subject string `json:"subject"`
}

func (h *handler) subscribe(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	var req subscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Subject == "" {
		http.Error(w, "subject is required", http.StatusBadRequest)
		return
	}
	sub, err := sess.hub.Subscribe(req.Subject)
	if err != nil {
		slog.Error("subscribe failed", "subject", req.Subject, "error", err)
		http.Error(w, fmt.Sprintf("subscribe failed: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

func (h *handler) unsubscribe(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "subscription id is required", http.StatusBadRequest)
		return
	}
	if err := sess.hub.Unsubscribe(id); err != nil {
		http.Error(w, fmt.Sprintf("unsubscribe failed: %s", err.Error()), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	subs := sess.hub.Subscriptions()
	if subs == nil {
		subs = []Subscription{}
	}
	writeJSON(w, http.StatusOK, subs)
}

type publishRequest struct {
	Subject      string `json:"subject"`
	Payload      string `json:"payload"`
	Debug        string `json:"debug"`
	DebugPayload bool   `json:"debugPayload"`
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Subject == "" {
		http.Error(w, "subject is required", http.StatusBadRequest)
		return
	}
	if err := sess.hub.Publish(req.Subject, req.Payload, debugHeadersFor(req.Debug, req.DebugPayload)); err != nil {
		slog.Error("publish failed", "subject", req.Subject, "error", err)
		http.Error(w, fmt.Sprintf("publish failed: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "published"})
}

func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ch := make(chan Message, 64)
	clientID := sess.hub.RegisterSSEClient(ch)
	defer sess.hub.UnregisterSSEClient(clientID)

	keepalive := time.NewTicker(sseKeepAlive)
	defer keepalive.Stop()

	fmt.Fprint(w, "event: connected\ndata: {}\n\n")
	flusher.Flush()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(msg); err != nil {
				continue
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n", buf.String())
			flusher.Flush()
		case <-keepalive.C:
			// Refresh the session so an actively-watched feed is not swept.
			h.sessions.touch(sess)
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

type requestConnectRequest struct {
	URL string `json:"url"`
}

func (h *handler) requestConnect(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	var req requestConnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}
	if err := sess.hub.ConnectRequest(req.URL); err != nil {
		slog.Error("connect to request NATS failed", "error", err)
		http.Error(w, fmt.Sprintf("connection failed: %s", err.Error()), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, sess.hub.Status())
}

func (h *handler) requestDisconnect(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	sess.hub.DisconnectRequest()
	writeJSON(w, http.StatusOK, map[string]string{"status": "disconnected"})
}

type natsRequestBody struct {
	Subject      string `json:"subject"`
	Payload      string `json:"payload"`
	TimeoutMs    int    `json:"timeoutMs"`
	Debug        string `json:"debug"`
	DebugPayload bool   `json:"debugPayload"`
}

// debugHeadersFor normalizes a client-supplied X-Debug level through the same
// strict parse the services use, so off/empty/unknown collapse to no header and
// 1/true/on canonicalize to "debug". The dropdown only offers valid tokens; this
// guards against a malformed body emitting a stray header.
func debugHeadersFor(level string, payload bool) DebugHeaders {
	return DebugHeaders{
		Level:   natsutil.ParseDebugLevel(level).String(),
		Payload: payload,
	}
}

func (h *handler) request(w http.ResponseWriter, r *http.Request) {
	sess := h.sessions.resolve(w, r)
	var req natsRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Subject == "" {
		http.Error(w, "subject is required", http.StatusBadRequest)
		return
	}
	if req.TimeoutMs <= 0 {
		http.Error(w, "timeoutMs must be greater than 0", http.StatusBadRequest)
		return
	}

	reply, err := sess.hub.Request(req.Subject, req.Payload, req.TimeoutMs, debugHeadersFor(req.Debug, req.DebugPayload))
	if err != nil {
		switch {
		case errors.Is(err, nats.ErrNoResponders):
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "no responders available for subject"})
		case errors.Is(err, nats.ErrTimeout):
			writeJSON(w, http.StatusRequestTimeout, map[string]string{"error": "request timed out"})
		case err.Error() == "not connected to request NATS":
			writeJSON(w, http.StatusConflict, map[string]string{"error": "not connected"})
		default:
			slog.Error("nats request failed", "subject", req.Subject, "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("request failed: %s", err.Error())})
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"payload": reply})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
