package main

import (
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

// sessionCookieName is the cookie that ties a browser to its isolated hub.
const sessionCookieName = "ndsid"

// session owns one browser's hub and tracks when it was last active.
type session struct {
	id       string
	hub      Hub
	mu       sync.Mutex
	lastSeen time.Time
}

func (s *session) touch(now time.Time) {
	s.mu.Lock()
	s.lastSeen = now
	s.mu.Unlock()
}

func (s *session) idle(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return now.Sub(s.lastSeen)
}

// sessionManager maps session cookies to per-browser hubs and reaps idle ones.
type sessionManager struct {
	mu          sync.Mutex
	sessions    map[string]*session
	factory     func() Hub
	idleTimeout time.Duration
	now         func() time.Time
	done        chan struct{}
}

func newSessionManager(factory func() Hub, idleTimeout time.Duration) *sessionManager {
	return &sessionManager{
		sessions:    make(map[string]*session),
		factory:     factory,
		idleTimeout: idleTimeout,
		now:         time.Now,
		done:        make(chan struct{}),
	}
}

// resolve returns the session for the request's cookie, creating one (and
// setting the cookie) when absent or unknown. It must be called before any
// response body is written so the Set-Cookie header lands.
func (m *sessionManager) resolve(w http.ResponseWriter, r *http.Request) *session {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if c, err := r.Cookie(sessionCookieName); err == nil {
		if s, ok := m.sessions[c.Value]; ok {
			s.touch(now)
			return s
		}
	}

	id := idgen.GenerateID()
	s := &session{id: id, hub: m.factory(), lastSeen: now}
	m.sessions[id] = s
	// local debug tool served over plain HTTP on localhost
	// nosemgrep: cookie-missing-secure
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    id,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	slog.Info("created debug session", "session", id)
	return s
}

// touch refreshes a session's activity timestamp using the manager's clock.
func (m *sessionManager) touch(s *session) {
	s.touch(m.now())
}

// sweep disconnects and removes sessions idle longer than idleTimeout.
func (m *sessionManager) sweep() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for id, s := range m.sessions {
		if s.idle(now) > m.idleTimeout {
			s.hub.Disconnect()
			delete(m.sessions, id)
			slog.Info("swept idle debug session", "session", id)
		}
	}
}

// start launches the janitor goroutine that periodically sweeps idle sessions.
func (m *sessionManager) start() {
	interval := time.Minute
	if half := m.idleTimeout / 2; half > 0 && half < interval {
		interval = half
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.sweep()
			case <-m.done:
				return
			}
		}
	}()
}

// shutdown stops the janitor and disconnects every remaining session's hub.
func (m *sessionManager) shutdown() {
	close(m.done)
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, s := range m.sessions {
		s.hub.Disconnect()
		delete(m.sessions, id)
	}
}
