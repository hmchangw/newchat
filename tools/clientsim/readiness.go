package main

// markConnUp/markConnDown/markReady are the idempotent transitions behind
// clientsim_conns_active and clientsim_conns_ready. Idempotence is not
// optional: nats.go fires DisconnectedErrCB on an explicit Close() as well
// as on a real drop (nats.go@v1.50.0 close()), so close() and the handler
// both arrive for the same event and a bare Dec in each would double-count.
// invalidatePlan marks the subscription plan unverified. Called when the
// connection drops: whatever the server's plan is now, this client has not
// checked it since, so only a completed walk may promote it again.
func (s *simClient) invalidatePlan() {
	s.mu.Lock()
	s.planVerified = false
	s.mu.Unlock()
}

func (s *simClient) markConnUp() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.connUp {
		return
	}
	s.connUp = true
	s.m.ConnsActive.Inc()
}

// markConnDown drops readiness with the connection: a client whose broker
// went away is not carrying its subscriptions either. The post-reconnect
// resync walk is what raises it back.
func (s *simClient) markConnDown() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.ready {
		s.ready = false
		s.m.readyDec()
	}
	if !s.connUp {
		return
	}
	s.connUp = false
	s.m.ConnsActive.Dec()
}

// markReady records that the full subscription plan is applied. A client
// that is not connected can never be ready.
func (s *simClient) markReady() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.connUp || s.ready {
		return
	}
	s.ready = true
	s.m.readyInc()
}

// markNotReady is the partial-plan path: still connected, but missing at
// least one room, so it must not be counted toward the fleet gate.
func (s *simClient) markNotReady() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.ready {
		return
	}
	s.ready = false
	s.m.readyDec()
}
