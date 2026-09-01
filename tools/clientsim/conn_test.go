package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimClient_ConnectAfterCloseDoesNotLeak(t *testing.T) {
	fc := newFakeConn(subListPage{HasMore: false})
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-lc", jwtModeExpiry, mint)
	dialed := make(chan struct{})
	proceed := make(chan struct{})
	s.dial = func(context.Context) (simConn, error) {
		close(dialed)
		<-proceed
		return fc, nil
	}
	require.NoError(t, s.primeJWT(context.Background()))

	errCh := make(chan error, 1)
	go func() { errCh <- s.connect(context.Background()) }()
	<-dialed
	s.close() // churn picks the client mid-dial
	close(proceed)

	err := <-errCh
	require.Error(t, err, "connect must refuse to install a conn after close")
	assert.Equal(t, int64(1), fc.closes.Load(), "the fresh conn must be closed, not leaked")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)
}

func TestDisconnectReason_Table(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"auth expired", nats.ErrAuthExpired, "auth_expired"},
		{"authorization", nats.ErrAuthorization, "authorization"},
		{"closed", nats.ErrConnectionClosed, "closed"},
		{"anything else", errors.New("tcp reset"), "other"},
		{"wrapped auth expired", fmt.Errorf("outer: %w", nats.ErrAuthExpired), "auth_expired"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, disconnectReason(tt.err))
		})
	}
}
