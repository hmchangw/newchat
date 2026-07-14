package natsutil

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

type fakeStatuser struct{ status nats.Status }

func (f fakeStatuser) Status() nats.Status { return f.status }

func TestHealthCheck_ReadyWhenConnectedOrReconnecting(t *testing.T) {
	for _, st := range []nats.Status{nats.CONNECTED, nats.RECONNECTING} {
		c := healthCheckFor(fakeStatuser{status: st})
		assert.Equal(t, "nats", c.Name)
		assert.NoError(t, c.Probe(context.Background()), "status %s should be ready", st)
	}
}

func TestHealthCheck_NotReadyWhenDisconnectedOrClosed(t *testing.T) {
	for _, st := range []nats.Status{nats.DISCONNECTED, nats.CLOSED} {
		c := healthCheckFor(fakeStatuser{status: st})
		assert.Error(t, c.Probe(context.Background()), "status %s should be not ready", st)
	}
}
