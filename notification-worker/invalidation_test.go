package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type invalidationTestMsg struct {
	jetstream.Msg
	data   []byte
	acks   int
	ackErr error
}

func (m *invalidationTestMsg) Data() []byte { return m.data }
func (m *invalidationTestMsg) Ack() error {
	m.acks++
	return m.ackErr
}

func TestProcessInvalidationMessage(t *testing.T) {
	t.Run("enqueues room and acknowledges", func(t *testing.T) {
		msg := &invalidationTestMsg{data: []byte(`{"type":"muted","roomId":"room-a","account":"alice","muted":true,"timestamp":1}`)}
		queue := make(chan string, 1)

		processInvalidationMessage(context.Background(), msg, queue)

		require.Equal(t, 1, msg.acks)
		assert.Equal(t, "room-a", <-queue)
	})

	t.Run("malformed payload acknowledges poison", func(t *testing.T) {
		msg := &invalidationTestMsg{data: []byte("not-json")}
		queue := make(chan string, 1)

		processInvalidationMessage(context.Background(), msg, queue)

		require.Equal(t, 1, msg.acks)
		assert.Empty(t, queue)
	})

	t.Run("full queue acknowledges because ttl reconciles", func(t *testing.T) {
		msg := &invalidationTestMsg{data: []byte(`{"type":"muted","roomId":"room-b","account":"alice","muted":false,"timestamp":2}`)}
		queue := make(chan string, 1)
		queue <- "already-full"

		processInvalidationMessage(context.Background(), msg, queue)

		require.Equal(t, 1, msg.acks)
		assert.Equal(t, "already-full", <-queue)
	})

	t.Run("acknowledgement failure is contained", func(t *testing.T) {
		msg := &invalidationTestMsg{
			data:   []byte(`{"type":"muted","roomId":"room-c","account":"alice","muted":true,"timestamp":3}`),
			ackErr: errors.New("ack unavailable"),
		}
		queue := make(chan string, 1)

		assert.NotPanics(t, func() {
			processInvalidationMessage(context.Background(), msg, queue)
		})

		require.Equal(t, 1, msg.acks)
		assert.Equal(t, "room-c", <-queue)
	})
}
