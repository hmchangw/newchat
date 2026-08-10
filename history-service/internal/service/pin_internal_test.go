package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// TestRedactUnavailablePins_ClearsForwardedMessage guards that a forwarded
// snapshot on a pre-access pin is scrubbed like the rest of the rich content —
// otherwise a restricted caller could read a forwarded message's body through
// a pin stub even though the pin itself is meant to be a placeholder.
func TestRedactUnavailablePins_ClearsForwardedMessage(t *testing.T) {
	accessSince := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	pinned := []models.Message{
		{
			MessageID: "old",
			CreatedAt: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC), // pre-accessSince
		},
	}
	pinned[0].ForwardedMessage = &models.ForwardedMessage{MessageID: "m-src", Msg: "body"}
	redactUnavailablePins(pinned, &accessSince)
	assert.Nil(t, pinned[0].ForwardedMessage)
}
