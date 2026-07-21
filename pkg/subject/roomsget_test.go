package subject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestRoomsGetSubject(t *testing.T) {
	// Server-to-server subject, account-agnostic (mirrors ThreadRoomInfoBatch).
	assert.Equal(t, "chat.server.request.history.site-a.rooms.get", subject.RoomsGet("site-a"))
}
