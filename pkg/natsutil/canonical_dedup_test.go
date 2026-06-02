package natsutil_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestCanonicalDedupID(t *testing.T) {
	editedAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	pinnedAt := time.Date(2026, 5, 14, 12, 5, 0, 0, time.UTC)
	unpinnedAtMs := time.Date(2026, 5, 14, 12, 10, 0, 0, time.UTC).UnixMilli()

	tests := []struct {
		name string
		evt  *model.MessageEvent
		want string
	}{
		{
			name: "created uses bare messageID",
			evt: &model.MessageEvent{
				Event:   model.EventCreated,
				Message: model.Message{ID: "msg-1"},
			},
			want: "msg-1",
		},
		{
			name: "updated includes op suffix and editedAtMs",
			evt: &model.MessageEvent{
				Event:   model.EventUpdated,
				Message: model.Message{ID: "msg-1", EditedAt: &editedAt},
			},
			want: fmt.Sprintf("msg-1:updated:%d", editedAt.UnixMilli()),
		},
		{
			name: "deleted includes op suffix only",
			evt: &model.MessageEvent{
				Event:   model.EventDeleted,
				Message: model.Message{ID: "msg-1"},
			},
			want: "msg-1:deleted",
		},
		{
			name: "pinned includes op suffix and pinnedAtMs",
			evt: &model.MessageEvent{
				Event:   model.EventPinned,
				Message: model.Message{ID: "msg-1", PinnedAt: &pinnedAt},
			},
			want: fmt.Sprintf("msg-1:pinned:%d", pinnedAt.UnixMilli()),
		},
		{
			name: "unpinned includes op suffix and event timestamp",
			evt: &model.MessageEvent{
				Event:     model.EventUnpinned,
				Message:   model.Message{ID: "msg-1"},
				Timestamp: unpinnedAtMs,
			},
			want: fmt.Sprintf("msg-1:unpinned:%d", unpinnedAtMs),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, natsutil.CanonicalDedupID(tc.evt))
		})
	}
}

func TestCanonicalDedupID_UnknownEventFallsBackToMessageID(t *testing.T) {
	got := natsutil.CanonicalDedupID(&model.MessageEvent{
		Event:   model.EventType("bogus"),
		Message: model.Message{ID: "msg-1"},
	})
	assert.Equal(t, "msg-1", got)
}
