package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type evictCall struct {
	account string
	roomID  string
}

type spyEvictor struct{ calls []evictCall }

func (s *spyEvictor) Evict(account, roomID string) {
	s.calls = append(s.calls, evictCall{account, roomID})
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestEvictOnSubscriptionUpdate(t *testing.T) {
	// Real add-path event shape.
	added := mustJSON(t, model.SubscriptionUpdateEvent{
		Action:       "added",
		Subscription: model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"},
	})
	// Real remove-path event shape (a different struct on the same subject).
	removed := mustJSON(t, model.SubscriptionRemovedEvent{
		Action: "removed",
		Subscription: model.RemovedSubscriptionRef{
			RoomID: "r1",
			U:      model.SubscriptionUser{Account: "alice"},
		},
	})

	tests := []struct {
		name    string
		subj    string
		payload []byte
		want    []evictCall
	}{
		{
			name:    "added evicts (account from subject, roomId from payload)",
			subj:    subject.SubscriptionUpdate("alice"),
			payload: added,
			want:    []evictCall{{"alice", "r1"}},
		},
		{
			name:    "removed evicts",
			subj:    subject.SubscriptionUpdate("alice"),
			payload: removed,
			want:    []evictCall{{"alice", "r1"}},
		},
		{
			name: "dotted bot account decoded from encoded subject token",
			subj: subject.SubscriptionUpdate("weather.site-a.bot"),
			payload: mustJSON(t, model.SubscriptionUpdateEvent{
				Action:       "added",
				Subscription: model.Subscription{User: model.SubscriptionUser{Account: "weather.site-a.bot"}, RoomID: "r2"},
			}),
			want: []evictCall{{"weather.site-a.bot", "r2"}},
		},
		{
			name: "read action does not evict (boundary unchanged, high frequency)",
			subj: subject.SubscriptionUpdate("alice"),
			payload: mustJSON(t, model.SubscriptionUpdateEvent{
				Action:       "read",
				Subscription: model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"},
			}),
			want: nil,
		},
		{
			name: "mute_toggled does not evict",
			subj: subject.SubscriptionUpdate("alice"),
			payload: mustJSON(t, model.SubscriptionUpdateEvent{
				Action:       "mute_toggled",
				Subscription: model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"},
			}),
			want: nil,
		},
		{
			name: "added with empty roomId does not evict",
			subj: subject.SubscriptionUpdate("alice"),
			payload: mustJSON(t, model.SubscriptionUpdateEvent{
				Action:       "added",
				Subscription: model.Subscription{User: model.SubscriptionUser{Account: "alice"}},
			}),
			want: nil,
		},
		{
			name:    "malformed payload does not evict or panic",
			subj:    subject.SubscriptionUpdate("alice"),
			payload: []byte("{not json"),
			want:    nil,
		},
		{
			name:    "wrong subject does not evict",
			subj:    "chat.user.alice.event.settings.update",
			payload: added,
			want:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spy := &spyEvictor{}
			assert.NotPanics(t, func() {
				evictOnSubscriptionUpdate(spy, tc.subj, tc.payload)
			})
			assert.Equal(t, tc.want, spy.calls)
		})
	}
}
