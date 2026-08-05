package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/service/mocks"
	"github.com/hmchangw/chat/pkg/displayfmt"
)

// A HistoryService built without an app store (apps == nil) must still resolve
// display names: the pre-refactor code only touched s.apps inside an IsBot
// branch, so a nil store was harmless. Passing s.apps.AppNameByAccount as a
// method value evaluates the receiver eagerly and segfaults before the bot
// check ever runs — regardless of the account.
func TestBotAwareDisplayName_NilAppStore(t *testing.T) {
	tests := []struct {
		name    string
		account string
	}{
		{"human account", "alice"},
		{"bot account", "helper.bot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &HistoryService{} // apps deliberately nil

			var got string
			require.NotPanics(t, func() {
				got = s.botAwareDisplayName(context.Background(), "Alice", "愛麗絲", tt.account)
			})
			// No app store ⇒ the composed name, for bots and humans alike.
			assert.Equal(t, displayfmt.CombineWithFallback("Alice", "愛麗絲", tt.account), got)
		})
	}
}

// With an app store wired, a bot account still prefers its app's display name.
func TestBotAwareDisplayName_BotUsesAppStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "helper.bot").Return("Helper App", nil).Times(1)

	s := &HistoryService{apps: apps}
	assert.Equal(t, "Helper App", s.botAwareDisplayName(context.Background(), "Alice", "愛麗絲", "helper.bot"))
}
