package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/config"
	"github.com/hmchangw/chat/history-service/internal/service/mocks"
)

// newAppNameService wires a service whose only live dependency is the app store.
func newAppNameService(t *testing.T, apps AppStore) *HistoryService {
	t.Helper()
	ctrl := gomock.NewController(t)
	return closeOnCleanupIn(t, New(
		mocks.NewMockMessageRepository(ctrl),
		mocks.NewMockSubscriptionRepository(ctrl),
		mocks.NewMockRoomRepository(ctrl),
		mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockThreadRoomRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl),
		mocks.NewMockUserStore(ctrl),
		apps,
		&config.Config{MessageHistoryFloorDays: 90, LargeRoomThreshold: 500, MaxPinnedPerRoom: 10},
	))
}

// A bot's app name is stable, but a busy room renders the same bot on every reaction.
// The cache only helps if it outlives the call — one built per call is pure overhead.
func TestBotAwareDisplayName_CachesAcrossCalls(t *testing.T) {
	ctrl := gomock.NewController(t)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "weather.bot").Return("Weather Bot", nil).Times(1)
	s := newAppNameService(t, apps)

	for range 4 {
		got := s.botAwareDisplayName(context.Background(), "Weather", "天氣", "weather.bot")
		assert.Equal(t, "Weather Bot", got)
	}
}

// Distinct bots are distinct answers; caching must key on the account, not collapse them.
func TestBotAwareDisplayName_CachesPerAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	apps := mocks.NewMockAppStore(ctrl)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "weather.bot").Return("Weather Bot", nil).Times(1)
	apps.EXPECT().AppNameByAccount(gomock.Any(), "news.bot").Return("News Bot", nil).Times(1)
	s := newAppNameService(t, apps)

	for range 3 {
		assert.Equal(t, "Weather Bot", s.botAwareDisplayName(context.Background(), "Weather", "", "weather.bot"))
		assert.Equal(t, "News Bot", s.botAwareDisplayName(context.Background(), "News", "", "news.bot"))
	}
}

// A human account never reaches the store, cached or not.
func TestBotAwareDisplayName_HumanAccountSkipsTheStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	apps := mocks.NewMockAppStore(ctrl)
	s := newAppNameService(t, apps)

	assert.Equal(t, "Ada Lovelace", s.botAwareDisplayName(context.Background(), "Ada Lovelace", "", "ada"))
}

// An unwired store must degrade to the composed name, not panic on a nil method value.
func TestBotAwareDisplayName_NoAppStoreDegrades(t *testing.T) {
	s := newAppNameService(t, nil)

	assert.Equal(t, "Weather", s.botAwareDisplayName(context.Background(), "Weather", "", "weather.bot"))
}
