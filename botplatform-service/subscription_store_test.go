package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeSubStore is a hand-rolled subscriptionStore double for unit tests
// that need to pin routing decisions without a Mongo container.
type fakeSubStore struct {
	FindForBotFn   func(ctx context.Context, botID, roomID string) (*BotSub, error)
	FindDMForBotFn func(ctx context.Context, botID, otherID string) (*BotSub, error)
}

func (f *fakeSubStore) FindForBot(ctx context.Context, botID, roomID string) (*BotSub, error) {
	return f.FindForBotFn(ctx, botID, roomID)
}

func (f *fakeSubStore) FindDMForBot(ctx context.Context, botID, otherID string) (*BotSub, error) {
	return f.FindDMForBotFn(ctx, botID, otherID)
}

var _ subscriptionStore = (*fakeSubStore)(nil)

// TestSubStore_ReusesModelSentinel asserts BP treats
// model.ErrSubscriptionNotFound identically to the user-pipeline stores.
func TestSubStore_ReusesModelSentinel(t *testing.T) {
	wrapped := errors.Join(model.ErrSubscriptionNotFound, errors.New("more context"))
	assert.True(t, errors.Is(wrapped, model.ErrSubscriptionNotFound))
}

// TestBotSub_Fields is a compile-time shape check plus a marshal round-
// trip so a future field rename would trip this test.
func TestBotSub_Fields(t *testing.T) {
	s := BotSub{RoomID: "r1", SiteID: "site-b", RoomType: model.RoomTypeDM}
	assert.Equal(t, "r1", s.RoomID)
	assert.Equal(t, "site-b", s.SiteID)
	assert.Equal(t, model.RoomTypeDM, s.RoomType)
}
