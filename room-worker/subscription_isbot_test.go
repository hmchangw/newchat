package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// TestNewSub_SetsIsBotFromAccount asserts subscriptions are stamped with the
// bot classification at creation time, so member-count reconciliation can read
// u.isBot directly instead of re-deriving it with a per-document regex.
func TestNewSub_SetsIsBotFromAccount(t *testing.T) {
	room := &model.Room{ID: "r1", SiteID: "site-a", Type: model.RoomTypeChannel}
	ts := time.Now().UTC()

	bot := newSub("s1", &model.User{ID: "u_bot", Account: "helper.bot"}, room, nil, "n", false, ts)
	assert.True(t, bot.User.IsBot, "helper.bot must be flagged as a bot")

	// Platform-admin pseudo-account is bot-like: stamped IsBot=true → appCount.
	pseudo := newSub("s2", &model.User{ID: "u_p", Account: "p_tchatadmin_siteA"}, room, nil, "n", false, ts)
	assert.True(t, pseudo.User.IsBot, "p_tchatadmin_ pseudo-account must be flagged as a bot")

	// QA p_ account is an ordinary user: stamped IsBot=false → userCount.
	qa := newSub("s3", &model.User{ID: "u_qa", Account: "p_webhook"}, room, nil, "n", false, ts)
	assert.False(t, qa.User.IsBot, "QA p_ account must not be flagged as a bot")

	human := newSub("s4", &model.User{ID: "u_h", Account: "alice"}, room, nil, "n", false, ts)
	assert.False(t, human.User.IsBot, "human account must not be flagged")
}
