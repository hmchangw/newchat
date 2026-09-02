package subject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// matches reports whether subj is captured by a trailing-">" stream subject,
// which is the only wildcard form the patterns under test use.
func matches(pattern, subj string) bool {
	prefix, ok := strings.CutSuffix(pattern, ">")
	if !ok {
		return pattern == subj
	}
	return strings.HasPrefix(subj, prefix)
}

// Every per-message canonical event has to exist on both lanes: history-service
// edits, deletes, pins and reactions on the failover lane must not be published
// to the live canonical stream, which sits on the cluster that is down.
func TestLane_MsgCanonical(t *testing.T) {
	for _, tc := range []struct {
		event        CanonicalEvent
		home, failed string
	}{
		{CanonicalCreated, "chat.msg.canonical.site-a.created", "chat.failover.msg.canonical.site-a.created"},
		{CanonicalUpdated, "chat.msg.canonical.site-a.updated", "chat.failover.msg.canonical.site-a.updated"},
		{CanonicalDeleted, "chat.msg.canonical.site-a.deleted", "chat.failover.msg.canonical.site-a.deleted"},
		{CanonicalPinned, "chat.msg.canonical.site-a.pinned", "chat.failover.msg.canonical.site-a.pinned"},
		{CanonicalUnpinned, "chat.msg.canonical.site-a.unpinned", "chat.failover.msg.canonical.site-a.unpinned"},
		{CanonicalReacted, "chat.msg.canonical.site-a.reacted", "chat.failover.msg.canonical.site-a.reacted"},
	} {
		t.Run(string(tc.event), func(t *testing.T) {
			assert.Equal(t, tc.home, LaneHome.MsgCanonical("site-a", tc.event))
			assert.Equal(t, tc.failed, LaneFailover.MsgCanonical("site-a", tc.event))
		})
	}
}

// The named builders and the lane-aware one must agree, because they are the
// same subject reached two ways — a drift would split the canonical feed.
func TestLane_MsgCanonical_MatchesTheNamedBuilders(t *testing.T) {
	assert.Equal(t, MsgCanonicalCreated("site-a"), LaneHome.MsgCanonical("site-a", CanonicalCreated))
	assert.Equal(t, MsgCanonicalUpdated("site-a"), LaneHome.MsgCanonical("site-a", CanonicalUpdated))
	assert.Equal(t, MsgCanonicalDeleted("site-a"), LaneHome.MsgCanonical("site-a", CanonicalDeleted))
	assert.Equal(t, MsgCanonicalPinned("site-a"), LaneHome.MsgCanonical("site-a", CanonicalPinned))
	assert.Equal(t, MsgCanonicalUnpinned("site-a"), LaneHome.MsgCanonical("site-a", CanonicalUnpinned))
	assert.Equal(t, MsgCanonicalReacted("site-a"), LaneHome.MsgCanonical("site-a", CanonicalReacted))
	assert.Equal(t, FailoverMsgCanonicalCreated("site-a"), LaneFailover.MsgCanonical("site-a", CanonicalCreated))
}

// Each lane's subjects must be captured by that lane's stream and no other,
// which is the whole reason the chat.failover.> root exists.
func TestLane_MsgCanonical_LandsOnItsOwnStreamFilter(t *testing.T) {
	for _, evt := range []CanonicalEvent{CanonicalCreated, CanonicalUpdated, CanonicalDeleted,
		CanonicalPinned, CanonicalUnpinned, CanonicalReacted} {
		home := LaneHome.MsgCanonical("site-a", evt)
		failover := LaneFailover.MsgCanonical("site-a", evt)

		require.True(t, matches(MsgCanonicalWildcard("site-a"), home), "%s on the live stream", evt)
		require.False(t, matches(FailoverMsgCanonicalWildcard("site-a"), home), "%s must not reach the standby", evt)
		require.True(t, matches(FailoverMsgCanonicalWildcard("site-a"), failover), "%s on the standby", evt)
		require.False(t, matches(MsgCanonicalWildcard("site-a"), failover), "%s must not reach the live stream", evt)
	}
}
