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

// The OUTBOX buffer follows the same rule as the canonical stream: a
// failover-lane federation event has to land on the buddy-hosted standby, since
// the live OUTBOX sits on the cluster that is down.
func TestLane_Outbox(t *testing.T) {
	assert.Equal(t, "chat.outbox.site-a.site-b.member_added", LaneHome.Outbox("site-a", "site-b", "member_added"))
	assert.Equal(t, "chat.failover.outbox.site-a.site-b.member_added", LaneFailover.Outbox("site-a", "site-b", "member_added"))

	// The named builders remain the public face; both must resolve to the lane form.
	assert.Equal(t, Outbox("site-a", "site-b", "x"), LaneHome.Outbox("site-a", "site-b", "x"))
	assert.Equal(t, FailoverOutbox("site-a", "site-b", "x"), LaneFailover.Outbox("site-a", "site-b", "x"))

	require.True(t, matches(OutboxWildcard("site-a"), LaneHome.Outbox("site-a", "site-b", "x")))
	require.False(t, matches(FailoverOutboxWildcard("site-a"), LaneHome.Outbox("site-a", "site-b", "x")))
	require.True(t, matches(FailoverOutboxWildcard("site-a"), LaneFailover.Outbox("site-a", "site-b", "x")))
	require.False(t, matches(OutboxWildcard("site-a"), LaneFailover.Outbox("site-a", "site-b", "x")))
}
