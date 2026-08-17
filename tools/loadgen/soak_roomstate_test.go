package main

import (
	"math/rand"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func soakRoomStateTestTopology(candidates int) *soakTopology {
	users := make([]model.User, 0, candidates+2)
	for i := range candidates + 2 {
		users = append(users, model.User{
			ID:      "u" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			Account: "user-" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
		})
	}
	return &soakTopology{
		BorrowedUsers: users,
		ActiveUsers:   users[:2],
		Rooms: []model.Room{
			{ID: "room-1", Type: model.RoomTypeChannel, Name: "soak-run-channel-000001"},
		},
		Subscriptions: []model.Subscription{
			{
				RoomID: "room-1", User: model.SubscriptionUser{ID: users[0].ID, Account: users[0].Account},
				Roles: []model.Role{model.RoleOwner}, IsSubscribed: true, RoomType: model.RoomTypeChannel,
			},
			{
				RoomID: "room-1", User: model.SubscriptionUser{ID: users[1].ID, Account: users[1].Account},
				Roles: []model.Role{model.RoleMember}, IsSubscribed: true, RoomType: model.RoomTypeChannel,
			},
		},
	}
}

func newSoakRoomStateTestPool(t *testing.T, candidates, quarantineMax int) (*soakRoomStatePool, *Metrics) {
	t.Helper()
	metrics := NewMetrics()
	pool, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(candidates), quarantineMax, metrics, rand.New(rand.NewSource(1)),
	)
	require.NoError(t, err)
	return pool, metrics
}

func soakGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, gauge.Write(&metric))
	return metric.GetGauge().GetValue()
}

func TestSoakRoomStatePool_CyclesCandidatesWithoutExhaustion(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	seen := make(map[string]int)
	for range 40 {
		intent, ok := pool.NextMemberIntent()
		require.True(t, ok, "the ring must never run dry while every cycle completes")
		assert.Equal(t, "user-a0", intent.Requester)
		assert.NotEqual(t, intent.Requester, intent.Account, "the owner is never a mutation target")
		seen[intent.Account]++
		pool.SettleMember(intent, failureResultGood)
	}
	assert.GreaterOrEqual(t, len(seen), 2, "candidates must be reused rather than consumed")
	for account, count := range seen {
		assert.GreaterOrEqual(t, count, 2, "candidate %s must be reused across cycles", account)
	}
}

func TestSoakRoomStatePool_PairsAddThenRemove(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.True(t, first.Add)
	assert.Equal(t, failureOperationMemberAdd, first.OperationType())
	pool.SettleMember(first, failureResultGood)

	second, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.False(t, second.Add, "a verified member must be removed before the candidate is reused")
	assert.Equal(t, first.Account, second.Account)
	assert.Equal(t, failureOperationMemberRemove, second.OperationType())

	pool.SettleMember(second, failureResultGood)
	third, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.True(t, third.Add)
}

func TestSoakRoomStatePool_LeaseBlocksConcurrentMemberMutations(t *testing.T) {
	pool, metrics := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextMemberIntent()
	require.True(t, ok)

	_, ok = pool.NextMemberIntent()
	assert.False(t, ok, "a room with an in-flight member mutation must not issue another")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomPoolExhausted.WithLabelValues(soakRoomPoolReasonNoCandidate)))

	pool.SettleMember(first, failureResultGood)
	_, ok = pool.NextMemberIntent()
	assert.True(t, ok)
}

func TestSoakRoomStatePool_RejectedMutationRestoresPriorState(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	require.True(t, intent.Add)
	pool.SettleMember(intent, failureResultBad)

	next, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.True(t, next.Add, "a rejected add changed nothing, so the candidate stays addable")
}

func TestSoakRoomStatePool_LocalFailureRestoresPriorState(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(intent, failureResultNotSent)

	next, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.True(t, next.Add, "a request that never left the process changed nothing")
}

func TestSoakRoomStatePool_UnverifiedResultQuarantinesCandidate(t *testing.T) {
	pool, metrics := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(intent, failureResultUnverified)

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.SoakRoomCandidates.WithLabelValues(string(soakMemberCandidateQuarantined))))

	probe, ok := pool.NextProbe()
	require.True(t, ok)
	assert.Equal(t, intent.Account, probe.Account)
	assert.False(t, probe.Mute)

	pool.ResolveMemberProbe(probe.RoomID, probe.Account, true)
	next, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.Equal(t, probe.Account, next.Account)
	assert.False(t, next.Add, "a candidate confirmed present must be removed, never re-added")
}

func TestSoakRoomStatePool_MissingResultQuarantinesCandidate(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(intent, failureResultMissingAfterDeadline)

	probe, ok := pool.NextProbe()
	require.True(t, ok)
	assert.Equal(t, intent.Account, probe.Account)
}

func TestSoakRoomStatePool_UnresolvedProbeReturnsToTheQueue(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(intent, failureResultUnverified)

	probe, ok := pool.NextProbe()
	require.True(t, ok)
	pool.ReleaseProbe(probe)

	again, ok := pool.NextProbe()
	require.True(t, ok)
	assert.Equal(t, probe.Account, again.Account, "an unanswered probe must be retried later")
}

func TestSoakRoomStatePool_QuarantineOverflowDegradesReversibly(t *testing.T) {
	pool, metrics := newSoakRoomStateTestPool(t, 3, 1)

	for range 3 {
		intent, ok := pool.NextMemberIntent()
		if !ok {
			break
		}
		pool.SettleMember(intent, failureResultUnverified)
	}

	assert.Equal(t, float64(1), soakGaugeValue(t, metrics.SoakRoomPoolDegraded))
	assert.Positive(t, testutil.ToFloat64(
		metrics.SoakRoomPoolExhausted.WithLabelValues(soakRoomPoolReasonQuarantineFull)))

	probe, ok := pool.NextProbe()
	require.True(t, ok)
	pool.ResolveMemberProbe(probe.RoomID, probe.Account, false)

	assert.Equal(t, float64(0), soakGaugeValue(t, metrics.SoakRoomPoolDegraded),
		"degradation must clear once the pool recovers")
}

func TestSoakRoomStatePool_MuteIntentAlternatesFromLastKnownState(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextMuteIntent()
	require.True(t, ok)
	assert.True(t, first.TargetMuted, "a seeded subscription is unmuted, so the first toggle mutes")
	pool.SettleMute(first, failureResultGood)

	// The lane round-robins accounts, so keep pulling until the same
	// subscription comes back around.
	next, found := soakNextMuteIntentFor(t, pool, first.Account)
	require.True(t, found)
	assert.False(t, next.TargetMuted, "the toggle landed, so the next one unmutes")
}

// soakNextMuteIntentFor drains mute intents until the given account reappears,
// settling the others so their leases do not block the search.
func soakNextMuteIntentFor(t *testing.T, pool *soakRoomStatePool, account string) (soakMuteIntent, bool) {
	t.Helper()
	for range 16 {
		intent, ok := pool.NextMuteIntent()
		if !ok {
			return soakMuteIntent{}, false
		}
		if intent.Account == account {
			return intent, true
		}
		pool.SettleMute(intent, failureResultGood)
	}
	return soakMuteIntent{}, false
}

func TestSoakRoomStatePool_AmbiguousMuteIsProbedNotRetoggled(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMuteIntent()
	require.True(t, ok)
	pool.SettleMute(intent, failureResultUnverified)

	_, found := soakNextMuteIntentFor(t, pool, intent.Account)
	assert.False(t, found, "an unknown mute state must be re-probed, never toggled again")

	probe, ok := pool.NextProbe()
	require.True(t, ok)
	assert.True(t, probe.Mute)
	assert.Equal(t, intent.Account, probe.Account)
	pool.ResolveMuteProbe(probe.RoomID, probe.Account, true)

	recovered, found := soakNextMuteIntentFor(t, pool, intent.Account)
	require.True(t, found)
	assert.False(t, recovered.TargetMuted, "the probe found it muted, so the next toggle unmutes")
}

func TestSoakRoomStatePool_RejectedMuteKeepsKnownState(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextMuteIntent()
	require.True(t, ok)
	pool.SettleMute(intent, failureResultBad)

	next, found := soakNextMuteIntentFor(t, pool, intent.Account)
	require.True(t, found)
	assert.Equal(t, intent.TargetMuted, next.TargetMuted,
		"a rejected toggle left the state alone, so the same target still applies")
}

func TestSoakRoomStatePool_MuteLeaseBlocksConcurrentToggles(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextMuteIntent()
	require.True(t, ok)

	second, ok := pool.NextMuteIntent()
	if ok {
		assert.NotEqual(t, first.Account, second.Account,
			"one account must never have two toggles in flight")
	}
}

func TestSoakRoomStatePool_RenameProducesDistinctNames(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.Equal(t, "user-a0", first.Requester)
	assert.Contains(t, first.NewName, "soak-run-channel-000001")
	pool.SettleRename(first, failureResultGood)

	second, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.NotEqual(t, first.NewName, second.NewName)
}

func TestSoakRoomStatePool_RenameLeaseBlocksConcurrentRenames(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextRenameIntent()
	require.True(t, ok)

	_, ok = pool.NextRenameIntent()
	assert.False(t, ok, "a room with an in-flight rename must not issue another")

	pool.SettleRename(first, failureResultGood)
	_, ok = pool.NextRenameIntent()
	assert.True(t, ok)
}

func TestSoakRoomStatePool_RenameNameStaysWithinRoomServiceLimit(t *testing.T) {
	topology := soakRoomStateTestTopology(3)
	topology.Rooms[0].Name = strings.Repeat("x", 120)
	pool, err := newSoakRoomStatePool(topology, 8, NewMetrics(), rand.New(rand.NewSource(2)))
	require.NoError(t, err)

	intent, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.LessOrEqual(t, utf8.RuneCountInString(intent.NewName), 100,
		"room-service rejects names longer than 100 runes")
}

func TestSoakRoomStatePool_RejectsTopologyWithoutUsableChannelRoom(t *testing.T) {
	_, err := newSoakRoomStatePool(&soakTopology{
		Rooms: []model.Room{{ID: "dm-1", Type: model.RoomTypeDM}},
	}, 8, NewMetrics(), rand.New(rand.NewSource(3)))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "channel room")
}

func TestSoakRoomStatePool_RejectsRoomWithoutCandidates(t *testing.T) {
	topology := soakRoomStateTestTopology(0)

	_, err := newSoakRoomStatePool(topology, 8, NewMetrics(), rand.New(rand.NewSource(4)))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "candidate")
}

func TestSoakRoomStatePool_RoomIDsAreStable(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	assert.Equal(t, []string{"room-1"}, pool.RoomIDs())
}

func TestSoakRoomStatePool_RejectsInvalidConstruction(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		topology      *soakTopology
		quarantineMax int
		rng           *rand.Rand
		want          string
	}{
		{
			name: "missing topology", quarantineMax: 8,
			rng: rand.New(rand.NewSource(1)), want: "topology",
		},
		{
			name: "zero quarantine capacity", topology: soakRoomStateTestTopology(3),
			quarantineMax: 0, rng: rand.New(rand.NewSource(1)), want: "quarantine capacity",
		},
		{
			name: "missing random source", topology: soakRoomStateTestTopology(3),
			quarantineMax: 8, want: "random source",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := newSoakRoomStatePool(
				testCase.topology, testCase.quarantineMax, NewMetrics(), testCase.rng,
			)

			require.Error(t, err)
			assert.Contains(t, err.Error(), testCase.want)
		})
	}
}

func TestSoakRoomStatePool_SkipsRoomsWithoutOwner(t *testing.T) {
	topology := soakRoomStateTestTopology(3)
	topology.Subscriptions[0].Roles = []model.Role{model.RoleMember}

	_, err := newSoakRoomStatePool(topology, 8, NewMetrics(), rand.New(rand.NewSource(5)))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "owner")
}

func TestSoakRoomStatePool_ExcludesBotsAndPlatformAdminsFromCandidates(t *testing.T) {
	topology := soakRoomStateTestTopology(2)
	topology.BorrowedUsers = append(topology.BorrowedUsers,
		model.User{ID: "bot", Account: "helper.bot"},
		model.User{ID: "admin", Account: "p_admin"},
	)

	pool, err := newSoakRoomStatePool(topology, 8, NewMetrics(), rand.New(rand.NewSource(6)))
	require.NoError(t, err)

	for range 12 {
		intent, ok := pool.NextMemberIntent()
		require.True(t, ok)
		assert.NotEqual(t, "helper.bot", intent.Account)
		assert.NotEqual(t, "p_admin", intent.Account)
		pool.SettleMember(intent, failureResultGood)
	}
}

func TestSoakRoomStatePool_IgnoresSettlementForUnknownRoom(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	assert.NotPanics(t, func() {
		pool.SettleMember(soakMemberIntent{RoomID: "missing", Account: "user-z9"}, failureResultGood)
		pool.SettleRename(soakRenameIntent{RoomID: "missing"}, failureResultGood)
		pool.SettleMute(soakMuteIntent{RoomID: "missing", Account: "user-z9"}, failureResultGood)
		pool.ResolveMemberProbe("missing", "user-z9", true)
		pool.ResolveMuteProbe("missing", "user-z9", true)
	})
}

func TestSoakRoomStatePool_NextProbeIsEmptyWithoutQuarantine(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	_, ok := pool.NextProbe()

	assert.False(t, ok)
}

func TestSoakRoomStatePool_WorksWithoutMetrics(t *testing.T) {
	pool, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(3), 8, nil, rand.New(rand.NewSource(7)),
	)
	require.NoError(t, err)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assert.NotPanics(t, func() {
		pool.SettleMember(intent, failureResultUnverified)
		probe, probed := pool.NextProbe()
		require.True(t, probed)
		pool.ResolveMemberProbe(probe.RoomID, probe.Account, false)
	})
}

func TestSoakRoomStatePool_RenameCountsUpFromAFixedBase(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.Equal(t, "soak-run-channel-000001-r1", first.NewName)
	pool.SettleRename(first, failureResultGood)

	second, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.Equal(t, "soak-run-channel-000001-r2", second.NewName,
		"names count up from the seeded base so they never chain or repeat")

	pool.SettleRename(second, failureResultUnverified)
	third, ok := pool.NextRenameIntent()
	require.True(t, ok)
	assert.Equal(t, "soak-run-channel-000001-r3", third.NewName,
		"an unresolved rename still advances so names stay unique")
}

func TestSoakRoomStatePool_RenameNamesStayUniqueAtTheLengthCap(t *testing.T) {
	topology := soakRoomStateTestTopology(3)
	topology.Rooms[0].Name = strings.Repeat("x", 96)
	pool, err := newSoakRoomStatePool(topology, 8, NewMetrics(), rand.New(rand.NewSource(21)))
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for range 25 {
		intent, ok := pool.NextRenameIntent()
		require.True(t, ok)
		assert.LessOrEqual(t, utf8.RuneCountInString(intent.NewName), soakRoomMaxNameRunes)
		_, duplicate := seen[intent.NewName]
		assert.False(t, duplicate,
			"a repeated name makes a lost rename look applied: %q", intent.NewName)
		seen[intent.NewName] = struct{}{}
		pool.SettleRename(intent, failureResultGood)
	}
}

func TestSoakRoomStatePool_UnconfirmedRenameMakesTheNameUnknown(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	name, known := pool.RoomName("room-1")
	require.True(t, known)
	assert.Equal(t, "soak-run-channel-000001", name)

	first, ok := pool.NextRenameIntent()
	require.True(t, ok)
	pool.SettleRename(first, failureResultUnverified)

	_, known = pool.RoomName("room-1")
	assert.False(t, known,
		"an unresolved rename leaves the real name unknown, so no name may be asserted as the prior one")

	second, ok := pool.NextRenameIntent()
	require.True(t, ok)
	pool.SettleRename(second, failureResultGood)
	name, known = pool.RoomName("room-1")
	assert.True(t, known)
	assert.Equal(t, second.NewName, name)
}

func TestSoakRoomStatePool_RejectedRenameKeepsTheKnownName(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	intent, ok := pool.NextRenameIntent()
	require.True(t, ok)
	pool.SettleRename(intent, failureResultBad)

	name, known := pool.RoomName("room-1")
	assert.True(t, known, "a rejected rename changed nothing")
	assert.Equal(t, "soak-run-channel-000001", name)
}

func TestSoakRoomStatePool_ReadIntentCarriesTheServerBaseline(t *testing.T) {
	topology := soakRoomStateTestTopology(3)
	seen := time.Date(2026, 8, 16, 3, 0, 0, 0, time.UTC)
	topology.Subscriptions[0].LastSeenAt = &seen
	pool, err := newSoakRoomStatePool(topology, 8, NewMetrics(), rand.New(rand.NewSource(30)))
	require.NoError(t, err)

	intent, ok := pool.NextReadIntent()
	require.True(t, ok)
	if intent.Account == "user-a0" {
		assert.True(t, intent.Known)
		assert.Equal(t, seen, intent.Baseline)
		return
	}
	assert.False(t, intent.Known, "a subscription that was never read has no baseline to beat")
}

func TestSoakRoomStatePool_ConfirmedReadBecomesTheNextBaseline(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	observed := time.Date(2026, 8, 16, 4, 0, 0, 0, time.UTC)

	first, ok := pool.NextReadIntent()
	require.True(t, ok)
	pool.SettleRead(first, failureResultGood, observed)

	next, found := soakNextReadIntentFor(t, pool, first.Account)
	require.True(t, found)
	assert.True(t, next.Known)
	assert.Equal(t, observed, next.Baseline,
		"the next mark-read must beat the timestamp the server just wrote")
}

func TestSoakRoomStatePool_UnresolvedReadDropsTheBaseline(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	observed := time.Date(2026, 8, 16, 4, 0, 0, 0, time.UTC)

	first, ok := pool.NextReadIntent()
	require.True(t, ok)
	pool.SettleRead(first, failureResultGood, observed)
	second, found := soakNextReadIntentFor(t, pool, first.Account)
	require.True(t, found)
	pool.SettleRead(second, failureResultUnverified, time.Time{})

	third, found := soakNextReadIntentFor(t, pool, first.Account)
	require.True(t, found)
	assert.False(t, third.Known,
		"an unresolved mark-read leaves the stored timestamp untrustworthy")
}

func TestSoakRoomStatePool_RejectedReadKeepsTheBaseline(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	observed := time.Date(2026, 8, 16, 4, 0, 0, 0, time.UTC)

	first, ok := pool.NextReadIntent()
	require.True(t, ok)
	pool.SettleRead(first, failureResultGood, observed)
	second, found := soakNextReadIntentFor(t, pool, first.Account)
	require.True(t, found)
	pool.SettleRead(second, failureResultBad, time.Time{})

	third, found := soakNextReadIntentFor(t, pool, first.Account)
	require.True(t, found)
	assert.True(t, third.Known)
	assert.Equal(t, observed, third.Baseline)
}

func TestSoakRoomStatePool_ReadLeaseBlocksConcurrentMarks(t *testing.T) {
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)

	first, ok := pool.NextReadIntent()
	require.True(t, ok)

	second, ok := pool.NextReadIntent()
	if ok {
		assert.NotEqual(t, first.Account, second.Account,
			"one subscription must never have two mark-reads in flight")
	}
}

func soakNextReadIntentFor(t *testing.T, pool *soakRoomStatePool, account string) (soakReadIntent, bool) {
	t.Helper()
	for range 16 {
		intent, ok := pool.NextReadIntent()
		if !ok {
			return soakReadIntent{}, false
		}
		if intent.Account == account {
			return intent, true
		}
		pool.SettleRead(intent, failureResultGood, time.Time{})
	}
	return soakReadIntent{}, false
}

// Retaking a recovered reservation has to move the candidate out of the
// runnable ring, not just relock the room. The rebuilt pool put the account
// back in a runnable slice from current topology; leaving it there lets the
// next dispatch mutate a target whose real state is still unknown, which is the
// resend the whole ledger design exists to prevent.
func TestSoakRoomStatePool_ReserveInFlightRemovesTheCandidateFromTheRing(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		kind   failureOperationType
		result failureResult
	}{
		{
			name: "unverified add is parked, not reissued",
			kind: failureOperationMemberAdd, result: failureResultUnverified,
		},
		{
			name: "unverified remove is parked, not reissued",
			kind: failureOperationMemberRemove, result: failureResultUnverified,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pool, err := newSoakRoomStatePool(
				soakRoomStateTestTopology(3), 8, nil, rand.New(rand.NewSource(1)),
			)
			require.NoError(t, err)

			intent, ok := pool.NextMemberIntent()
			require.True(t, ok)
			pool.SettleMember(intent, failureResultGood)
			roomID, account := intent.RoomID, intent.Account

			require.True(t, pool.ReserveInFlight(testCase.kind, roomID, account))
			pool.SettleMember(
				soakMemberIntent{
					RoomID: roomID, Account: account,
					Add: testCase.kind == failureOperationMemberAdd,
				},
				testCase.result,
			)

			for range 32 {
				next, more := pool.NextMemberIntent()
				if !more {
					break
				}
				assert.NotEqual(t, account, next.Account,
					"a quarantined target must not be dispatched again")
				pool.SettleMember(next, failureResultGood)
			}
		})
	}
}

// A recovered reservation that resolves cleanly must return the candidate to
// the ring exactly once. Settle appends, so leaving the original entry in place
// would duplicate it and let two operations target the same account at once.
func TestSoakRoomStatePool_ReserveInFlightDoesNotDuplicateTheCandidate(t *testing.T) {
	pool, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(3), 8, nil, rand.New(rand.NewSource(2)),
	)
	require.NoError(t, err)

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(intent, failureResultGood)
	roomID, account := intent.RoomID, intent.Account

	require.True(t, pool.ReserveInFlight(failureOperationMemberAdd, roomID, account))
	pool.SettleMember(
		soakMemberIntent{RoomID: roomID, Account: account, Add: true},
		failureResultGood,
	)

	// The ring legitimately recycles a candidate across dispatches, so the
	// invariant is about the slices themselves: one entry, in one of them.
	room := pool.byID[roomID]
	require.NotNil(t, room)
	occurrences := 0
	for _, candidate := range room.available {
		if candidate == account {
			occurrences++
		}
	}
	for _, candidate := range room.members {
		if candidate == account {
			occurrences++
		}
	}
	assert.Equal(t, 1, occurrences, "the candidate must appear in the ring exactly once")
}

// The candidate gauges are maintained incrementally rather than recomputed, so
// they are only correct if every transition adjusts them. This walks a full
// add/remove cycle and checks the published totals against the pool's own
// slices, which is what a recomputing implementation would have reported.
func TestSoakRoomStatePool_CandidateGaugesTrackEveryTransition(t *testing.T) {
	metrics := NewMetrics()
	pool, err := newSoakRoomStatePool(
		soakRoomStateTestTopology(3), 8, metrics, rand.New(rand.NewSource(3)),
	)
	require.NoError(t, err)

	assertGaugesMatchPool := func(stage string) {
		t.Helper()
		expected := make(map[soakMemberCandidateState]int)
		for _, room := range pool.rooms {
			for _, state := range room.states {
				expected[state]++
			}
		}
		for _, state := range soakMemberCandidateStates {
			assert.Equal(t, float64(expected[state]), testutil.ToFloat64(
				metrics.SoakRoomCandidates.WithLabelValues(string(state)),
			), "%s: %s", stage, state)
		}
	}

	assertGaugesMatchPool("startup")

	intent, ok := pool.NextMemberIntent()
	require.True(t, ok)
	assertGaugesMatchPool("dispatched")

	pool.SettleMember(intent, failureResultGood)
	assertGaugesMatchPool("settled good")

	next, ok := pool.NextMemberIntent()
	require.True(t, ok)
	pool.SettleMember(next, failureResultUnverified)
	assertGaugesMatchPool("quarantined")

	require.True(t, pool.ReserveInFlight(failureOperationMemberAdd, intent.RoomID, intent.Account))
	assertGaugesMatchPool("reserved after restart")
}
