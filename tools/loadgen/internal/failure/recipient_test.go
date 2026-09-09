package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestRecipientEvidence_ClassifiesRoutesAndDuplicates(t *testing.T) {
	evidence := NewRecipientEvidence(false)
	require.NoError(t, evidence.ExpectDelivery(&RecipientExpectationConfig{
		OperationID: "message-1",
		Recipients:  []string{"alice"},
		RoomID:      "room-1",
		EventType:   model.RoomEventNewMessage,
		Route:       RecipientRouteRoom,
		Source:      RecipientSourceTopology,
		Complete:    true,
	}))

	assert.Equal(t, RecipientEvidenceExpected, evidence.ObserveDelivery(
		"message-1", "alice", "room-1", model.RoomEventNewMessage, RecipientDeliveryRoomGlobal,
	))
	assert.Equal(t, RecipientEvidenceDuplicate, evidence.ObserveDelivery(
		"message-1", "alice", "room-1", model.RoomEventNewMessage, RecipientDeliveryRoomGlobal,
	))
	assert.Equal(t, RecipientEvidenceExpected, evidence.ObserveDelivery(
		"message-1", "alice", "room-1", model.RoomEventNewMessage, RecipientDeliveryRoomLocal,
	))
	assert.False(t, evidence.Complete("message-1"))
	assert.Equal(t, ObservationBad, evidence.Finalize("message-1", true).Observation)
}

func TestRecipientEvidence_ValidatesAndBoundsExpectations(t *testing.T) {
	evidence := NewRecipientEvidence(false)
	evidence.SetCapacity(1)
	require.Error(t, evidence.ExpectDelivery(nil))
	require.NoError(t, evidence.Expect("op-1", []string{"alice"}))
	require.ErrorContains(t, evidence.Expect("op-2", []string{"bob"}), "capacity")
	assert.Equal(t, 1, evidence.Capacity())
	assert.Equal(t, 1, evidence.Len())
	assert.Equal(t, 1, evidence.ForgetAll([]string{"op-1"}))
}

func TestRecipientEvidence_ReplaysDurablePositiveEvidence(t *testing.T) {
	evidence := NewRecipientEvidence(false)
	require.NoError(t, evidence.Expect("op", []string{"alice", "bob"}))
	require.True(t, evidence.ReplayPositive("missing", "op", "bob"))
	require.True(t, evidence.ReplayPositive("unexpected", "op", "mallory"))
	require.False(t, evidence.ReplayPositive("unknown", "op", "nobody"))

	result := evidence.Finalize("op", true)
	assert.Equal(t, ObservationBad, result.Observation)
	assert.Equal(t, []string{"bob"}, result.Missing)
	assert.Equal(t, []string{"mallory"}, result.Unexpected)
	assert.True(t, result.Durable("missing", "bob"))
}

func TestFailureRecipientEvidence_ExactPartialUnexpectedAndDuplicate(t *testing.T) {
	tests := []struct {
		name                           string
		observed                       []string
		want                           Observation
		missing, unexpected, duplicate []string
	}{
		{name: "exact", observed: []string{"bob", "alice"}, want: ObservationGood},
		{name: "partial", observed: []string{"alice"}, want: ObservationMissingAfterDeadline, missing: []string{"bob"}},
		{name: "unexpected", observed: []string{"alice", "bob", "mallory"}, want: ObservationBad, unexpected: []string{"mallory"}},
		{name: "duplicate", observed: []string{"alice", "alice", "bob"}, want: ObservationBad, duplicate: []string{"alice"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			evidence := NewRecipientEvidence(false)
			require.NoError(t, evidence.Expect("message-1", []string{"alice", "bob"}))
			for _, recipient := range tc.observed {
				evidence.Observe("message-1", recipient)
			}
			result := evidence.Finalize("message-1", true)
			assert.Equal(t, tc.want, result.Observation)
			assert.Equal(t, tc.missing, result.Missing)
			assert.Equal(t, tc.unexpected, result.Unexpected)
			assert.Equal(t, tc.duplicate, result.Duplicates)
		})
	}
}

func TestFailureRecipientEvidence_IncompleteSetStaysUnverified(t *testing.T) {
	evidence := NewRecipientEvidence(false)
	require.NoError(t, evidence.ExpectDelivery(&RecipientExpectationConfig{
		OperationID: "message-1", Recipients: []string{"alice"}, RoomID: "room-1",
		EventType: model.RoomEventNewThreadMessage, Route: RecipientRouteUser,
		Source: RecipientSourceThreadFollowers, Complete: false,
	}))
	assert.Equal(t, RecipientEvidenceExpected, evidence.ObserveDelivery(
		"message-1", "bob", "room-1", model.RoomEventNewThreadMessage, RecipientDeliveryUser,
	))
	result := evidence.Finalize("message-1", true)
	assert.Equal(t, ObservationUnverified, result.Observation)
	assert.Empty(t, result.Missing)
	assert.Empty(t, result.Unexpected)
}

func TestFailureRecipientEvidence_AllowsConfiguredDuplicatesAndForget(t *testing.T) {
	var absent *RecipientEvidence
	assert.Error(t, absent.Expect("", nil))
	assert.Zero(t, absent.ForgetAll([]string{"op"}))
	assert.Zero(t, absent.Len())

	evidence := NewRecipientEvidence(true)
	assert.Error(t, evidence.Expect("message", []string{"alice", "alice"}))
	require.NoError(t, evidence.Expect("message", []string{"alice"}))
	assert.Error(t, evidence.Expect("message", []string{"alice"}))
	assert.True(t, evidence.Observe("message", "alice"))
	assert.True(t, evidence.Observe("message", "alice"))
	assert.True(t, evidence.Complete("message"))
	result := evidence.Finalize("message", true)
	assert.Equal(t, ObservationGood, result.Observation)
	assert.Equal(t, []string{"alice"}, result.Duplicates)
	evidence.Forget("missing")
	assert.False(t, evidence.Observe("missing", "alice"))
	assert.Equal(t, ObservationUnverified, evidence.Finalize("missing", true).Observation)
}

func TestFailureRecipientEvidence_DurableAllowedDuplicateDoesNotOverrideMissing(t *testing.T) {
	evidence := NewRecipientEvidence(true)
	require.NoError(t, evidence.Expect("message", []string{"alice", "bob"}))
	assert.True(t, evidence.ReplayPositive("duplicate", "message", "alice"))
	assert.True(t, evidence.ReplayPositive("missing", "message", "bob"))

	result := evidence.Finalize("message", false)
	assert.Equal(t, ObservationMissingAfterDeadline, result.Observation)
	assert.Equal(t, []string{"bob"}, result.Missing)
	assert.Equal(t, []string{"alice"}, result.Duplicates)
}

func TestFailureRecipientEvidence_RejectsMalformedExpectations(t *testing.T) {
	tests := []RecipientExpectationConfig{
		{OperationID: "op", Recipients: []string{"alice"}, RoomID: "room"},
		{OperationID: "op", Recipients: []string{"alice"}, EventType: model.RoomEventNewMessage},
		{OperationID: "op", Recipients: []string{"alice"}, Route: RecipientExpectedRoute("bad")},
		{OperationID: "op", Recipients: []string{"alice"}, Source: RecipientSetSource("bad")},
		{OperationID: "op", Recipients: []string{""}},
	}
	for _, config := range tests {
		evidence := NewRecipientEvidence(false)
		assert.Error(t, evidence.ExpectDelivery(&config))
	}
}
