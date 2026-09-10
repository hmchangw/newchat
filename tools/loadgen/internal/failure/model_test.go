package failure

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureOperation_PreservesPersistenceShapeAndLegacyID(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	operation := Operation{
		SchemaVersion: 2, ID: "operation-1", CorrelationID: "correlation-1", RunID: "run-1",
		Scenario: ScenarioMessageSoak, Lane: LaneMessageSend, OperationType: OperationMessageCreate,
		StartedAt: now, VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
		Targets:            map[string]string{"messageId": "message-1"},
		Effects:            MessageCreateExpectedEffects(true, false, 1, "sha256"),
		Expected:           []Observer{ObserverAdmission, ObserverHistory, ObserverRecipient},
		Attributes:         map[string]string{"phase": "measured"},
		Observations:       map[Observer]Observation{ObserverAdmission: ObservationGood},
		ObservationReasons: map[Observer]Reason{ObserverAdmission: ReasonAdmissionRejected},
		FinalResult:        ResultBad, FinalReason: ReasonHistoryContentMismatch,
		EvidenceRefs: []string{"recipient:operation-1"}, LifecycleState: OperationActive,
	}
	assertJSONFields(t, operation, []string{
		"schemaVersion", "operationId", "correlationId", "runId", "scenario", "lane",
		"operationType", "startedAt", "verifyAfter", "deadline", "targets",
		"expectedEffects", "expected", "attributes", "observations",
		"observationReasons", "finalResult", "finalReason", "evidenceRefs", "lifecycleState",
	})

	minimal := Operation{
		SchemaVersion: 2, ID: "operation-2", Scenario: ScenarioMessageSoak,
		Lane: LaneMessageSend, StartedAt: now, VerifyAfter: now, Deadline: now,
	}
	assertJSONFields(t, minimal, []string{
		"schemaVersion", "operationId", "scenario", "lane", "startedAt", "verifyAfter", "deadline",
	})

	legacy := Operation{
		ID: "legacy-1", Scenario: ScenarioMessageSoak, Lane: LaneMessageSend,
		StartedAt: now, VerifyAfter: now, Deadline: now,
	}
	encoded, err := json.Marshal(legacy)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.ElementsMatch(t, []string{
		"id", "operationId", "scenario", "lane", "startedAt", "verifyAfter", "deadline",
	}, keys(fields))
	assert.Equal(t, json.RawMessage(`"legacy-1"`), fields["id"])
	assert.Equal(t, json.RawMessage(`""`), fields["operationId"])

	var decoded Operation
	require.NoError(t, json.Unmarshal([]byte(`{
		"id":"legacy-2","scenario":"message_soak","lane":"message_send",
		"startedAt":"2026-08-12T01:02:03Z","verifyAfter":"2026-08-12T01:02:03Z",
		"deadline":"2026-08-12T01:02:03Z"
	}`), &decoded))
	assert.Equal(t, "legacy-2", decoded.ID)
}

func TestFailureOperation_ValidationContract(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	valid := func() *Operation {
		return &Operation{
			SchemaVersion: 2, ID: "message-1", RunID: "run-1",
			Scenario: ScenarioMessageSoak, Lane: LaneMessageSend,
			OperationType: OperationMessageCreate, LifecycleState: OperationJournaled,
			StartedAt: now, VerifyAfter: now.Add(time.Second), Deadline: now.Add(time.Minute),
			Targets: map[string]string{"messageId": "message-1"},
			Effects: MessageCreateExpectedEffects(true, false, 1, "sha256"),
		}
	}

	require.NoError(t, ValidateOperation(valid()))
	assert.Error(t, ValidateOperation(nil))
	for _, test := range []struct {
		name   string
		mutate func(*Operation)
	}{
		{name: "missing identity", mutate: func(o *Operation) { o.ID = "" }},
		{name: "unknown scenario", mutate: func(o *Operation) { o.Scenario = "tenant" }},
		{name: "unknown lane", mutate: func(o *Operation) { o.Lane = "other" }},
		{name: "future schema", mutate: func(o *Operation) { o.SchemaVersion = 99 }},
		{name: "invalid lifecycle", mutate: func(o *Operation) { o.LifecycleState = "pending" }},
		{name: "unknown operation", mutate: func(o *Operation) { o.OperationType = "other" }},
		{name: "missing run", mutate: func(o *Operation) { o.RunID = "" }},
		{name: "unsupported effect", mutate: func(o *Operation) { o.Effects[0].Observer = "other" }},
		{name: "duplicate effect", mutate: func(o *Operation) { o.Effects = append(o.Effects, o.Effects[0]) }},
		{name: "cardinality mode", mutate: func(o *Operation) { o.Effects[2].Cardinality.Mode = "count" }},
		{name: "cardinality count", mutate: func(o *Operation) { o.Effects[2].Cardinality.Count = 0 }},
		{name: "cardinality hash", mutate: func(o *Operation) { o.Effects[2].Cardinality.SHA256 = "" }},
		{name: "missing timestamp", mutate: func(o *Operation) { o.StartedAt = time.Time{} }},
		{name: "verify before start", mutate: func(o *Operation) { o.VerifyAfter = now.Add(-time.Second) }},
		{name: "deadline before verify", mutate: func(o *Operation) { o.Deadline = now }},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation := valid()
			test.mutate(operation)
			assert.Error(t, ValidateOperation(operation))
		})
	}

	legacy := &Operation{
		ID: "legacy", Scenario: ScenarioMessageSoak, Lane: LaneMessageSend,
		StartedAt: now, VerifyAfter: now, Deadline: now,
	}
	assert.Error(t, ValidateOperation(legacy))
	legacy.Expected = []Observer{""}
	assert.Error(t, ValidateOperation(legacy))
	legacy.Expected = []Observer{ObserverAdmission, ObserverAdmission}
	assert.Error(t, ValidateOperation(legacy))
	legacy.Expected = []Observer{ObserverAdmission}
	require.NoError(t, ValidateOperation(legacy))
	assert.NotNil(t, legacy.Observations)
	assert.Equal(t, -1, legacy.HeapIndex())
}

func TestFailureObserverContract_DeclaresEachLaneAndOptionalObserver(t *testing.T) {
	contract := NewObserverContract(true, true)

	require.NoError(t, ValidateObserverContract(contract))
	assert.Equal(t, ObserverContractSchemaVersion, contract.SchemaVersion)
	assert.Equal(t, []Observer{
		ObserverAdmission, ObserverHistory, ObserverRecipient, ObserverRoomState, ObserverSearchIndex,
	}, contract.Observers)
	assert.Equal(t, []Observer{
		ObserverAdmission, ObserverHistory, ObserverRecipient, ObserverSearchIndex,
	}, contract.Lanes[LaneMessageSend])
	for _, lane := range []string{LaneMemberMutation, LaneRoomMutation, LaneRoomCreate, LaneReadReceipt} {
		assert.Equal(t, []Observer{ObserverAdmission, ObserverRoomState}, contract.Lanes[lane])
	}

	clone := CloneObserverContract(&contract)
	require.NotNil(t, clone)
	assert.True(t, EqualObserverContract(contract, *clone))
	clone.Lanes[LaneMessageSend][0] = ObserverHistory
	assert.False(t, EqualObserverContract(contract, *clone))
	assert.Nil(t, CloneObserverContract(nil))
	assert.True(t, OperationMatchesObserverContract(&Operation{
		Scenario: ScenarioMessageSoak, Lane: LaneMemberMutation,
		Expected: []Observer{ObserverRoomState, ObserverAdmission},
	}, contract))
	assert.False(t, OperationMatchesObserverContract(nil, contract))
}

func TestFailureExpectedEffects_MapOperationsToObservers(t *testing.T) {
	assert.Len(t, MessageCreateExpectedEffects(false, false, 0, ""), 2)
	assert.Len(t, MessageCreateExpectedEffects(true, true, 2, "hash"), 4)
	assert.Equal(t, EffectMemberState, MemberMutationExpectedEffects()[1].Effect)
	assert.Equal(t, EffectRoomName, RoomMutationExpectedEffects(OperationRoomRename)[1].Effect)
	assert.Equal(t, EffectSubscriptionMute, RoomMutationExpectedEffects(OperationMuteToggle)[1].Effect)
	assert.Equal(t, EffectSubscriptionRead, ReadReceiptExpectedEffects()[1].Effect)
	assert.Equal(t, EffectRoomCreated, RoomCreateExpectedEffects()[1].Effect)

	definition, ok := ObserverDefinitionFor(ObserverRoomState)
	require.True(t, ok)
	assert.Equal(t, ObserverQuery, definition.Mode)
	assert.True(t, definition.FinalReconciliation)
	_, ok = ObserverDefinitionFor("other")
	assert.False(t, ok)
	assert.True(t, ValidLane(LaneMessageSend))
	assert.False(t, ValidLane("other"))
	assert.True(t, ValidObservation(ObservationGood))
	assert.False(t, ValidObservation("other"))
	assert.True(t, ValidReason(ReasonNone))
	assert.False(t, ValidReason("other"))
	assert.True(t, ValidResult(ResultGood))
	assert.False(t, ValidResult("other"))
}

func TestFailureOperationResult_PreservesEvidencePrecedence(t *testing.T) {
	for _, test := range []struct {
		name         string
		lifecycle    OperationLifecycle
		observations map[Observer]Observation
		want         Result
	}{
		{name: "all good", observations: map[Observer]Observation{
			ObserverAdmission: ObservationGood, ObserverHistory: ObservationGood,
		}, want: ResultGood},
		{name: "accepted missing", lifecycle: OperationActive, observations: map[Observer]Observation{
			ObserverAdmission: ObservationGood, ObserverHistory: ObservationMissingAfterDeadline,
		}, want: ResultMissingAfterDeadline},
		{name: "rejected admission", lifecycle: OperationActive, observations: map[Observer]Observation{
			ObserverAdmission: ObservationBad, ObserverHistory: ObservationMissingAfterDeadline,
		}, want: ResultBad},
		{name: "ambiguous admission", lifecycle: OperationActive, observations: map[Observer]Observation{
			ObserverAdmission: ObservationUnverified, ObserverHistory: ObservationMissingAfterDeadline,
		}, want: ResultUnverified},
		{name: "bad outranks unverified", observations: map[Observer]Observation{
			ObserverAdmission: ObservationBad, ObserverHistory: ObservationUnverified,
		}, want: ResultBad},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, OperationResult(&Operation{
				LifecycleState: test.lifecycle, Observations: test.observations,
			}))
		})
	}

	operation := &Operation{
		Expected: []Observer{ObserverAdmission, ObserverHistory},
		Observations: map[Observer]Observation{
			ObserverAdmission: ObservationGood, ObserverHistory: ObservationBad,
		},
		ObservationReasons: map[Observer]Reason{ObserverHistory: ReasonHistoryContentMismatch},
	}
	assert.Equal(t, ReasonHistoryContentMismatch, OperationFinalReason(operation, ResultBad))
	assert.Equal(t, ReasonNone, OperationFinalReason(operation, ResultGood))
	assert.Equal(t, ReasonHistoryMissing,
		DefaultReason(ObserverHistory, ObservationMissingAfterDeadline))
	assert.Equal(t, ReasonNone, DefaultReason(ObserverSearchIndex, ObservationGood))
}

func TestFailureOperation_CloneOwnsMutableState(t *testing.T) {
	original := &Operation{
		Expected: []Observer{ObserverAdmission},
		Effects: []ExpectedEffect{{
			Effect: EffectRecipientEvent, Observer: ObserverRecipient,
			Cardinality: &Cardinality{Mode: "exact_set_hash", Count: 1, SHA256: "hash"},
		}},
		Targets:            map[string]string{"messageId": "message-1"},
		Attributes:         map[string]string{"phase": "measured"},
		Observations:       map[Observer]Observation{ObserverAdmission: ObservationGood},
		ObservationReasons: map[Observer]Reason{ObserverAdmission: ReasonNone},
		EvidenceRefs:       []string{"evidence-1"},
	}
	original.SetNextVerifyAt(time.Now())
	original.SetClaimed(true)
	original.SetHeapIndex(3)

	clone := CloneOperation(original)
	require.NotNil(t, clone)
	clone.Targets["messageId"] = "changed"
	clone.Effects[0].Cardinality.Count = 2
	clone.Expected[0] = ObserverHistory

	assert.Equal(t, "message-1", original.Targets["messageId"])
	assert.Equal(t, 1, original.Effects[0].Cardinality.Count)
	assert.Equal(t, ObserverAdmission, original.Expected[0])
	assert.Equal(t, -1, clone.HeapIndex())
	assert.True(t, clone.Claimed())
	assert.False(t, clone.NextVerifyAt().IsZero())
	emptyClone := CloneOperation(&Operation{})
	assert.NotNil(t, emptyClone.Targets)
	assert.NotNil(t, emptyClone.Attributes)
	assert.NotNil(t, emptyClone.Observations)
	assert.NotNil(t, emptyClone.ObservationReasons)
	assert.Nil(t, CloneOperation(nil))
}

func assertJSONFields(t *testing.T, value any, want []string) {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.ElementsMatch(t, want, keys(fields))
}

func keys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	return result
}
