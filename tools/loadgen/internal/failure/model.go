package failure

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

type Observer string

const (
	ObserverAdmission   Observer = "admission"
	ObserverHistory     Observer = "cassandra_history"
	ObserverRecipient   Observer = "recipient_broadcast"
	ObserverRoomState   Observer = "room_state"
	ObserverSearchIndex Observer = "search_index"
)

const ObserverContractSchemaVersion = 2

const (
	ScenarioMessageSoak = "message_soak"

	LaneMessageSend    = "message_send"
	LaneMemberMutation = "member_mutation"
	LaneRoomMutation   = "room_mutation"
	LaneRoomCreate     = "room_create"
	LaneReadReceipt    = "read_receipt"
)

type ObserverContract struct {
	SchemaVersion            int                   `json:"schemaVersion"`
	Scenario                 string                `json:"scenario"`
	Observers                []Observer            `json:"observers"`
	Lanes                    map[string][]Observer `json:"lanes"`
	RecipientObserverEnabled bool                  `json:"recipientObserverEnabled"`
}

func NewObserverContract(recipientEnabled, searchEnabled bool) ObserverContract {
	messageObservers := []Observer{ObserverAdmission, ObserverHistory}
	if recipientEnabled {
		messageObservers = append(messageObservers, ObserverRecipient)
	}
	if searchEnabled {
		messageObservers = append(messageObservers, ObserverSearchIndex)
	}
	roomObservers := []Observer{ObserverAdmission, ObserverRoomState}
	observers := []Observer{ObserverAdmission, ObserverHistory, ObserverRoomState}
	if recipientEnabled {
		observers = append(observers, ObserverRecipient)
	}
	if searchEnabled {
		observers = append(observers, ObserverSearchIndex)
	}
	slices.Sort(observers)
	return ObserverContract{
		SchemaVersion: ObserverContractSchemaVersion,
		Scenario:      ScenarioMessageSoak,
		Observers:     observers,
		Lanes: map[string][]Observer{
			LaneMessageSend:    messageObservers,
			LaneMemberMutation: slices.Clone(roomObservers),
			LaneRoomMutation:   slices.Clone(roomObservers),
			LaneRoomCreate:     slices.Clone(roomObservers),
			LaneReadReceipt:    slices.Clone(roomObservers),
		},
		RecipientObserverEnabled: recipientEnabled,
	}
}

type OperationType string

const (
	OperationMessageCreate OperationType = "message_create"
	OperationMemberAdd     OperationType = "member_add"
	OperationMemberRemove  OperationType = "member_remove"
	OperationRoomRename    OperationType = "room_rename"
	OperationMuteToggle    OperationType = "mute_toggle"
	OperationRoomCreate    OperationType = "room_create"
	OperationMessageRead   OperationType = "message_read"
)

var operationTypeRegistry = map[OperationType]struct{}{
	OperationMessageCreate: {}, OperationMemberAdd: {}, OperationMemberRemove: {},
	OperationRoomRename: {}, OperationMuteToggle: {}, OperationRoomCreate: {},
	OperationMessageRead: {},
}

type OperationLifecycle string

const (
	OperationJournaled OperationLifecycle = "journaled"
	OperationActive    OperationLifecycle = "active"
)

type Effect string

const (
	EffectAdmission        Effect = "admission"
	EffectMessagePersisted Effect = "message_persisted"
	EffectRecipientEvent   Effect = "recipient_event"
	EffectMemberState      Effect = "member_state"
	EffectRoomName         Effect = "room_name"
	EffectSubscriptionMute Effect = "subscription_mute"
	EffectRoomCreated      Effect = "room_created"
	EffectSubscriptionRead Effect = "subscription_read"
	EffectMessageIndexed   Effect = "message_indexed"
)

type Cardinality struct {
	Mode   string `json:"mode"`
	Count  int    `json:"count"`
	SHA256 string `json:"sha256"`
}

type ExpectedEffect struct {
	Effect      Effect       `json:"effect"`
	Observer    Observer     `json:"observer"`
	Required    bool         `json:"required"`
	Cardinality *Cardinality `json:"cardinality,omitempty"`
}

func MessageCreateExpectedEffects(
	recipientEnabled bool,
	searchEnabled bool,
	recipientCount int,
	recipientHash string,
) []ExpectedEffect {
	effects := []ExpectedEffect{
		{Effect: EffectAdmission, Observer: ObserverAdmission, Required: true},
		{Effect: EffectMessagePersisted, Observer: ObserverHistory, Required: true},
	}
	if recipientEnabled {
		effects = append(effects, ExpectedEffect{
			Effect: EffectRecipientEvent, Observer: ObserverRecipient, Required: true,
			Cardinality: &Cardinality{Mode: "exact_set_hash", Count: recipientCount, SHA256: recipientHash},
		})
	}
	if searchEnabled {
		effects = append(effects, ExpectedEffect{
			Effect: EffectMessageIndexed, Observer: ObserverSearchIndex, Required: true,
		})
	}
	return effects
}

func MemberMutationExpectedEffects() []ExpectedEffect {
	return []ExpectedEffect{
		{Effect: EffectAdmission, Observer: ObserverAdmission, Required: true},
		{Effect: EffectMemberState, Observer: ObserverRoomState, Required: true},
	}
}

func RoomMutationExpectedEffects(operationType OperationType) []ExpectedEffect {
	effect := EffectRoomName
	if operationType == OperationMuteToggle {
		effect = EffectSubscriptionMute
	}
	return []ExpectedEffect{
		{Effect: EffectAdmission, Observer: ObserverAdmission, Required: true},
		{Effect: effect, Observer: ObserverRoomState, Required: true},
	}
}

func ReadReceiptExpectedEffects() []ExpectedEffect {
	return []ExpectedEffect{
		{Effect: EffectAdmission, Observer: ObserverAdmission, Required: true},
		{Effect: EffectSubscriptionRead, Observer: ObserverRoomState, Required: true},
	}
}

func RoomCreateExpectedEffects() []ExpectedEffect {
	return []ExpectedEffect{
		{Effect: EffectAdmission, Observer: ObserverAdmission, Required: true},
		{Effect: EffectRoomCreated, Observer: ObserverRoomState, Required: true},
	}
}

type Observation string

const (
	ObservationGood                 Observation = "good"
	ObservationBad                  Observation = "bad"
	ObservationUnverified           Observation = "unverified"
	ObservationMissingAfterDeadline Observation = "missing_after_deadline"
)

type Reason string

const (
	ReasonNone                      Reason = ""
	ReasonAdmissionRejected         Reason = "admission_rejected"
	ReasonHistoryContentMismatch    Reason = "history_content_mismatch"
	ReasonHistoryMissing            Reason = "history_missing"
	ReasonRecipientDuplicate        Reason = "recipient_duplicate"
	ReasonRecipientUnexpected       Reason = "recipient_unexpected"
	ReasonRecipientIdentityMismatch Reason = "recipient_identity_mismatch"
	ReasonRecipientMissing          Reason = "recipient_missing"
	ReasonPublishLocalError         Reason = "publish_local_error"
	ReasonMemberStateMismatch       Reason = "member_state_mismatch"
	ReasonRoomNameMismatch          Reason = "room_name_mismatch"
	ReasonMuteStateMismatch         Reason = "mute_state_mismatch"
	ReasonRoomStateMissing          Reason = "room_state_missing"
	ReasonReadStateRegressed        Reason = "read_state_regressed"
)

var reasonRegistry = map[Reason]struct{}{
	ReasonNone: {}, ReasonAdmissionRejected: {}, ReasonHistoryContentMismatch: {},
	ReasonHistoryMissing: {}, ReasonRecipientDuplicate: {}, ReasonRecipientUnexpected: {},
	ReasonRecipientIdentityMismatch: {}, ReasonRecipientMissing: {}, ReasonPublishLocalError: {},
	ReasonMemberStateMismatch: {}, ReasonRoomNameMismatch: {}, ReasonMuteStateMismatch: {},
	ReasonRoomStateMissing: {}, ReasonReadStateRegressed: {},
}

var ErrObserverContractMismatch = errors.New("failure observer contract mismatch")

type Result string

const (
	ResultGood                 Result = "good"
	ResultBad                  Result = "bad"
	ResultUnverified           Result = "unverified"
	ResultNotSent              Result = "not_sent"
	ResultMissingAfterDeadline Result = "missing_after_deadline"
)

type ObserverMode string

const (
	ObserverEvent ObserverMode = "event"
	ObserverQuery ObserverMode = "query"
	ObserverBoth  ObserverMode = "both"
)

type ObserverDefinition struct {
	Name                Observer
	Mode                ObserverMode
	Effects             []Effect
	FinalReconciliation bool
}

var observerRegistry = map[Observer]ObserverDefinition{
	ObserverAdmission: {Name: ObserverAdmission, Mode: ObserverEvent, Effects: []Effect{EffectAdmission}},
	ObserverHistory: {
		Name: ObserverHistory, Mode: ObserverQuery, Effects: []Effect{EffectMessagePersisted},
		FinalReconciliation: true,
	},
	ObserverRecipient: {
		Name: ObserverRecipient, Mode: ObserverEvent, Effects: []Effect{EffectRecipientEvent},
		FinalReconciliation: true,
	},
	ObserverRoomState: {
		Name: ObserverRoomState, Mode: ObserverQuery,
		Effects: []Effect{
			EffectMemberState, EffectRoomName, EffectSubscriptionMute,
			EffectRoomCreated, EffectSubscriptionRead,
		},
		FinalReconciliation: true,
	},
	ObserverSearchIndex: {
		Name: ObserverSearchIndex, Mode: ObserverQuery, Effects: []Effect{EffectMessageIndexed},
		FinalReconciliation: true,
	},
}

func ObserverDefinitionFor(observer Observer) (ObserverDefinition, bool) {
	definition, ok := observerRegistry[observer]
	definition.Effects = slices.Clone(definition.Effects)
	return definition, ok
}

type Operation struct {
	SchemaVersion      int                      `json:"schemaVersion,omitempty"`
	ID                 string                   `json:"operationId"`
	CorrelationID      string                   `json:"correlationId,omitempty"`
	RunID              string                   `json:"runId,omitempty"`
	Scenario           string                   `json:"scenario"`
	Lane               string                   `json:"lane"`
	OperationType      OperationType            `json:"operationType,omitempty"`
	StartedAt          time.Time                `json:"startedAt"`
	VerifyAfter        time.Time                `json:"verifyAfter"`
	Deadline           time.Time                `json:"deadline"`
	Targets            map[string]string        `json:"targets,omitempty"`
	Effects            []ExpectedEffect         `json:"expectedEffects,omitempty"`
	Expected           []Observer               `json:"expected,omitempty"`
	Attributes         map[string]string        `json:"attributes,omitempty"`
	Observations       map[Observer]Observation `json:"observations,omitempty"`
	ObservationReasons map[Observer]Reason      `json:"observationReasons,omitempty"`
	FinalResult        Result                   `json:"finalResult,omitempty"`
	FinalReason        Reason                   `json:"finalReason,omitempty"`
	EvidenceRefs       []string                 `json:"evidenceRefs,omitempty"`
	LifecycleState     OperationLifecycle       `json:"lifecycleState,omitempty"`

	nextVerifyAt time.Time
	claimed      bool
	heapIndex    int
}

//nolint:gocritic // A value receiver preserves json.Marshaler behavior for operation values and pointers.
func (o Operation) MarshalJSON() ([]byte, error) {
	type operationAlias Operation
	if o.SchemaVersion == 0 {
		legacy := struct {
			ID string `json:"id"`
			operationAlias
		}{ID: o.ID, operationAlias: operationAlias(o)}
		legacy.operationAlias.ID = ""
		return json.Marshal(legacy)
	}
	return json.Marshal(operationAlias(o))
}

func (o *Operation) UnmarshalJSON(data []byte) error {
	type operationAlias Operation
	var decoded struct {
		LegacyID string `json:"id"`
		operationAlias
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*o = Operation(decoded.operationAlias)
	if o.ID == "" {
		o.ID = decoded.LegacyID
	}
	return nil
}

func (o *Operation) NextVerifyAt() time.Time {
	if o == nil {
		return time.Time{}
	}
	return o.nextVerifyAt
}

func (o *Operation) SetNextVerifyAt(at time.Time) {
	if o != nil {
		o.nextVerifyAt = at
	}
}

func (o *Operation) Claimed() bool { return o != nil && o.claimed }

func (o *Operation) SetClaimed(claimed bool) {
	if o != nil {
		o.claimed = claimed
	}
}

func (o *Operation) HeapIndex() int {
	if o == nil {
		return -1
	}
	return o.heapIndex
}

func (o *Operation) SetHeapIndex(index int) {
	if o != nil {
		o.heapIndex = index
	}
}

func ValidateOperation(operation *Operation) error {
	if operation == nil {
		return fmt.Errorf("failure operation is required")
	}
	if operation.ID == "" || operation.Scenario == "" || operation.Lane == "" {
		return fmt.Errorf("failure operation requires ID, scenario, and lane")
	}
	if operation.Scenario != ScenarioMessageSoak {
		return fmt.Errorf("failure operation %q has unsupported scenario %q", operation.ID, operation.Scenario)
	}
	if !ValidLane(operation.Lane) {
		return fmt.Errorf("failure operation %q has unsupported lane %q", operation.ID, operation.Lane)
	}
	if operation.SchemaVersion != 0 && operation.SchemaVersion != 2 {
		return fmt.Errorf("failure operation %q has unsupported schema version %d", operation.ID, operation.SchemaVersion)
	}
	if operation.SchemaVersion == 2 {
		if operation.LifecycleState != OperationJournaled && operation.LifecycleState != OperationActive {
			return fmt.Errorf("failure operation %q has invalid lifecycle state %q", operation.ID, operation.LifecycleState)
		}
		if _, known := operationTypeRegistry[operation.OperationType]; !known {
			return fmt.Errorf("version 2 failure operation %q has unsupported type %q", operation.ID, operation.OperationType)
		}
		if operation.RunID == "" || len(operation.Targets) == 0 || len(operation.Effects) == 0 {
			return fmt.Errorf("version 2 failure operation %q requires run, type, targets, and effects", operation.ID)
		}
		operation.Expected = make([]Observer, 0, len(operation.Effects))
		seenEffects := make(map[string]struct{}, len(operation.Effects))
		for _, effect := range operation.Effects {
			definition, known := observerRegistry[effect.Observer]
			if !known || !slices.Contains(definition.Effects, effect.Effect) {
				return fmt.Errorf("failure operation %q has unsupported effect %q for observer %q", operation.ID, effect.Effect, effect.Observer)
			}
			key := string(effect.Effect) + "\x00" + string(effect.Observer)
			if _, duplicate := seenEffects[key]; duplicate {
				return fmt.Errorf("failure operation %q repeats effect %q", operation.ID, effect.Effect)
			}
			seenEffects[key] = struct{}{}
			if effect.Cardinality != nil && (effect.Cardinality.Mode != "exact_set_hash" || effect.Cardinality.Count <= 0 || effect.Cardinality.SHA256 == "") {
				return fmt.Errorf("failure operation %q has invalid effect cardinality", operation.ID)
			}
			if effect.Required {
				operation.Expected = append(operation.Expected, effect.Observer)
			}
		}
	}
	if operation.StartedAt.IsZero() || operation.VerifyAfter.IsZero() || operation.Deadline.IsZero() {
		return fmt.Errorf("failure operation %q requires timestamps", operation.ID)
	}
	if operation.VerifyAfter.Before(operation.StartedAt) {
		return fmt.Errorf("failure operation %q verify time precedes start", operation.ID)
	}
	if operation.Deadline.Before(operation.VerifyAfter) {
		return fmt.Errorf("failure operation %q deadline precedes verification", operation.ID)
	}
	if len(operation.Expected) == 0 {
		return fmt.Errorf("failure operation %q requires an observer", operation.ID)
	}
	seen := make(map[Observer]struct{}, len(operation.Expected))
	for _, observer := range operation.Expected {
		if observer == "" {
			return fmt.Errorf("failure operation %q has an empty observer", operation.ID)
		}
		if _, duplicate := seen[observer]; duplicate {
			return fmt.Errorf("failure operation %q repeats observer %q", operation.ID, observer)
		}
		seen[observer] = struct{}{}
	}
	if operation.Observations == nil {
		operation.Observations = make(map[Observer]Observation)
	}
	operation.heapIndex = -1
	return nil
}

func ValidObservation(observation Observation) bool {
	switch observation {
	case ObservationGood, ObservationBad, ObservationUnverified, ObservationMissingAfterDeadline:
		return true
	default:
		return false
	}
}

func ValidReason(reason Reason) bool {
	_, known := reasonRegistry[reason]
	return known
}

func ValidResult(result Result) bool {
	switch result {
	case ResultGood, ResultBad, ResultUnverified, ResultNotSent, ResultMissingAfterDeadline:
		return true
	default:
		return false
	}
}

func OperationResult(operation *Operation) Result {
	result := ResultGood
	admissionAccepted := operation.Observations[ObserverAdmission] == ObservationGood
	for observer, observation := range operation.Observations {
		switch observation {
		case ObservationGood:
		case ObservationMissingAfterDeadline:
			if observer == ObserverAdmission || operation.LifecycleState == OperationJournaled || !admissionAccepted {
				if result == ResultGood {
					result = ResultUnverified
				}
				continue
			}
			return ResultMissingAfterDeadline
		case ObservationBad:
			result = ResultBad
		case ObservationUnverified:
			if result == ResultGood {
				result = ResultUnverified
			}
		}
	}
	return result
}

func OperationFinalReason(operation *Operation, result Result) Reason {
	var terminalObservation Observation
	switch result {
	case ResultBad:
		terminalObservation = ObservationBad
	case ResultMissingAfterDeadline:
		terminalObservation = ObservationMissingAfterDeadline
	default:
		return ReasonNone
	}
	for _, observer := range operation.Expected {
		if operation.Observations[observer] != terminalObservation {
			continue
		}
		if reason := operation.ObservationReasons[observer]; reason != ReasonNone {
			return reason
		}
	}
	return ReasonNone
}

func CloneOperation(operation *Operation) *Operation {
	if operation == nil {
		return nil
	}
	cloned := *operation
	cloned.heapIndex = -1
	cloned.Expected = slices.Clone(operation.Expected)
	cloned.Effects = slices.Clone(operation.Effects)
	for index := range cloned.Effects {
		if operation.Effects[index].Cardinality != nil {
			cardinality := *operation.Effects[index].Cardinality
			cloned.Effects[index].Cardinality = &cardinality
		}
	}
	cloned.Targets = make(map[string]string, len(operation.Targets))
	for key, value := range operation.Targets {
		cloned.Targets[key] = value
	}
	cloned.EvidenceRefs = slices.Clone(operation.EvidenceRefs)
	cloned.Attributes = make(map[string]string, len(operation.Attributes))
	for key, value := range operation.Attributes {
		cloned.Attributes[key] = value
	}
	cloned.Observations = make(map[Observer]Observation, len(operation.Observations))
	for observer, observation := range operation.Observations {
		cloned.Observations[observer] = observation
	}
	cloned.ObservationReasons = make(map[Observer]Reason, len(operation.ObservationReasons))
	for observer, reason := range operation.ObservationReasons {
		cloned.ObservationReasons[observer] = reason
	}
	return &cloned
}

func DefaultReason(observer Observer, observation Observation) Reason {
	switch {
	case observer == ObserverAdmission && observation == ObservationBad:
		return ReasonAdmissionRejected
	case observer == ObserverHistory && observation == ObservationBad:
		return ReasonHistoryContentMismatch
	case observer == ObserverHistory && observation == ObservationMissingAfterDeadline:
		return ReasonHistoryMissing
	case observer == ObserverRecipient && observation == ObservationBad:
		return ReasonRecipientIdentityMismatch
	case observer == ObserverRecipient && observation == ObservationMissingAfterDeadline:
		return ReasonRecipientMissing
	default:
		return ReasonNone
	}
}

func ValidateRegisteredObservers(observers []Observer) error {
	seen := make(map[Observer]struct{}, len(observers))
	for _, observer := range observers {
		if _, ok := observerRegistry[observer]; !ok {
			return fmt.Errorf("unsupported failure observer %q", observer)
		}
		if _, duplicate := seen[observer]; duplicate {
			return fmt.Errorf("duplicate failure observer %q", observer)
		}
		seen[observer] = struct{}{}
	}
	return nil
}

func ValidateObserverContract(contract ObserverContract) error {
	if contract.SchemaVersion != ObserverContractSchemaVersion {
		return fmt.Errorf("unsupported observer contract schema version %d", contract.SchemaVersion)
	}
	if contract.Scenario != ScenarioMessageSoak {
		return fmt.Errorf("observer contract scenario must be %q", ScenarioMessageSoak)
	}
	if err := ValidateRegisteredObservers(contract.Observers); err != nil {
		return err
	}
	if len(contract.Lanes) == 0 {
		return fmt.Errorf("observer contract must declare at least one lane")
	}
	for lane, observers := range contract.Lanes {
		if !ValidLane(lane) {
			return fmt.Errorf("observer contract declares unsupported lane %q", lane)
		}
		if len(observers) == 0 {
			return fmt.Errorf("observer contract lane %q declares no observer", lane)
		}
		if err := ValidateRegisteredObservers(observers); err != nil {
			return fmt.Errorf("observer contract lane %q: %w", lane, err)
		}
	}
	hasRecipient := slices.Contains(contract.Lanes[LaneMessageSend], ObserverRecipient)
	if hasRecipient != contract.RecipientObserverEnabled {
		return fmt.Errorf("recipient observer enablement does not match configured observers")
	}
	return nil
}

func EqualObserverContract(left, right ObserverContract) bool {
	return left.SchemaVersion == right.SchemaVersion &&
		left.Scenario == right.Scenario &&
		left.RecipientObserverEnabled == right.RecipientObserverEnabled &&
		slices.Equal(left.Observers, right.Observers) &&
		maps.EqualFunc(left.Lanes, right.Lanes, slices.Equal)
}

func CloneObserverContract(contract *ObserverContract) *ObserverContract {
	if contract == nil {
		return nil
	}
	cloned := *contract
	cloned.Observers = slices.Clone(contract.Observers)
	if contract.Lanes != nil {
		cloned.Lanes = make(map[string][]Observer, len(contract.Lanes))
		for lane, observers := range contract.Lanes {
			cloned.Lanes[lane] = slices.Clone(observers)
		}
	}
	return &cloned
}

func OperationMatchesObserverContract(operation *Operation, contract ObserverContract) bool {
	if operation == nil || operation.Scenario != contract.Scenario {
		return false
	}
	configured, known := contract.Lanes[operation.Lane]
	if !known {
		return false
	}
	expected := slices.Clone(operation.Expected)
	slices.Sort(expected)
	laneObservers := slices.Clone(configured)
	slices.Sort(laneObservers)
	return slices.Equal(expected, laneObservers)
}

func ValidLane(lane string) bool {
	switch lane {
	case LaneMessageSend, LaneMemberMutation, LaneRoomMutation, LaneRoomCreate, LaneReadReceipt:
		return true
	default:
		return false
	}
}
