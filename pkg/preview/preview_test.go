package preview

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func TestTruncateContent_RuneBoundaries(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"short passes through", "hello", "hello"},
		{"exactly at cap unchanged", strings.Repeat("a", MaxContentRunes), strings.Repeat("a", MaxContentRunes)},
		{"over cap truncated", strings.Repeat("a", MaxContentRunes+1), strings.Repeat("a", MaxContentRunes)},
		{"multi-byte runes counted as runes not bytes", strings.Repeat("好", MaxContentRunes+3), strings.Repeat("好", MaxContentRunes)},
		// 3 bytes per rune, so this is 3x over the cap in BYTES but exactly at it
		// in runes: it must come back untouched.
		{"multi-byte exactly at cap unchanged", strings.Repeat("好", MaxContentRunes), strings.Repeat("好", MaxContentRunes)},
		{"multi-byte over the byte cap but under the rune cap", strings.Repeat("好", 200), strings.Repeat("好", 200)},
		{"empty stays empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, truncateContent(tt.in))
		})
	}
}

func TestBuild_TruncatesAndNormalizesUTC(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Taipei")
	require.NoError(t, err)
	long := strings.Repeat("x", MaxContentRunes+50)
	got := Build(model.PreviewMessage{
		MessageID: "m1",
		Sender:    model.Participant{Account: "alice"},
		Content:   long,
		CreatedAt: time.Date(2026, 8, 5, 18, 0, 0, 0, loc),
	})
	assert.Equal(t, "m1", got.MessageID)
	assert.Equal(t, strings.Repeat("x", MaxContentRunes), got.Content)
	assert.Equal(t, time.UTC, got.CreatedAt.Location())
}

// Content was capped from the start; the collections were not, so one wide message could
// size the writer's buffer, the stored document and every read of it at once (#290).
func TestBuild_CapsAttachmentsAndMentions(t *testing.T) {
	atts := make([]cassandra.Attachment, MaxAttachments+5)
	for i := range atts {
		atts[i] = cassandra.Attachment{ID: "f" + strconv.Itoa(i)}
	}
	mentions := make([]model.Participant, MaxMentions+7)
	for i := range mentions {
		mentions[i] = model.Participant{Account: "u" + strconv.Itoa(i)}
	}

	got := Build(model.PreviewMessage{MessageID: "m1", Attachments: atts, Mentions: mentions})

	require.Len(t, got.Attachments, MaxAttachments)
	require.Len(t, got.Mentions, MaxMentions)
	assert.Equal(t, "f0", got.Attachments[0].ID, "the cap keeps the head, not an arbitrary window")
	assert.Equal(t, "u0", got.Mentions[0].Account)
}

// Under the cap nothing is touched — including the nil case, which must stay nil rather
// than becoming an empty slice that would serialize as [] instead of being omitted.
func TestBuild_LeavesCollectionsUnderTheCapAlone(t *testing.T) {
	got := Build(model.PreviewMessage{
		MessageID:   "m1",
		Attachments: []cassandra.Attachment{{ID: "f1"}},
	})
	require.Len(t, got.Attachments, 1)
	assert.Nil(t, got.Mentions, "an absent collection must not be materialized")
}

func TestBotAwareDisplayName_Fallbacks(t *testing.T) {
	appName := func(name string, err error) AppNameLookup {
		return func(context.Context, string) (string, error) { return name, err }
	}
	tests := []struct {
		name    string
		account string
		lookup  AppNameLookup
		want    string
	}{
		// displayfmt.CombineWithFallback(eng, chinese, account) composes the
		// human name; assert against its real output for ("Alice","愛麗絲",...).
		{"human ignores lookup", "alice", appName("ShouldNotAppear", nil), ""},
		{"bot uses app name", "helper.bot", appName("Helper App", nil), "Helper App"},
		{"bot lookup error falls back composed", "helper.bot", appName("", errors.New("db down")), ""},
		{"bot empty app name falls back composed", "helper.bot", appName("", nil), ""},
		{"nil lookup falls back composed", "helper.bot", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BotAwareDisplayName(context.Background(), tt.lookup, "Alice", "愛麗絲", tt.account)
			if tt.want == "" {
				// Fallback path: must equal the composed name, not the app name.
				composed := BotAwareDisplayName(context.Background(), nil, "Alice", "愛麗絲", "alice")
				assert.Equal(t, composed, got)
				assert.NotContains(t, got, "ShouldNotAppear")
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// NOTE: "helper.bot" carries model.IsBot's recognized suffix (".bot").

// assertGuard checks the shared watermark rule: asOf >= $ifNull(previewAsOf, 0).
func assertGuard(t *testing.T, cond bson.A, asOf int64) {
	t.Helper()
	assertGuardExpr(t, cond[0].(bson.M), asOf)
}

// assertGuardExpr checks the watermark expression itself, for callers that nest it
// inside a larger condition.
func assertGuardExpr(t *testing.T, expr bson.M, asOf int64) {
	t.Helper()
	gte := expr["$gte"].(bson.A)
	assert.EqualValues(t, asOf, gte[0])
	ifNull := gte[1].(bson.M)["$ifNull"].(bson.A)
	assert.Equal(t, "$previewAsOf", ifNull[0])
	assert.EqualValues(t, 0, ifNull[1])
}

func TestGuardedSetFields_Shape(t *testing.T) {
	sealed := Sealed{
		Meta:       model.PreviewMeta{MessageID: "m1", Sender: model.Participant{DisplayName: "$notAFieldPath"}},
		Ciphertext: []byte{0x01},
		Nonce:      []byte{0x02},
		KeyEpoch:   3,
		ForMsgID:   "m2",
	}
	fields := GuardedSetFields(sealed, 1754388000000)

	require.Contains(t, fields, "previewAsOf")
	for _, f := range previewDocFields {
		require.Contains(t, fields, f)

		cond := fields[f].(bson.M)["$cond"].(bson.A)
		// Every stored value is $literal-wrapped so a "$"-prefixed string is
		// never evaluated as an aggregation field path.
		_, hasLiteral := cond[1].(bson.M)["$literal"]
		assert.True(t, hasLiteral, "%s must be $literal-wrapped", f)
		// Guard failure leaves the existing value untouched.
		assert.Equal(t, "$"+f, cond[2], "%s must be preserved when the guard fails", f)
		assertGuard(t, cond, 1754388000000)
	}

	asOfCond := fields["previewAsOf"].(bson.M)["$cond"].(bson.A)
	assert.EqualValues(t, 1754388000000, asOfCond[1])
	assert.Equal(t, "$previewAsOf", asOfCond[2])
}

func TestGuardedClearFields_Shape(t *testing.T) {
	fields := GuardedClearFields(1754388000000)

	require.Contains(t, fields, "previewAsOf")
	for _, f := range previewDocFields {
		require.Contains(t, fields, f)

		cond := fields[f].(bson.M)["$cond"].(bson.A)
		assert.Equal(t, "$$REMOVE", cond[1], "%s must be dropped, not written empty", f)
		assert.Equal(t, "$"+f, cond[2], "%s must be preserved when the guard fails", f)
		assertGuard(t, cond, 1754388000000)
	}

	// previewAsOf still advances on clear, so a redelivered older write can't
	// resurrect the cleared preview.
	asOfCond := fields["previewAsOf"].(bson.M)["$cond"].(bson.A)
	assert.EqualValues(t, 1754388000000, asOfCond[1])
	assert.Equal(t, "$previewAsOf", asOfCond[2])
}

// Set and clear must touch exactly the same keys, or a partial clear strands a
// fragment: a nonce and epoch describing a ciphertext that no longer exists.
func TestGuardedSetAndClear_CoverIdenticalFields(t *testing.T) {
	set := GuardedSetFields(Sealed{}, 1)
	clear := GuardedClearFields(1)

	setKeys := make([]string, 0, len(set))
	for k := range set {
		setKeys = append(setKeys, k)
	}
	clearKeys := make([]string, 0, len(clear))
	for k := range clear {
		clearKeys = append(clearKeys, k)
	}
	assert.ElementsMatch(t, setKeys, clearKeys)

	// And both cover the canonical list plus the watermark itself.
	assert.Len(t, setKeys, len(previewDocFields)+1)
}

// Advancing the freshness key must not disturb the body. This is the ineligible-insert
// case: a system message becomes the room's newest, so previewForMsgId has to follow
// lastMsgId or the identity check reports the (still correct) preview as stale.
func TestGuardedAdvanceKeyFields_MovesOnlyTheKey(t *testing.T) {
	fields := GuardedAdvanceKeyFields("m-newest", 1754388000000)

	require.Contains(t, fields, "previewAsOf")
	require.Contains(t, fields, "previewForMsgId")

	keyCond := fields["previewForMsgId"].(bson.M)["$cond"].(bson.A)
	assert.Equal(t, bson.M{"$literal": "m-newest"}, keyCond[1])
	assert.Equal(t, "$previewForMsgId", keyCond[2])
	// Two conjuncts now: the watermark, and "the key was already current".
	and := keyCond[0].(bson.M)["$and"].(bson.A)
	require.Len(t, and, 2)
	assertGuardExpr(t, and[0].(bson.M), 1754388000000)

	for _, f := range previewBodyFields {
		assert.NotContains(t, fields, f, "%s is the body; advancing the key must leave it alone", f)
	}
}

// The mutation write is an update, never a creation: under eager persistence an insert
// is the only thing that may mint a preview, because only an insert knows the
// previewForMsgId that makes one readable.
func TestGuardedUpdateBodyFields_MovesOnlyTheBodyAndRequiresExisting(t *testing.T) {
	sealed := Sealed{
		Meta:       model.PreviewMeta{MessageID: "m1", Sender: model.Participant{DisplayName: "$notAFieldPath"}},
		Ciphertext: []byte{0x01},
		Nonce:      []byte{0x02},
		KeyEpoch:   3,
		ForMsgID:   "ignored",
	}
	fields := GuardedUpdateBodyFields(sealed, "m-observed", 1754388000000)

	require.Contains(t, fields, "previewAsOf")
	assert.NotContains(t, fields, "previewForMsgId",
		"a mutation does not move lastMsgId, so the freshness key must survive untouched")

	for _, f := range previewBodyFields {
		require.Contains(t, fields, f)
		cond := fields[f].(bson.M)["$cond"].(bson.A)
		_, hasLiteral := cond[1].(bson.M)["$literal"]
		assert.True(t, hasLiteral, "%s must be $literal-wrapped", f)
		assert.Equal(t, "$"+f, cond[2], "%s must be preserved when the guard fails", f)

		// Two conjuncts: the watermark, and the stored key equalling the observed one.
		// The equality also refuses to create — a missing key reads as "" and cannot
		// equal a non-empty observed id, so only an insert may mint a preview.
		and := cond[0].(bson.M)["$and"].(bson.A)
		require.Len(t, and, 2)
		assertGuardExpr(t, and[0].(bson.M), 1754388000000)
		assert.Equal(t, bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$previewForMsgId", ""}}, "m-observed"}}, and[1])
	}
}

// The body write is pinned to the key the walk OBSERVED. Without that conjunct an
// insert landing between the walk and this write advances previewForMsgId, the older
// body is stored under the newer key, and the reader's identity check passes on the
// mismatch — the #224 shape, reached by a different route.
func TestGuardedUpdateBodyFields_PinsTheBodyToTheObservedKey(t *testing.T) {
	sealed := Sealed{
		Meta:       model.PreviewMeta{MessageID: "m1"},
		Ciphertext: []byte{0x01},
		Nonce:      []byte{0x02},
		KeyEpoch:   3,
	}
	fields := GuardedUpdateBodyFields(sealed, "m-observed", 1754388000000)

	// The guard lives inside each field's $cond; rendering the whole update is the
	// least brittle way to assert on it without mirroring the pipeline's shape here.
	rendered := fmt.Sprintf("%v", fields)
	assert.Contains(t, rendered, "m-observed",
		"the guard must compare the stored freshness key against the observed one")
	assert.Contains(t, rendered, "previewForMsgId",
		"the comparison must read the stored key, not just the incoming one")
}

// A key-only advance may only run while the stored key still matches the stored
// lastMsgId. Without that conjunct it can REVALIDATE a stale body: if an eligible
// message's body write lost the watermark while unguarded lastMsgId advanced, the
// resulting mismatch is what withholds the stale preview — and a later ineligible
// message would otherwise heal it by moving the key back into agreement.
func TestGuardedAdvanceKeyFields_OnlyAdvancesAnAlreadyCurrentKey(t *testing.T) {
	fields := GuardedAdvanceKeyFields("m-newest", 1754388000000)

	keyCond := fields["previewForMsgId"].(bson.M)["$cond"].(bson.A)
	and, ok := keyCond[0].(bson.M)["$and"].(bson.A)
	require.True(t, ok, "the advance must be conjunctive, not the watermark alone")
	require.Len(t, and, 2)

	rendered := fmt.Sprintf("%v", and)
	assert.Contains(t, rendered, "previewForMsgId",
		"the guard must read the stored freshness key")
	assert.Contains(t, rendered, "lastMsgId",
		"the guard must compare it against the stored lastMsgId (pre-update in a $set stage)")
}

// The invalidate is keyed on the STORED BODY's own message id, not on the freshness key.
// The key does not identify what the body describes — an ineligible insert advances it
// over an untouched body — so pinning to it would miss exactly the case this repairs.
func TestGuardedInvalidateKeyFields_KeysOnTheStoredBody(t *testing.T) {
	fields := GuardedInvalidateKeyFields("m-mutated")

	require.Contains(t, fields, "previewForMsgId")
	cond := fields["previewForMsgId"].(bson.M)["$cond"].(bson.A)
	assert.Equal(t,
		bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$previewMeta.messageId", ""}}, "m-mutated"}},
		cond[0], "the predicate must compare the stored body's message id against the mutated one")
	assert.Equal(t, "$$REMOVE", cond[1], "a passing guard withdraws the key")
	assert.Equal(t, "$previewForMsgId", cond[2], "a failing guard leaves the key alone")
}

// Withdrawing the key without the watermark that protects it would strand the room: the
// warm-back meant to refill it is watermark-guarded, so a future-stamped previewAsOf
// (clock skew, a future-dated insert) would reject the repair as well as the write that
// failed. They go together, under one predicate, so the room cannot end up certified-by
// -nothing and un-repairable at once.
func TestGuardedInvalidateKeyFields_WithdrawsTheWatermarkWithTheKey(t *testing.T) {
	fields := GuardedInvalidateKeyFields("m-mutated")

	require.Contains(t, fields, "previewAsOf")
	key := fields["previewForMsgId"].(bson.M)["$cond"].(bson.A)
	mark := fields["previewAsOf"].(bson.M)["$cond"].(bson.A)
	assert.Equal(t, key[0], mark[0], "both must move under the same predicate or neither is safe")
	assert.Equal(t, "$$REMOVE", mark[1])
	assert.Equal(t, "$previewAsOf", mark[2])
}

// This repair follows a write that did NOT land, and a watermark comparison is one of the
// ways such a write fails to land. Guarding the repair on the watermark would reject it
// for the same reason, which is the failure rather than a safeguard against it.
func TestGuardedInvalidateKeyFields_IsNotItselfWatermarkGuarded(t *testing.T) {
	fields := GuardedInvalidateKeyFields("m-mutated")

	assert.NotContains(t, fmt.Sprintf("%v", fields), "$gte",
		"a watermark conjunct would reject exactly the writes this repair exists to follow")
}

// The body survives. The reader is what stops trusting it, and the walk that replaces it
// runs on the next read; clearing it here would be a destructive write authorised by a
// mutation that, by definition, failed to establish what the room now holds.
func TestGuardedInvalidateKeyFields_LeavesTheBodyIntact(t *testing.T) {
	fields := GuardedInvalidateKeyFields("m-mutated")

	for _, f := range previewBodyFields {
		assert.NotContains(t, fields, f, "%s must survive; only its certification is withdrawn", f)
	}
	assert.Len(t, fields, 2, "the key and its watermark, nothing else")
}
