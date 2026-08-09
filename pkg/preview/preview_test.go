package preview

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

func TestTruncateContent(t *testing.T) {
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
		VisibleTo: "alice",
	})
	assert.Equal(t, "m1", got.MessageID)
	assert.Equal(t, strings.Repeat("x", MaxContentRunes), got.Content)
	assert.Equal(t, time.UTC, got.CreatedAt.Location())
	assert.Equal(t, "alice", got.VisibleTo)
}

func TestBotAwareDisplayName(t *testing.T) {
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
	gte := cond[0].(bson.M)["$gte"].(bson.A)
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
