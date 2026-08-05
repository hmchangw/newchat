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
		{"empty stays empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TruncateContent(tt.in))
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

func TestGuardedSetFields_Shape(t *testing.T) {
	pvw := &model.PreviewMessage{MessageID: "m1", Content: "$notAFieldPath"}
	fields := GuardedSetFields(pvw, 1754388000000)

	// Both guarded fields present, keyed for a $set pipeline stage.
	require.Contains(t, fields, "previewMessage")
	require.Contains(t, fields, "previewAsOf")

	// The preview doc must be wrapped in $literal so "$"-prefixed content
	// strings are never evaluated as aggregation field paths.
	cond := fields["previewMessage"].(bson.M)["$cond"].(bson.A)
	_, hasLiteral := cond[1].(bson.M)["$literal"]
	assert.True(t, hasLiteral, "preview doc must be $literal-wrapped")

	// Guard compares incoming asOf against $ifNull(previewAsOf, 0).
	gte := cond[0].(bson.M)["$gte"].(bson.A)
	assert.EqualValues(t, 1754388000000, gte[0])
}

func TestGuardedClearFields_Shape(t *testing.T) {
	fields := GuardedClearFields(1754388000000)

	// Both guarded fields present, keyed for a $set pipeline stage.
	require.Contains(t, fields, "previewMessage")
	require.Contains(t, fields, "previewAsOf")

	// The true branch REMOVES the stored preview rather than writing a value.
	cond := fields["previewMessage"].(bson.M)["$cond"].(bson.A)
	assert.Equal(t, "$$REMOVE", cond[1], "clear must drop the field, not store an empty doc")
	assert.Equal(t, "$previewMessage", cond[2], "guard failure must leave the stored preview untouched")

	// Same watermark rule as GuardedSetFields: asOf >= $ifNull(previewAsOf, 0).
	gte := cond[0].(bson.M)["$gte"].(bson.A)
	assert.EqualValues(t, 1754388000000, gte[0])
	ifNull := gte[1].(bson.M)["$ifNull"].(bson.A)
	assert.Equal(t, "$previewAsOf", ifNull[0])
	assert.EqualValues(t, 0, ifNull[1])

	// previewAsOf still advances on clear, so a redelivered older create can't
	// resurrect the cleared preview.
	asOfCond := fields["previewAsOf"].(bson.M)["$cond"].(bson.A)
	assert.EqualValues(t, 1754388000000, asOfCond[1])
	assert.Equal(t, "$previewAsOf", asOfCond[2])
}
