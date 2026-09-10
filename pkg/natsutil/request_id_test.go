package natsutil_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestWithRequestID_RoundTrip(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "req-abc-123")
	assert.Equal(t, "req-abc-123", natsutil.RequestIDFromContext(ctx))
}

func TestWithRequestID_EmptyIsNoOp(t *testing.T) {
	parent := context.Background()
	ctx := natsutil.WithRequestID(parent, "")
	assert.True(t, ctx == parent, "empty id must return the parent ctx unchanged")
	assert.Equal(t, "", natsutil.RequestIDFromContext(ctx))
}

func TestWithRequestID_OverwritesExistingValue(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "first")
	ctx = natsutil.WithRequestID(ctx, "second")
	assert.Equal(t, "second", natsutil.RequestIDFromContext(ctx))
}

func TestRequestIDFromContext_MissingReturnsEmpty(t *testing.T) {
	assert.Equal(t, "", natsutil.RequestIDFromContext(context.Background()))
}

func TestHeaderForContext_WithID(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "req-xyz")
	h := natsutil.HeaderForContext(ctx)
	assert.NotNil(t, h)
	assert.Equal(t, "req-xyz", h.Get(natsutil.RequestIDHeader))
}

func TestHeaderForContext_WithoutIDReturnsNil(t *testing.T) {
	h := natsutil.HeaderForContext(context.Background())
	assert.Nil(t, h, "no request ID in ctx must return a nil header (not an empty one)")
}

func TestHeaderForContext_RoundTripViaStampRequestID(t *testing.T) {
	original := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	h := natsutil.HeaderForContext(original)
	recovered, id := natsutil.StampRequestID(context.Background(), h, "")
	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", id)
	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", natsutil.RequestIDFromContext(recovered))
}

func TestRequestIDHeader_Constant(t *testing.T) {
	assert.Equal(t, "X-Request-ID", natsutil.RequestIDHeader)
}

func TestNewMsg_AttachesHeaderFromContext(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "req-newmsg-test")
	msg := natsutil.NewMsg(ctx, "chat.foo.bar", []byte("payload"))
	assert.Equal(t, "chat.foo.bar", msg.Subject)
	assert.Equal(t, []byte("payload"), msg.Data)
	assert.Equal(t, "req-newmsg-test", msg.Header.Get(natsutil.RequestIDHeader))
}

func TestNewMsg_NoIDLeavesHeaderNil(t *testing.T) {
	msg := natsutil.NewMsg(context.Background(), "chat.foo.bar", []byte("payload"))
	assert.Nil(t, msg.Header)
}

func TestStampRequestID(t *testing.T) {
	const validUUID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	cases := []struct {
		name    string
		headers nats.Header
		wantID  string // "" means "any minted UUID"
	}{
		{
			name:    "valid_uuid_passes_through",
			headers: nats.Header{natsutil.RequestIDHeader: []string{validUUID}},
			wantID:  validUUID,
		},
		{
			name:    "nil_headers_mints_fresh",
			headers: nil,
		},
		{
			name:    "empty_headers_mints_fresh",
			headers: nats.Header{},
		},
		{
			name:    "empty_value_mints_fresh",
			headers: nats.Header{natsutil.RequestIDHeader: []string{""}},
		},
		{
			name:    "malformed_value_mints_fresh",
			headers: nats.Header{natsutil.RequestIDHeader: []string{"not-a-uuid"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, id := natsutil.StampRequestID(context.Background(), tc.headers, "chat.test.subject")
			assert.NotEmpty(t, id)
			assert.Equal(t, id, natsutil.RequestIDFromContext(ctx),
				"id must be stamped on returned ctx")
			if tc.wantID != "" {
				assert.Equal(t, tc.wantID, id)
			} else {
				assert.Len(t, id, 36, "minted id must be a 36-char hyphenated UUID")
			}
		})
	}
}

// RequireRequestID is the strict variant used on entry points whose downstream
// pipeline derives JetStream Nats-Msg-Id / Mongo dedup keys from the request
// ID. Silently minting at the server would break client-retry deduplication
// for those paths, so missing/malformed inbound headers must produce a typed
// BadRequest instead.
func TestRequireRequestID(t *testing.T) {
	const validUUID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

	t.Run("valid_uuid_passes_through_and_stamps_ctx", func(t *testing.T) {
		h := nats.Header{natsutil.RequestIDHeader: []string{validUUID}}
		ctx, id, err := natsutil.RequireRequestID(context.Background(), h, "chat.test.subject")
		require.NoError(t, err)
		assert.Equal(t, validUUID, id)
		assert.Equal(t, validUUID, natsutil.RequestIDFromContext(ctx))
	})

	cases := []struct {
		name    string
		headers nats.Header
	}{
		{name: "nil_headers_rejects", headers: nil},
		{name: "empty_headers_rejects", headers: nats.Header{}},
		{name: "empty_value_rejects", headers: nats.Header{natsutil.RequestIDHeader: []string{""}}},
		{name: "malformed_value_rejects", headers: nats.Header{natsutil.RequestIDHeader: []string{"not-a-uuid"}}},
		{name: "wrong_length_rejects", headers: nats.Header{natsutil.RequestIDHeader: []string{"01970a4f8c2d7c9aabcde0123456789f"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, id, err := natsutil.RequireRequestID(context.Background(), tc.headers, "chat.test.subject")
			require.Error(t, err)
			assert.Empty(t, id, "no id should be returned on reject")
			assert.Empty(t, natsutil.RequestIDFromContext(ctx),
				"ctx must not carry a minted id on reject (callers may still want to log against the inbound ctx)")

			var ec *errcode.Error
			require.True(t, errors.As(err, &ec), "must return a typed *errcode.Error so errnats.Reply maps it to BadRequest")
			assert.Equal(t, errcode.CodeBadRequest, ec.Code)
		})
	}
}

func TestNewMsgEncoded_SetsEncodingHeader(t *testing.T) {
	// No request-id in ctx: NewMsg returns a nil Header; the helper owns the guard.
	msg := natsutil.NewMsgEncoded(context.Background(), "chat.hr.a.users.upsert", []byte("z"), natsutil.EncodingZstd)
	assert.Equal(t, natsutil.EncodingZstd, msg.Header.Get(natsutil.HeaderNatsEncoding))
}

func TestNewMsgEncoded_KeepsRequestIDHeader(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "req-encoded-test")
	msg := natsutil.NewMsgEncoded(ctx, "chat.foo", []byte("z"), natsutil.EncodingZstd)
	assert.Equal(t, "req-encoded-test", msg.Header.Get(natsutil.RequestIDHeader))
	assert.Equal(t, natsutil.EncodingZstd, msg.Header.Get(natsutil.HeaderNatsEncoding))
}

func TestNewMsgEncoded_EmptyEncodingMatchesNewMsg(t *testing.T) {
	msg := natsutil.NewMsgEncoded(context.Background(), "chat.foo", []byte("p"), "")
	assert.Nil(t, msg.Header)
}

// TestInboxDedupID_DistinguishesEventTypes is the regression guard for a silent
// federation data-loss bug: one handler pass publishing several event types to
// the same peer under the same request ID produced one identical Nats-Msg-Id
// for all of them, so JetStream's stream-level dedup dropped every publish after
// the first.
//
// room-worker's Teams membership sync does exactly that — joinedAt refresh, then
// member_added, then member_removed, same ctx, same destination — so departed
// members were never removed at their home site.
func TestInboxDedupID_DistinguishesEventTypes(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")

	refreshed := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberJoinedAtRefreshed, "seed")
	added := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberAdded, "seed")
	removed := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberRemoved, "seed")

	assert.NotEqual(t, added, removed,
		"member_added and member_removed to one peer in one pass must not collide")
	assert.NotEqual(t, added, refreshed)
	assert.NotEqual(t, removed, refreshed)
}

// TestInboxDedupID_StableForRedelivery pins the property the dedup ID exists
// for: the same logical publish, retried, must produce the same ID so the
// destination stores it once.
func TestInboxDedupID_StableForRedelivery(t *testing.T) {
	ctx := natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")

	first := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberAdded, "seed")
	second := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberAdded, "seed")
	assert.Equal(t, first, second, "a redelivery of the same event must dedup")

	other := natsutil.InboxDedupID(ctx, "site-c", model.InboxMemberAdded, "seed")
	assert.NotEqual(t, first, other, "different destinations must not dedup against each other")
}

// TestInboxDedupID_FallsBackToPayloadSeed keeps the partial-deployment path
// distinguishable too: with no request ID on ctx the seed carries the identity,
// and the event type must still separate the lanes.
func TestInboxDedupID_FallsBackToPayloadSeed(t *testing.T) {
	ctx := context.Background()

	added := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberAdded, "seed-1")
	removed := natsutil.InboxDedupID(ctx, "site-b", model.InboxMemberRemoved, "seed-1")
	assert.NotEqual(t, added, removed)
	assert.Contains(t, added, "seed-1", "the payload seed must still carry the identity")
}
