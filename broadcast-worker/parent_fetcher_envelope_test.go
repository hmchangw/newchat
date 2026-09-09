package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
)

// fetchReplying stands up a responder that replies with exactly payload and
// runs the real FetchParent against it, so the `ok && Code.Valid()` shape is
// exercised end to end rather than re-implemented in the test.
func fetchReplying(t *testing.T, payload []byte) (*ParentMessageInfo, error) {
	t.Helper()
	nc := startTestNATS(t)
	_, err := nc.Subscribe(context.Background(), subject.MsgGet("alice", "room-1", "site-a"),
		func(_ context.Context, m *nats.Msg) { _ = m.Respond(payload) })
	require.NoError(t, err)
	f := newHistoryParentFetcher(nc, natsmetrics.Publisher{})
	return f.FetchParent(context.Background(), "alice", "room-1", "site-a", "parent-msg")
}

// producedEnvelopes drives every server-side error-reply path this repo has
// through its real marshaller, so the table below is the complete set of
// envelopes an in-fleet peer can put on the wire.
func producedEnvelopes(t *testing.T) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	return map[string][]byte{
		"BadRequest":            errnats.Marshal(ctx, errcode.BadRequest("x")),
		"Unauthenticated":       errnats.Marshal(ctx, errcode.Unauthenticated("x")),
		"Forbidden":             errnats.Marshal(ctx, errcode.Forbidden("x")),
		"NotFound":              errnats.Marshal(ctx, errcode.NotFound("x")),
		"Conflict":              errnats.Marshal(ctx, errcode.Conflict("x")),
		"TooManyRequests":       errnats.Marshal(ctx, errcode.TooManyRequests("x")),
		"Unavailable":           errnats.Marshal(ctx, errcode.Unavailable("x")),
		"Internal":              errnats.Marshal(ctx, errcode.Internal("x")),
		"raw error collapsed":   errnats.Marshal(ctx, errors.New("boom")),
		"with reason":           errnats.Marshal(ctx, errcode.Forbidden("x", errcode.WithReason(errcode.MessageThreadParentNotFound))),
		"with metadata":         errnats.Marshal(ctx, errcode.Unavailable("x", errcode.WithMetadata("retryAfter", "5"))),
		"MarshalQuiet":          errnats.MarshalQuiet(errcode.Internal("x")),
		"oversize fallback":     errnats.OversizeEnvelope(),
		"natsutil marshal-fail": []byte(`{"code":"internal","error":"internal error"}`),
	}
}

// TestErrorEnvelope_ProducersAreAlwaysWellFormed pins the invariant the whole
// hand-rolled `Parse + Code.Valid()` idiom depends on: every envelope this
// build can emit carries a canonical Code, a non-empty message, and (when
// present) string-valued metadata. errcode.New panics on a non-canonical Code
// or an empty message, and WithMetadata takes ...string, so nothing else is
// constructible — but nothing pinned that until now.
func TestErrorEnvelope_ProducersAreAlwaysWellFormed(t *testing.T) {
	for name, data := range producedEnvelopes(t) {
		t.Run(name, func(t *testing.T) {
			var probe struct {
				Code     string          `json:"code"`
				Message  string          `json:"error"`
				Metadata json.RawMessage `json:"metadata"`
			}
			require.NoError(t, json.Unmarshal(data, &probe), "envelope must be JSON: %s", data)
			assert.True(t, errcode.Code(probe.Code).Valid(), "non-canonical code %q in %s", probe.Code, data)
			assert.NotEmpty(t, probe.Message, "empty message in %s", data)
			if len(probe.Metadata) > 0 {
				var m map[string]string
				assert.NoError(t, json.Unmarshal(probe.Metadata, &m), "non-string metadata in %s", data)
			}
		})
	}
}

// TestFetchParent_RelaysEveryProducibleEnvelope asserts the existing call site
// is correct for every envelope the fleet can actually produce: each one comes
// back as a typed *errcode.Error, never as a silent empty result.
func TestFetchParent_RelaysEveryProducibleEnvelope(t *testing.T) {
	for name, data := range producedEnvelopes(t) {
		t.Run(name, func(t *testing.T) {
			got, err := fetchReplying(t, data)
			require.Error(t, err, "envelope %s must not read as success", data)
			assert.Nil(t, got)
			var typed *errcode.Error
			assert.True(t, errors.As(err, &typed), "envelope %s should relay typed", data)
		})
	}
}

// TestFetchParent_FailsOpenOnUnproducibleEnvelopes documents the exact gap
// PR #477 closes, and the exact precondition each case needs. None of these
// three payloads is constructible by any producer above; each requires a peer
// built from different source than this build.
func TestFetchParent_FailsOpenOnUnproducibleEnvelopes(t *testing.T) {
	cases := []struct{ name, payload, precondition string }{
		{"code outside the closed set", `{"code":"quota_exhausted","error":"tenant over quota"}`,
			"a peer whose category.go declares a 9th Code"},
		{"metadata retyped", `{"code":"unavailable","error":"upstream down","metadata":{"retryAfter":5}}`,
			"a peer whose Error.Metadata is map[string]any"},
		{"envelope without a code", `{"error":"message not found"}`,
			"a peer that replies outside errnats/Classify"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fetchReplying(t, []byte(tc.payload))
			// Today: the remote refusal is swallowed and a zero-value parent is
			// returned with a nil error. Precondition: tc.precondition.
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Empty(t, got.SenderAccount, "the fail-open yields a zero-value parent")

			// FromReply turns the same bytes into a failure, untyped so an
			// unknown code can never reach errcode.New (which panics on one).
			remoteErr := errcode.FromReply([]byte(tc.payload))
			require.Error(t, remoteErr)
			var typed *errcode.Error
			assert.False(t, errors.As(remoteErr, &typed), "must stay untyped")
		})
	}
}

// TestFetchParent_ReasonSkewAlreadyWorks covers the one wire evolution that is
// actually routine here — a new Reason, which every feature PR may add to a
// codes_*.go file. Reason is an open-set string and is not validated, so the
// existing call site relays it correctly with no change needed.
func TestFetchParent_ReasonSkewAlreadyWorks(t *testing.T) {
	got, err := fetchReplying(t, []byte(`{"code":"forbidden","reason":"a_reason_this_build_has_never_seen","error":"nope"}`))
	require.Error(t, err)
	assert.Nil(t, got)
	var typed *errcode.Error
	require.True(t, errors.As(err, &typed))
	assert.Equal(t, errcode.CodeForbidden, typed.Code)
	assert.Equal(t, errcode.Reason("a_reason_this_build_has_never_seen"), typed.Reason)
}

// TestFromReply_IgnoresNonJSON records the one direction FromReply is weaker
// than the decoder that follows it: a non-JSON reply is not its business, so
// every caller still needs its own decode to catch a corrupt body.
func TestFromReply_IgnoresNonJSON(t *testing.T) {
	assert.NoError(t, errcode.FromReply([]byte("not json at all")))
	got, err := fetchReplying(t, []byte("not json at all"))
	assert.Nil(t, got)
	assert.Error(t, err, "the sonic decode, not the envelope check, is what catches this")
}
