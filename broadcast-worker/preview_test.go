package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/preview"
)

// fakeCipher is a reversible non-encryption so tests assert on what was sealed; encErr fails.
type fakeCipher struct{ encErr error }

func (f fakeCipher) Encrypt(_ context.Context, _ string, fields atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) {
	if f.encErr != nil {
		return nil, atrest.EncMeta{}, f.encErr
	}
	b, err := json.Marshal(fields)
	return b, atrest.EncMeta{Nonce: []byte("nonce")}, err
}

func (f fakeCipher) Decrypt(_ context.Context, _ string, payload []byte, _ atrest.EncMeta) (atrest.EncryptedFields, error) {
	var out atrest.EncryptedFields
	err := json.Unmarshal(payload, &out)
	return out, err
}

func (f fakeCipher) EnsureDEK(context.Context, string) error { return nil }

func testSealer(cipher atrest.Cipher) *previewSealer {
	return newPreviewSealer(cipher, preview.Key{SiteID: "site-test", Epoch: 1}, nil)
}

// openSealed reverses fakeCipher so a test can read what a preview actually carries.
func openSealed(t *testing.T, s *preview.Sealed) atrest.EncryptedFields {
	t.Helper()
	var fields atrest.EncryptedFields
	require.NoError(t, json.Unmarshal(s.Ciphertext, &fields))
	return fields
}

func TestEligibleAsPreview(t *testing.T) {
	assert.True(t, eligibleAsPreview(&model.Message{Type: ""}), "an ordinary message represents the room")
	assert.True(t, eligibleAsPreview(&model.Message{Type: model.MessageTypeImportant}),
		"重要訊息 is client-set, not a system type, and previews like any other message")
	assert.False(t, eligibleAsPreview(&model.Message{Type: model.MessageTypeMembersAdded}),
		"a membership notice is not representative room content")
}

func TestPreviewSealer_SealInserted_CarriesTheMessageWithoutReadingAnything(t *testing.T) {
	att, err := json.Marshal(cassandra.Attachment{ID: "a-1", Title: "spec.pdf"})
	require.NoError(t, err)
	msg := &model.Message{
		ID: "m-1", RoomID: "r-1", UserID: "u-1", UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
		Attachments: [][]byte{att},
	}
	users := map[string]model.User{"alice": {EngName: "Alice", ChineseName: "愛麗絲"}}
	mentions := []model.Participant{{Account: "bob", EngName: "Bob"}}

	sealed, err := testSealer(fakeCipher{}).sealInserted(context.Background(), msg, users, mentions)
	require.NoError(t, err)

	assert.Equal(t, "m-1", sealed.Meta.MessageID)
	assert.Equal(t, "alice", sealed.Meta.Sender.Account)
	assert.Equal(t, "Alice 愛麗絲", sealed.Meta.Sender.DisplayName, "the preview sender must be render-ready")
	assert.Equal(t, mentions, sealed.Meta.Mentions)
	assert.Equal(t, 1, sealed.KeyEpoch)

	// Content and attachments are user-authored: inside the ciphertext, never in the meta.
	fields := openSealed(t, sealed)
	assert.Equal(t, "hello", fields.Msg)
	require.Len(t, fields.Attachments, 1)
	body, err := json.Marshal(sealed.Meta)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "hello", "the body must not leak into the plaintext meta")
}

func TestPreviewSealer_SealInserted_TruncatesLongContent(t *testing.T) {
	long := make([]rune, preview.MaxContentRunes+50)
	for i := range long {
		long[i] = 'x'
	}
	msg := &model.Message{ID: "m-1", Content: string(long), CreatedAt: time.Now().UTC()}

	sealed, err := testSealer(fakeCipher{}).sealInserted(context.Background(), msg, nil, nil)
	require.NoError(t, err)
	assert.Len(t, []rune(openSealed(t, sealed).Msg), preview.MaxContentRunes)
}

func TestPreviewSealer_SealInserted_RejectsUndecodableAttachment(t *testing.T) {
	msg := &model.Message{ID: "m-1", CreatedAt: time.Now().UTC(), Attachments: [][]byte{[]byte("not json")}}

	_, err := testSealer(fakeCipher{}).sealInserted(context.Background(), msg, nil, nil)
	require.Error(t, err, "a blob the gatekeeper wrote must decode; silently dropping it would hide upstream corruption")
}

// The three outcomes drive three different writes; "nothing to store" and "unknown"
// must not collapse into one nil — the first advances the key, the second may not.
func TestHandler_PreviewForInserted(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name       string
		sealer     *previewSealer
		msg        *model.Message
		wantNil    bool
		wantFailed bool
	}{
		{"previews off", testSealer(nil), &model.Message{ID: "m-1", CreatedAt: now}, true, false},
		{"system message", testSealer(fakeCipher{}), &model.Message{ID: "m-1", CreatedAt: now, Type: model.MessageTypeMembersAdded}, true, false},
		{"seal fails", testSealer(fakeCipher{encErr: errors.New("vault down")}), &model.Message{ID: "m-1", CreatedAt: now}, true, true},
		{"ordinary message", testSealer(fakeCipher{}), &model.Message{ID: "m-1", CreatedAt: now}, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{sealer: tc.sealer}
			got, failed := h.previewForInserted(context.Background(), tc.msg, nil, nil)
			assert.Equal(t, tc.wantFailed, failed,
				"only an ELIGIBLE message that could not be sealed may suppress the freshness key")
			if tc.wantNil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
		})
	}
}

// fakeBulkWriter captures what the writer drained, so a test asserts on the batch
// rather than on MongoDB.
type fakeBulkWriter struct {
	mu    sync.Mutex
	calls []map[string]roomPreviewUpdate
	err   error
}

func (f *fakeBulkWriter) BulkUpdateRoomPreview(_ context.Context, updates map[string]roomPreviewUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[string]roomPreviewUpdate, len(updates))
	for k, v := range updates {
		cp[k] = v
	}
	f.calls = append(f.calls, cp)
	return f.err
}

func (f *fakeBulkWriter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// A newer INELIGIBLE message must advance the last-message id without evicting the
// preview it cannot replace, so a room whose latest activity is a join notice still shows.
func TestPreviewWriter_IneligibleMessageAdvancesTheKeyButKeepsThePreview(t *testing.T) {
	bulk := &fakeBulkWriter{}
	w := newPreviewWriter(bulk)
	t0 := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	sealed := &preview.Sealed{Meta: model.PreviewMeta{MessageID: "m-eligible"}}

	w.buffer(roomPreview{RoomID: "r-1", MsgID: "m-eligible", At: t0, Preview: sealed})
	w.buffer(roomPreview{RoomID: "r-1", MsgID: "m-system", At: t0.Add(time.Second), Preview: nil})
	require.NoError(t, w.Flush(context.Background()))

	require.Len(t, bulk.calls, 1)
	got := bulk.calls[0]["r-1"]
	assert.Equal(t, "m-system", got.msgID, "the freshness key follows the newest message, eligible or not")
	require.NotNil(t, got.pvw)
	assert.Equal(t, "m-eligible", got.pvw.Meta.MessageID, "the system message must not evict the preview")
}

func TestPreviewWriter_NewerEligiblePreviewWins(t *testing.T) {
	bulk := &fakeBulkWriter{}
	w := newPreviewWriter(bulk)
	t0 := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)

	// Delivered newest-first, as an unordered consumer may see them.
	w.buffer(roomPreview{
		RoomID: "r-1", MsgID: "m-2", At: t0.Add(time.Second),
		Preview: &preview.Sealed{Meta: model.PreviewMeta{MessageID: "m-2"}},
	})
	w.buffer(roomPreview{
		RoomID: "r-1", MsgID: "m-1", At: t0,
		Preview: &preview.Sealed{Meta: model.PreviewMeta{MessageID: "m-1"}},
	})
	require.NoError(t, w.Flush(context.Background()))

	got := bulk.calls[0]["r-1"]
	assert.Equal(t, "m-2", got.msgID)
	assert.Equal(t, "m-2", got.pvw.Meta.MessageID, "arrival order must not decide which preview survives")
}

// Same millisecond, so created_at cannot order them: the id has to. unread-worker
// coalesces the same pair with the same comparator, and the reader only serves a
// stored preview while the two agree on the room's newest message — so a tie broken
// by arrival order here is a preview that reads as stale until the next message.
func TestPreviewWriter_SameMillisecondTieBreaksOnTheMessageID(t *testing.T) {
	t0 := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	for _, order := range [][2]string{{"m-1", "m-2"}, {"m-2", "m-1"}} {
		t.Run(order[0]+" then "+order[1], func(t *testing.T) {
			bulk := &fakeBulkWriter{}
			w := newPreviewWriter(bulk)
			for _, id := range order {
				w.buffer(roomPreview{
					RoomID: "r-1", MsgID: id, At: t0,
					Preview: &preview.Sealed{Meta: model.PreviewMeta{MessageID: id}},
				})
			}
			require.NoError(t, w.Flush(context.Background()))

			got := bulk.calls[0]["r-1"]
			assert.Equal(t, "m-2", got.msgID, "the higher id wins regardless of arrival order")
			assert.Equal(t, "m-2", got.pvw.Meta.MessageID)
		})
	}
}

// A seal failure is not an ineligible message: it must ride the preview clock, or an
// older successful seal arriving later in the same flush window would overwrite it (#224).
func TestPreviewWriter_SealFailureSuppressesThePreviewWrite(t *testing.T) {
	t0 := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	sealedFor := func(id string) *preview.Sealed {
		return &preview.Sealed{Meta: model.PreviewMeta{MessageID: id}}
	}
	tests := []struct {
		name       string
		updates    []roomPreview
		wantMsgID  string
		wantFailed bool
		wantPvw    string // "" when no body may be written
	}{
		{
			name: "a failure after a good seal withdraws it",
			updates: []roomPreview{
				{RoomID: "r-1", MsgID: "m-1", At: t0, Preview: sealedFor("m-1")},
				{RoomID: "r-1", MsgID: "m-2", At: t1, PreviewFailed: true},
			},
			wantMsgID: "m-2", wantFailed: true,
		},
		{
			name: "an older good seal must not overtake a newer failure",
			updates: []roomPreview{
				{RoomID: "r-1", MsgID: "m-2", At: t1, PreviewFailed: true},
				{RoomID: "r-1", MsgID: "m-1", At: t0, Preview: sealedFor("m-1")},
			},
			wantMsgID: "m-2", wantFailed: true,
		},
		{
			name: "a newer clean seal heals an earlier failure",
			updates: []roomPreview{
				{RoomID: "r-1", MsgID: "m-1", At: t0, PreviewFailed: true},
				{RoomID: "r-1", MsgID: "m-2", At: t1, Preview: sealedFor("m-2")},
			},
			wantMsgID: "m-2", wantPvw: "m-2",
		},
		{
			name: "a later ineligible message leaves the failure standing",
			updates: []roomPreview{
				{RoomID: "r-1", MsgID: "m-1", At: t0, PreviewFailed: true},
				{RoomID: "r-1", MsgID: "m-sys", At: t1},
			},
			wantMsgID: "m-sys", wantFailed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bulk := &fakeBulkWriter{}
			w := newPreviewWriter(bulk)
			for _, u := range tc.updates {
				w.buffer(u)
			}
			require.NoError(t, w.Flush(context.Background()))

			require.Len(t, bulk.calls, 1)
			got := bulk.calls[0]["r-1"]
			assert.Equal(t, tc.wantMsgID, got.msgID, "the key follows the newest message either way")
			assert.Equal(t, tc.wantFailed, got.pvwFailed)
			if tc.wantPvw == "" {
				assert.Nil(t, got.pvw)
				return
			}
			require.NotNil(t, got.pvw)
			assert.Equal(t, tc.wantPvw, got.pvw.Meta.MessageID)
		})
	}
}

func TestPreviewWriter_EmptyBufferIsNotWritten(t *testing.T) {
	bulk := &fakeBulkWriter{}
	w := newPreviewWriter(bulk)

	require.NoError(t, w.Flush(context.Background()))
	assert.Zero(t, bulk.callCount(), "an idle interval must not issue a BulkWrite")
}

// A nil writer is what a deployment with preview persistence off gets, and the handler
// calls into it on every insert — so every method has to tolerate it.
func TestPreviewWriter_NilIsInert(t *testing.T) {
	var w *previewWriter

	assert.NotPanics(t, func() { w.buffer(roomPreview{RoomID: "r-1", MsgID: "m-1"}) })
	assert.NoError(t, w.Flush(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx, time.Millisecond, time.Second) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run on a nil writer must return immediately")
	}
}

// The room pointer moved to unread-worker, which holds its messages un-acked until
// MongoDB takes them. This write must not touch those fields: a best-effort write that
// drops on failure racing a durable, retried one would let a stalled preview flush
// resurrect an older lastMsgId over the pointer unread-worker had already advanced.
func TestPreviewUpdate_TouchesOnlyThePreviewFields(t *testing.T) {
	at := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	sealed := &preview.Sealed{Meta: model.PreviewMeta{MessageID: "m-1"}}
	updates := map[string]roomPreviewUpdate{
		"eligible":   {msgID: "m-1", at: at, pvw: sealed, pvwAt: at},
		"ineligible": {msgID: "m-sys", at: at},
		"failed":     {msgID: "m-1", at: at, pvwFailed: true, pvwAt: at},
	}
	for name, u := range updates {
		t.Run(name, func(t *testing.T) {
			fields := previewUpdate(&u)[0][0].Value.(bson.M)
			for _, owned := range []string{"lastMsgAt", "lastMsgId", "lastMentionAllAt", "updatedAt"} {
				assert.NotContains(t, fields, owned, "%s belongs to unread-worker", owned)
			}
			for k := range fields {
				assert.True(t, strings.HasPrefix(k, "preview"),
					"%q is not a preview field; this write must stay within its half of the document", k)
			}
		})
	}
}

func TestPreviewUpdate_EligibleMessageStampsTheNewestIDNotTheSealedOne(t *testing.T) {
	at := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)

	fields := previewUpdate(&roomPreviewUpdate{
		msgID: "m-system", at: at,
		// Sealed while m-eligible was newest; a system message arrived in the same window.
		pvw:   &preview.Sealed{Meta: model.PreviewMeta{MessageID: "m-eligible"}, ForMsgID: "m-eligible"},
		pvwAt: at.Add(-time.Second),
	})[0][0].Value.(bson.M)

	require.Contains(t, fields, "previewCiphertext")
	keyCond := fields["previewForMsgId"].(bson.M)["$cond"].(bson.A)
	assert.Equal(t, bson.M{"$literal": "m-system"}, keyCond[1],
		"the freshness key must name the room's newest message, or the identity check fails against lastMsgId")

	// The KEY takes the newest message's identity; the WATERMARK takes the preview's own
	// clock. They are different questions: "what is this body paired with" versus "when
	// was this body established". Stamping the body with the system message's time would
	// claim it is as-of a moment it knows nothing about -- and would beat a mutation that
	// landed in between carrying the correct body.
	assert.EqualValues(t, at.Add(-time.Second).UnixMilli(),
		fields["previewAsOf"].(bson.M)["$cond"].(bson.A)[1],
		"the body write is ordered by pvwAt, not by the room's newest-message clock")
}

// The writer keeps pvwAt on its own clock precisely so a later ineligible message
// cannot displace the preview. The flush must not discard that separation: a mutation
// landing between the sealed message and the flush writes the correct body, and a flush
// stamped with the ineligible message's later time would overwrite it with stale content
// under a key that then equals lastMsgId -- so the reader serves it as current.
func TestPreviewUpdate_BodyWriteCannotOutrankAnInterveningMutation(t *testing.T) {
	sealedAt := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	ineligibleAt := sealedAt.Add(10 * time.Second)

	for _, tc := range []struct {
		name string
		upd  roomPreviewUpdate
	}{
		{"a stored body", roomPreviewUpdate{
			msgID: "m-system", at: ineligibleAt,
			pvw:   &preview.Sealed{Meta: model.PreviewMeta{MessageID: "m-eligible"}},
			pvwAt: sealedAt,
		}},
		{"a cleared body", roomPreviewUpdate{
			msgID: "m-system", at: ineligibleAt,
			pvwFailed: true, pvwAt: sealedAt,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := previewUpdate(&tc.upd)[0][0].Value.(bson.M)
			got := fields["previewAsOf"].(bson.M)["$cond"].(bson.A)[1]
			assert.EqualValues(t, sealedAt.UnixMilli(), got,
				"the preview write is as-of when the preview was established")
			assert.NotEqualValues(t, ineligibleAt.UnixMilli(), got,
				"the newest message's clock must not order the preview")
		})
	}
}

func TestPreviewUpdate_IneligibleMessageAdvancesTheKeyOnly(t *testing.T) {
	at := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)

	fields := previewUpdate(&roomPreviewUpdate{msgID: "m-system", at: at})[0][0].Value.(bson.M)

	assert.Contains(t, fields, "previewForMsgId")
	assert.NotContains(t, fields, "previewCiphertext", "there is no new body to write; the stored one is still correct")
	assert.NotContains(t, fields, "previewMeta")
	// A "$"-prefixed message id would be read as a field path inside a pipeline stage.
	assert.Equal(t, bson.M{"$literal": "m-system"},
		fields["previewForMsgId"].(bson.M)["$cond"].(bson.A)[1])
}

// ForMsgID is not in the AEAD's authenticated data, so a stale body opens cleanly under a
// moved key and the reader's identity guard passes on it — hence the clear (#224).
func TestPreviewUpdate_SealFailureClearsTheStaleBody(t *testing.T) {
	at := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)

	fields := previewUpdate(&roomPreviewUpdate{
		msgID: "m-unsealable", at: at, pvwFailed: true, pvwAt: at,
	})[0][0].Value.(bson.M)

	// Assert the $cond pass-branch, not just the key: GuardedSetFields emits the same
	// keys, so presence alone would still pass if this regressed to a write.
	for _, f := range []string{"previewMeta", "previewCiphertext", "previewNonce", "previewKeyEpoch", "previewForMsgId"} {
		require.Contains(t, fields, f, "every preview field must be cleared, not left behind")
		cond, ok := fields[f].(bson.M)["$cond"].(bson.A)
		require.True(t, ok, "%s must be a guarded $cond", f)
		assert.Equal(t, "$$REMOVE", cond[1], "%s's pass branch must remove the field, not write one", f)
	}
	assert.Contains(t, fields, "previewAsOf", "the watermark must advance so an older write cannot resurrect the body")
}

// The bug the clear closes: pvwFailed lives only until the next flush, so a later
// ineligible message would restamp the key over the stale body and stop the walk.
func TestPreviewUpdate_IneligibleAfterSealFailureCannotRevalidateAStaleBody(t *testing.T) {
	t0 := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)

	// Window 1: eligible m2 fails to seal. The stored body is still m1's.
	cleared := previewUpdate(&roomPreviewUpdate{
		msgID: "m2", at: t0, pvwFailed: true, pvwAt: t0,
	})[0][0].Value.(bson.M)
	require.Contains(t, cleared, "previewCiphertext", "precondition: the failure clears the body")

	// Window 2: an ineligible system message. pvwFailed is gone with the flushed buffer.
	advanced := previewUpdate(&roomPreviewUpdate{
		msgID: "m3-system", at: t0.Add(time.Second),
	})[0][0].Value.(bson.M)

	// The advance is still correct in isolation — but it can only ever revalidate a body
	// that survived window 1, and window 1 no longer leaves one.
	assert.Contains(t, advanced, "previewForMsgId")
	assert.NotContains(t, advanced, "previewCiphertext",
		"the advance must never write a body; it can only point at one that is already there")
}

// The app-name read is the last unfenced Mongo read on the fan-out path, and the one
// whose failure costs least — so it must fail FAST and degrade, never stall the seal.
func TestGuardedAppNameLookup(t *testing.T) {
	t.Run("an open breaker fails without reaching Mongo", func(t *testing.T) {
		var calls int
		inner := func(context.Context, string) (string, error) {
			calls++
			return "", errors.New("mongo unreachable")
		}
		b := circuitbreaker.New(1, time.Minute)
		lookup := guardedAppNameLookup(inner, b)

		_, err := lookup(context.Background(), "bot-1")
		require.Error(t, err)
		require.Equal(t, circuitbreaker.StateOpen, b.State())

		_, err = lookup(context.Background(), "bot-1")
		assert.ErrorIs(t, err, circuitbreaker.ErrOpen)
		assert.Equal(t, 1, calls, "an open breaker must not reach Mongo")
	})

	t.Run("a nil breaker passes through", func(t *testing.T) {
		lookup := guardedAppNameLookup(func(context.Context, string) (string, error) {
			return "Helper Bot", nil
		}, nil)

		got, err := lookup(context.Background(), "bot-1")
		require.NoError(t, err)
		assert.Equal(t, "Helper Bot", got)
	})

	t.Run("a nil inner stays nil so BotAwareDisplayName skips it", func(t *testing.T) {
		assert.Nil(t, guardedAppNameLookup(nil, circuitbreaker.New(1, time.Minute)))
	})
}

// blockingCipher stalls until its context is cancelled, standing in for a wedged
// Vault transit or a Mongo app-name read that never answers.
type blockingCipher struct{ entered chan struct{} }

func (b blockingCipher) Encrypt(ctx context.Context, _ string, _ atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) {
	close(b.entered)
	<-ctx.Done()
	return nil, atrest.EncMeta{}, ctx.Err()
}

func (b blockingCipher) Decrypt(context.Context, string, []byte, atrest.EncMeta) (atrest.EncryptedFields, error) {
	return atrest.EncryptedFields{}, errors.New("not used")
}

func (b blockingCipher) EnsureDEK(context.Context, string) error { return nil }

// A stalled seal must give up on its own budget rather than the handler's: the fan-out
// that follows is required work, and the preview is not.
func TestHandler_PreviewForInserted_StalledSealLeavesTheFanOutBudgetIntact(t *testing.T) {
	cipher := blockingCipher{entered: make(chan struct{})}
	sealer := testSealer(cipher)
	sealer.timeout = 20 * time.Millisecond
	h := &Handler{sealer: sealer}

	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	done := make(chan struct{})
	var sealed *preview.Sealed
	var failed bool
	go func() {
		defer close(done)
		sealed, failed = h.previewForInserted(parent, &model.Message{
			ID: "msg-1", RoomID: "room-1", Content: "hi", CreatedAt: time.Now(),
		}, nil, nil)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("previewForInserted ran past its own timeout — it is using the caller's deadline")
	}

	<-cipher.entered
	assert.Nil(t, sealed, "a stalled seal stores nothing")
	assert.True(t, failed, "a stalled seal is a seal failure, not an ineligible message")
	assert.NoError(t, parent.Err(), "bounding the seal must not cancel the context fan-out still needs")
}

// Bounding the seal is not by itself a reserve: WithTimeout inherits the parent's earlier
// deadline, so on a short budget a wedged dependency spends all of it and hands the
// required fan-out a dead context. Below the reserve the seal must not start at all.
func TestHandler_PreviewForInserted_SkipsTheSealWhenTheBudgetCannotCoverIt(t *testing.T) {
	cipher := blockingCipher{entered: make(chan struct{})}
	sealer := testSealer(cipher)
	sealer.timeout = time.Second
	h := &Handler{sealer: sealer}

	// Less left than timeout+reserve, but far more than a test takes to run.
	parent, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()

	sealed, failed := h.previewForInserted(parent, &model.Message{
		ID: "msg-1", RoomID: "room-1", Content: "hi", CreatedAt: time.Now(),
	}, nil, nil)

	assert.Nil(t, sealed, "a skipped seal stores nothing")
	assert.True(t, failed, "an eligible message with no sealed body is a seal failure: letting the "+
		"key advance would certify the previous body for a message it does not describe")
	assert.NoError(t, parent.Err(), "the fan-out budget must survive the skip")

	select {
	case <-cipher.entered:
		t.Fatal("the cipher was called; the seal must be skipped, not attempted and abandoned")
	default:
	}
}

// A caller with no deadline at all has budget by definition — the common case on the
// JetStream path, and it must not be mistaken for an exhausted one.
func TestHandler_PreviewForInserted_SealsWhenTheCallerHasNoDeadline(t *testing.T) {
	h := &Handler{sealer: testSealer(fakeCipher{})}

	sealed, failed := h.previewForInserted(context.Background(), &model.Message{
		ID: "msg-1", RoomID: "room-1", Content: "hi", CreatedAt: time.Now(),
	}, nil, nil)

	assert.False(t, failed, "a deadline-free caller must not read as an exhausted budget")
	require.NotNil(t, sealed, "the seal must run")
}
