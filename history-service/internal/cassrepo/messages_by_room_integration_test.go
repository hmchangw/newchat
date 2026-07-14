//go:build integration

package cassrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	cassmodel "github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Database {
	t.Helper()
	return testutil.MongoDB(t, "history_cassrepo_test")
}

// newTestVaultWrapper constructs an atrest.KeyWrapper backed by the
// shared dev Vault container (started once per test process) and
// registers cleanup. Used by tests that need a real atrest.Cipher.
func newTestVaultWrapper(t *testing.T, ctx context.Context) atrest.KeyWrapper {
	t.Helper()
	v := testutil.Vault(t, ctx)
	w, err := atrest.NewVaultKeyWrapper(ctx, atrest.VaultConfig{
		Address:      v.Address,
		TransitMount: v.TransitMount,
		TransitKey:   v.TransitKey,
		Token:        v.Token,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		// Best-effort: tests can't meaningfully act on a Close failure.
		_ = w.Close()
	})
	return w
}

// seedMessages inserts count messages into messages_by_room spaced 1 minute apart
// starting from base. bucket is derived from each message's created_at.
func seedMessages(t *testing.T, session *gocql.Session, roomID string, base time.Time, count int) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	sender := models.Participant{ID: "u1", Account: "user1"}
	for i := 0; i < count; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		bucket := sizer.Of(ts)
		err := session.Query(
			`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg) VALUES (?, ?, ?, ?, ?, ?)`,
			roomID, bucket, ts, fmt.Sprintf("m%d", i), sender, fmt.Sprintf("msg-%d", i),
		).Exec()
		require.NoError(t, err)
	}
}

// seedMessage inserts a single message with an explicit createdAt and messageID.
func seedMessage(t *testing.T, session *gocql.Session, roomID, messageID string, createdAt time.Time) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	bucket := sizer.Of(createdAt)
	err := session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, type)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucket, createdAt, messageID,
		map[string]interface{}{"id": "u1", "account": "alice"},
		"m", "site-A", createdAt, "text",
	).Exec()
	require.NoError(t, err)
}

func TestRepository_GetMessagesBefore(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 3)
	require.NoError(t, err)

	page, err := repo.GetMessagesBefore(ctx, "r1", base.Add(10*time.Minute), time.Time{}, q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 3)
	assert.True(t, page.Data[0].CreatedAt.After(page.Data[1].CreatedAt))
}

func TestRepository_GetMessagesBetweenDesc(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetMessagesBetweenDesc(ctx, "r1", base.Add(1*time.Minute), base.Add(4*time.Minute), q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 2)                                          // m2 (2min), m3 (3min) — excludes 1min and 4min
	assert.True(t, page.Data[0].CreatedAt.After(page.Data[1].CreatedAt)) // DESC order
}

func TestRepository_GetMessagesAfter(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetMessagesAfter(ctx, "r1", base.Add(2*time.Minute), time.Now().UTC().Add(time.Hour), q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 2)                                           // m3 (3min), m4 (4min) — strictly after 2min
	assert.True(t, page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt)) // ASC order
}

func TestRepository_GetAllMessagesAsc(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 3)
	require.NoError(t, err)

	page, err := repo.GetAllMessagesAsc(ctx, "r1", base.Add(-time.Hour), time.Now().UTC().Add(time.Hour), q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 3)
	assert.True(t, page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt)) // ASC order
	assert.True(t, page.HasNext)
}

func TestHybridRead_LegacyAndEncrypted(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	now := time.Now().UTC().Truncate(time.Millisecond)
	roomID := "r-hybrid-1"

	// Insert a legacy plaintext row directly with CQL.
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, msg, site_id) VALUES (?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(now), now, "legacy", "plaintext-body", "site-a",
	).Exec())

	// Insert an encrypted row by going through the cipher.
	enc := atrest.EncryptedFields{Msg: "encrypted-body"}
	payload, meta, err := cipher.Encrypt(ctx, roomID, enc)
	require.NoError(t, err)
	encTS := now.Add(time.Second)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(encTS), encTS, "encrypted", payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetAllMessagesAsc(ctx, roomID, now.Add(-time.Hour), now.Add(time.Hour), q)
	require.NoError(t, err)
	require.Len(t, page.Data, 2)

	bodies := []string{page.Data[0].Msg, page.Data[1].Msg}
	assert.Contains(t, bodies, "plaintext-body")
	assert.Contains(t, bodies, "encrypted-body")

	// Encrypted row should NOT leak enc_* fields above the repo layer.
	for _, m := range page.Data {
		assert.Empty(t, m.EncPayload)
		assert.Nil(t, m.EncMeta)
	}
}

func TestRepository_GetMessagesBefore_ThreadRoomID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	sizer := msgbucket.New(24 * time.Hour)
	sender := models.Participant{ID: "u1", Account: "user1"}
	ts := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	bucket := sizer.Of(ts)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_room_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"r-thread", bucket, ts, "m-thread", sender, "reply", "tr-42",
	).Exec())

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetMessagesBefore(ctx, "r-thread", ts.Add(1*time.Minute), time.Time{}, q)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, "tr-42", page.Data[0].ThreadRoomID)
}

// --- New cross-bucket / floor / cap integration tests ---

func TestGetMessagesBefore_CrossBucketWalkDESC(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	roomID := "room-walk-desc"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		seedMessage(t, session, roomID, fmt.Sprintf("m%d", i), base.AddDate(0, 0, -i))
	}

	floor := base.AddDate(0, 0, -10)

	page, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{PageSize: 3})
	require.NoError(t, err)
	require.Len(t, page.Data, 3)
	assert.Equal(t, "m0", page.Data[0].MessageID)
	assert.Equal(t, "m1", page.Data[1].MessageID)
	assert.Equal(t, "m2", page.Data[2].MessageID)
	assert.True(t, page.HasNext)

	cursor, err := NewCursor(page.NextCursor)
	require.NoError(t, err)
	page2, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{Cursor: cursor, PageSize: 3})
	require.NoError(t, err)
	require.Len(t, page2.Data, 2)
	assert.Equal(t, "m3", page2.Data[0].MessageID)
	assert.Equal(t, "m4", page2.Data[1].MessageID)
	assert.False(t, page2.HasNext)
}

func TestGetMessagesBefore_FloorTerminates(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, nil)
	ctx := context.Background()

	roomID := "room-floor"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		seedMessage(t, session, roomID, fmt.Sprintf("m%d", i), base.AddDate(0, 0, -i))
	}
	floor := base.AddDate(0, 0, -2) // only m0, m1, m2 survive the floor

	page, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 3)
	assert.False(t, page.HasNext)
}

func TestGetMessagesBefore_MaxBucketsCap(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 2, nil) // cap at 2 buckets
	ctx := context.Background()

	roomID := "room-cap"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	floor := base.AddDate(0, 0, -10)

	page, err := repo.GetMessagesBefore(ctx, roomID, base, floor, PageRequest{PageSize: 50})
	require.NoError(t, err)
	assert.Empty(t, page.Data)
	assert.True(t, page.HasNext, "non-terminal cursor when cap reached")
}

// TestGetMessages_DecryptErrorHaltsWalk verifies that a decrypt error on
// an early-bucket row halts the page walk immediately and surfaces the
// FIRST error to the caller — a later bucket's decrypt failure must not
// overwrite *scanErr. Without scanMessagesUpTo's halt signal, fillPage
// would advance past the first failure and the operator-visible error
// would reflect the last bucket's failure mode, masking the root cause.
func TestGetMessages_DecryptErrorHaltsWalk(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, cipher)

	roomID := "r-halt-1"

	// Two encrypted rows, two different buckets (different days).
	day1 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)

	encPayload, encMeta, err := cipher.Encrypt(ctx, roomID, atrest.EncryptedFields{Msg: "v"})
	require.NoError(t, err)

	// Day-1 row: nonce length 11 → ErrPayloadMalformed on decrypt (the
	// first error the walk will encounter when ASC-walking from day1).
	badLenMeta := &cassmodel.EncMeta{Nonce: encMeta.Nonce[:11]}
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(day1), day1, "m-day1-bad", encPayload, badLenMeta, "site-a",
	).Exec())

	// Day-2 row: 12-byte nonce that won't authenticate (different from
	// the row's wrap nonce) → ErrAuthFailed on decrypt. This is the
	// SECOND error the walk would hit if it didn't halt.
	wrongNonce := make([]byte, 12)
	for i := range wrongNonce {
		wrongNonce[i] = 0xFF
	}
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(day2), day2, "m-day2-bad", encPayload, &cassmodel.EncMeta{Nonce: wrongNonce}, "site-a",
	).Exec())

	page, walkErr := repo.GetAllMessagesAsc(ctx, roomID,
		day1.Add(-time.Hour), day2.Add(time.Hour),
		PageRequest{PageSize: 10})
	require.Error(t, walkErr, "decrypt failure must surface as an error")
	assert.Empty(t, page.Data, "no rows must be returned on decrypt failure")
	assert.ErrorIs(t, walkErr, atrest.ErrPayloadMalformed,
		"walk must halt on the day-1 malformed-nonce error; surfacing ErrAuthFailed means the walk continued past day1 and let day2 overwrite scanErr")
}

func TestGetMessagesBefore_SysMsgDataRoundTrips(t *testing.T) {
	session := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, nil)
	ctx := context.Background()

	roomID := "room-sysmsg"
	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	sender := models.Participant{ID: "u_a", Account: "alice"}

	payload, err := json.Marshal(model.MembersAdded{Individuals: []string{"u1", "u2"}, Orgs: []string{"o1"}, AddedUsersCount: 3})
	require.NoError(t, err)

	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, type, sys_msg_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(createdAt), createdAt, "m-sys", sender,
		`"alice" added 2 people and 1 organization to the chatroom`,
		model.MessageTypeMembersAdded, payload,
	).Exec())

	page, err := repo.GetMessagesBefore(ctx, roomID, createdAt.Add(time.Second), time.Time{}, PageRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 1)

	got := page.Data[0]
	assert.Equal(t, model.MessageTypeMembersAdded, got.Type)
	require.NotEmpty(t, got.SysMsgData, "sysMsgData must survive the load-history round-trip")

	var decoded model.MembersAdded
	require.NoError(t, json.Unmarshal(got.SysMsgData, &decoded))
	assert.Equal(t, []string{"u1", "u2"}, decoded.Individuals)
	assert.Equal(t, []string{"o1"}, decoded.Orgs)
	assert.Equal(t, 3, decoded.AddedUsersCount, "whole payload (not just the slices) must survive")
}
