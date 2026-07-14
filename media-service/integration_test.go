//go:build integration

package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMongo, testutil.EnsureMinIO, testutil.EnsureNATS)
}

func TestMongoStore_EmployeeID(t *testing.T) {
	db := testutil.MongoDB(t, "avatar")
	ctx := context.Background()
	_, err := db.Collection("users").InsertOne(ctx, model.User{ID: "u1", Account: "alice", EmployeeID: "E123"})
	require.NoError(t, err)
	st := newMongoStore(db)

	eid, found, err := st.EmployeeID(ctx, "alice")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "E123", eid)

	_, found, err = st.EmployeeID(ctx, "ghost")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestMongoStore_BotSite(t *testing.T) {
	db := testutil.MongoDB(t, "avatar")
	ctx := context.Background()
	_, err := db.Collection("users").InsertOne(ctx, model.User{ID: "b1", Account: "helper.bot", SiteID: "site-b"})
	require.NoError(t, err)
	st := newMongoStore(db)

	site, found, err := st.BotSite(ctx, "helper.bot")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "site-b", site)

	_, found, err = st.BotSite(ctx, "ghost.bot")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestMongoStore_RoomSite(t *testing.T) {
	db := testutil.MongoDB(t, "avatar")
	ctx := context.Background()
	_, err := db.Collection("subscriptions").InsertOne(ctx, model.Subscription{
		ID: "sub1", RoomID: "room-1", SiteID: "site-b", RoomType: model.RoomTypeChannel, Name: "General",
	})
	require.NoError(t, err)
	st := newMongoStore(db)

	site, rt, name, found, err := st.RoomSite(ctx, "room-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "site-b", site)
	assert.Equal(t, model.RoomTypeChannel, rt)
	assert.Equal(t, "General", name)

	_, _, _, found, err = st.RoomSite(ctx, "nope")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestMongoStore_AvatarAndSetBotAvatar(t *testing.T) {
	db := testutil.MongoDB(t, "avatar")
	ctx := context.Background()
	st := newMongoStore(db)

	_, found, err := st.Avatar(ctx, model.AvatarSubjectBot, "helper.bot")
	require.NoError(t, err)
	assert.False(t, found)

	av := &model.Avatar{
		ID: "bot:helper.bot", SubjectType: model.AvatarSubjectBot, SubjectID: "helper.bot",
		MinioKey: "bot/helper.bot", ContentType: "image/png", Size: 10, ETag: "e1",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	require.NoError(t, st.SetBotAvatar(ctx, av))

	got, found, err := st.Avatar(ctx, model.AvatarSubjectBot, "helper.bot")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "bot/helper.bot", got.MinioKey)

	av.ETag = "e2"
	require.NoError(t, st.SetBotAvatar(ctx, av))
	got, _, err = st.Avatar(ctx, model.AvatarSubjectBot, "helper.bot")
	require.NoError(t, err)
	assert.Equal(t, "e2", got.ETag)

	count, err := db.Collection("avatars").CountDocuments(ctx, bson.M{"_id": "bot:helper.bot"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestMinioBlobStore_PutGet(t *testing.T) {
	client, bucket := testutil.MinIO(t, "avatar")
	ctx := context.Background()
	bs := newMinioBlobStore(client, bucket)

	etag, err := bs.Put(ctx, "bot/x", strings.NewReader("PNGDATA"), int64(len("PNGDATA")), "image/png")
	require.NoError(t, err)
	assert.NotEmpty(t, etag)

	rc, info, err := bs.Get(ctx, "bot/x")
	require.NoError(t, err)
	defer rc.Close()
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "PNGDATA", string(body))
	assert.Equal(t, "image/png", info.ContentType)
	assert.Equal(t, int64(7), info.Size)
	assert.NotEmpty(t, info.ETag)
}

func TestMinioBlobStore_GetMissing(t *testing.T) {
	client, bucket := testutil.MinIO(t, "avatar")
	bs := newMinioBlobStore(client, bucket)
	_, _, err := bs.Get(context.Background(), "bot/missing")
	assert.ErrorIs(t, err, errBlobNotFound)
}
