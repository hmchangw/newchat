//go:build integration

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestMongoStore_IsMemberAndGetRoomSiteID(t *testing.T) {
	db := testutil.MongoDB(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub1", "roomId": "r1", "u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)
	_, err = db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "r1", "name": "Room 1", "siteId": "site-x"})
	require.NoError(t, err)

	s := NewMongoStore(db)

	member, err := s.IsMember(ctx, "r1", "alice")
	require.NoError(t, err)
	require.True(t, member)

	member, err = s.IsMember(ctx, "r1", "bob")
	require.NoError(t, err)
	require.False(t, member)

	siteID, err := s.GetRoomSiteID(ctx, "r1")
	require.NoError(t, err)
	require.Equal(t, "site-x", siteID)

	_, err = s.GetRoomSiteID(ctx, "missing")
	require.True(t, errors.Is(err, ErrRoomNotFound))
}

func TestMongoStore_GetUpload(t *testing.T) {
	db := testutil.MongoDB(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.Collection("uploads").InsertOne(ctx, bson.M{
		"_id": "file_xyz789", "userId": "user_abc123", "rid": "r1",
		"name": "quarterly-report.pdf", "type": "application/pdf", "size": int64(2458624),
		"store": "AmazonS3:Uploads", "complete": true,
		"AmazonS3": bson.M{"path": "app-001/uploads/r1/user_abc123/file_xyz789"},
	})
	require.NoError(t, err)

	s := NewMongoStore(db)

	up, err := s.GetUpload(ctx, "file_xyz789")
	require.NoError(t, err)
	require.Equal(t, "r1", up.RID)
	require.Equal(t, "quarterly-report.pdf", up.Name)
	require.Equal(t, "application/pdf", up.Type)
	require.Equal(t, int64(2458624), up.Size)
	require.Equal(t, "app-001/uploads/r1/user_abc123/file_xyz789", up.AmazonS3.Path)

	_, err = s.GetUpload(ctx, "missing")
	require.True(t, errors.Is(err, ErrUploadNotFound))
}

func TestMinioObjectStore_Open(t *testing.T) {
	client, bucket := testutil.MinIO(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "app-001/uploads/r1/u1/f1"
	payload := []byte("PDFDATA-binary")
	_, err := client.PutObject(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/pdf"})
	require.NoError(t, err)

	s := newMinioObjectStore(client, bucket, 5*time.Minute)

	reader, err := s.Open(ctx, key)
	require.NoError(t, err)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Missing key surfaces as an error (mapped to 503 by the handler).
	_, err = s.Open(ctx, "does/not/exist")
	require.Error(t, err)
}
