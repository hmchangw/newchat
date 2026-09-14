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

func TestMongoStore_MemberSiteID(t *testing.T) {
	db := testutil.MongoDB(t, "uploadsvc")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Local room: subscription carries the room's siteId.
	_, err := db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub1", "roomId": "r1", "siteId": "site-x", "u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)
	// Cross-site room: only the mirrored subscription exists — no rooms doc at all.
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub2", "roomId": "r-remote", "siteId": "site-remote", "u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)
	// Legacy subscription without siteId decodes to "".
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub3", "roomId": "r-legacy", "u": bson.M{"_id": "u1", "account": "alice"},
	})
	require.NoError(t, err)

	s := NewMongoStore(db)

	siteID, member, err := s.MemberSiteID(ctx, "r1", "alice")
	require.NoError(t, err)
	require.True(t, member)
	require.Equal(t, "site-x", siteID)

	siteID, member, err = s.MemberSiteID(ctx, "r-remote", "alice")
	require.NoError(t, err)
	require.True(t, member)
	require.Equal(t, "site-remote", siteID)

	siteID, member, err = s.MemberSiteID(ctx, "r-legacy", "alice")
	require.NoError(t, err)
	require.True(t, member)
	require.Empty(t, siteID)

	_, member, err = s.MemberSiteID(ctx, "r1", "bob")
	require.NoError(t, err)
	require.False(t, member)
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
