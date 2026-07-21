//go:build integration

package userstore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Collection {
	return testutil.MongoDB(t, "userstore_test").Collection("users")
}

func TestMongoStore_FindUserByID(t *testing.T) {
	col := setupMongo(t)
	store := NewMongoStore(col)
	ctx := context.Background()

	_, err := col.InsertOne(ctx, bson.M{
		"_id":         "u-1",
		"account":     "alice",
		"siteId":      "site-a",
		"engName":     "Alice Wang",
		"chineseName": "愛麗絲",
		"employeeId":  "EMP001",
	})
	require.NoError(t, err)

	tests := []struct {
		name         string
		id           string
		wantErr      bool
		wantNotFound bool
		wantAccount  string
	}{
		{
			name:        "found",
			id:          "u-1",
			wantAccount: "alice",
		},
		{
			name:         "not found",
			id:           "nonexistent",
			wantErr:      true,
			wantNotFound: true,
		},
		{
			name:         "empty id returns not found",
			id:           "",
			wantErr:      true,
			wantNotFound: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.FindUserByID(ctx, tt.id)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantNotFound {
					assert.True(t, errors.Is(err, ErrUserNotFound), "expected ErrUserNotFound, got: %v", err)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantAccount, got.Account)
			assert.Equal(t, "Alice Wang", got.EngName)
			assert.Equal(t, "愛麗絲", got.ChineseName)
		})
	}
}

func TestMongoStore_FindUserByAccount(t *testing.T) {
	col := setupMongo(t)
	store := NewMongoStore(col)
	ctx := context.Background()

	_, err := col.InsertOne(ctx, bson.M{
		"_id":         "u-1",
		"account":     "alice",
		"siteId":      "site-a",
		"engName":     "Alice Wang",
		"chineseName": "愛麗絲",
		"employeeId":  "EMP001",
	})
	require.NoError(t, err)

	tests := []struct {
		name         string
		account      string
		wantErr      bool
		wantNotFound bool
		wantID       string
	}{
		{name: "found", account: "alice", wantID: "u-1"},
		{name: "not found", account: "nobody", wantErr: true, wantNotFound: true},
		{name: "empty account returns not found", account: "", wantErr: true, wantNotFound: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.FindUserByAccount(ctx, tt.account)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
				if tt.wantNotFound {
					assert.True(t, errors.Is(err, ErrUserNotFound), "expected ErrUserNotFound, got: %v", err)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.wantID, got.ID)
			assert.Equal(t, "Alice Wang", got.EngName)
			assert.Equal(t, "愛麗絲", got.ChineseName)
		})
	}
}

func TestMongoStore_FindUsersByAccounts(t *testing.T) {
	col := setupMongo(t)
	store := NewMongoStore(col)
	ctx := context.Background()

	_, err := col.InsertMany(ctx, []any{
		bson.M{"_id": "u-1", "account": "alice", "siteId": "site-a", "engName": "Alice Wang", "chineseName": "愛麗絲", "employeeId": "EMP001", "sectName": "Cardiology"},
		bson.M{"_id": "u-2", "account": "bob", "siteId": "site-a", "engName": "Bob Chen", "chineseName": "鮑勃", "employeeId": "EMP002", "sectName": "Radiology"},
	})
	require.NoError(t, err)

	tests := []struct {
		name      string
		accounts  []string
		wantCount int
	}{
		{name: "all found", accounts: []string{"alice", "bob"}, wantCount: 2},
		{name: "partial match", accounts: []string{"alice", "nobody"}, wantCount: 1},
		{name: "empty slice", accounts: []string{}, wantCount: 0},
		{name: "no match", accounts: []string{"nobody"}, wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.FindUsersByAccounts(ctx, tt.accounts)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}

	t.Run("projection carries display fields incl. sectName and employeeId", func(t *testing.T) {
		// A projection regression would silently blank room-worker's member_added enrichment.
		got, err := store.FindUsersByAccounts(ctx, []string{"alice"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "Alice Wang", got[0].EngName)
		assert.Equal(t, "愛麗絲", got[0].ChineseName)
		assert.Equal(t, "EMP001", got[0].EmployeeID)
		assert.Equal(t, "Cardiology", got[0].SectName)
	})
}
