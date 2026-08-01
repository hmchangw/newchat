package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestHTTPVerifier_PostsChatIDsAndDecodesReply(t *testing.T) {
	var gotPath string
	var gotBody model.TeamsRoomVerifyRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(model.TeamsRoomVerifyResponse{
			SiteID: "site-a", RequestedCount: 1, FoundCount: 1,
			Chats: []model.TeamsRoomVerifyResult{
				{ChatID: "19:abc@thread.v2", RoomID: "room1", RoomExists: true, SubscriptionCount: 2, RoomUserCount: 2},
			},
		}))
	}))
	defer srv.Close()

	verify := newHTTPVerifier(5 * time.Second)
	got, err := verify(context.Background(), srv.URL, []string{"19:abc@thread.v2"})
	require.NoError(t, err)

	assert.Equal(t, verifyPath, gotPath)
	assert.Equal(t, []string{"19:abc@thread.v2"}, gotBody.ChatIDs)
	assert.Equal(t, "site-a", got.SiteID)
	require.Len(t, got.Chats, 1)
	assert.Equal(t, 2, got.Chats[0].SubscriptionCount)
}

func TestHTTPVerifier_Errors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"server error", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}},
		{"bad request", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}},
		{"undecodable body", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"chats":`))
		}},
		{"empty results", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"siteId":"site-a","requestedCount":1,"foundCount":0,"chats":[]}`))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			verify := newHTTPVerifier(5 * time.Second)
			_, err := verify(context.Background(), srv.URL, []string{"19:abc@thread.v2"})
			require.Error(t, err)
		})
	}
}

func TestHTTPVerifier_UnreachableHost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now

	verify := newHTTPVerifier(1 * time.Second)
	_, err := verify(context.Background(), url, []string{"19:abc@thread.v2"})
	require.Error(t, err)
}
