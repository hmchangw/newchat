package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// benchMessageData builds one realistic MESSAGES_CANONICAL event payload.
func benchMessageData(b *testing.B) []byte {
	b.Helper()
	evt := model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "01J9ZXAMPLE0MSGID20", RoomID: "01J9ZROOMID17CHARS",
			UserID: "01J9ZUSERID17CHARS", UserAccount: "alice@site-a",
			Content:   "Hey team, can someone review the search-sync perf PR when you get a sec? It touches the user-room scripted updates.",
			CreatedAt: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
		},
		SiteID:    "site-a",
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

// benchMemberData builds one INBOX member_added event fanning out to n accounts.
func benchMemberData(b *testing.B, n int) []byte {
	b.Helper()
	accounts := make([]string, n)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("user%05d@site-a", i)
	}
	payload := model.InboxMemberEvent{
		RoomID: "01J9ZROOMID17CHARS", RoomName: "engineering-team",
		RoomType: model.RoomTypeChannel, SiteID: "site-a",
		Accounts: accounts, JoinedAt: 1735689600000, Timestamp: 1735689600000,
	}
	payloadData, err := json.Marshal(&payload)
	if err != nil {
		b.Fatal(err)
	}
	evt := model.InboxEvent{
		Type: model.InboxMemberAdded, SiteID: "site-a", DestSiteID: "site-a",
		Payload: payloadData, Timestamp: 1735689600000,
	}
	data, err := json.Marshal(&evt)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

// BenchmarkBuildAction_Message measures per-message parse+build cost for the
// 1:1 messages collection — the CPU work a pipelined flush would overlap with
// the previous batch's ES round-trip.
func BenchmarkBuildAction_Message(b *testing.B) {
	coll := newMessageCollection("messages-site-a-v1", "site-a", time.Time{}, false)
	data := benchMessageData(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coll.BuildAction(data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildAction_UserRoom measures per-EVENT parse+build cost for the
// user-room collection at several fan-out widths. Cost is reported per event,
// so divide by the account count for per-action cost.
func BenchmarkBuildAction_UserRoom(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("accounts=%d", n), func(b *testing.B) {
			coll := newUserRoomCollection("user-room-site-a")
			data := benchMemberData(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := coll.BuildAction(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBuildAction_Spotlight mirrors the user-room benchmark for the
// spotlight collection (index/delete actions, no scripted-update body build).
func BenchmarkBuildAction_Spotlight(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("accounts=%d", n), func(b *testing.B) {
			coll := newSpotlightCollection("spotlight-site-a-v1", false)
			data := benchMemberData(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := coll.BuildAction(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
