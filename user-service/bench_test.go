package main

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

// benchPage builds a page of realistic weight: each row carries the nested room
// and a full previewMessage, which is what dominates the payload in production.
func benchPage(n int) *models.PagedSubscriptionListResponse {
	at := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	items := make([]model.SubscriptionItem, n)
	for i := range items {
		sub := &model.Subscription{
			ID: "01970a4f8c2d7c9a01970a4f8c2d7c9a", RoomID: "01970a4f8c2d7c9aQ",
			SiteID: "site-a", RoomType: model.RoomTypeChannel, Name: "engineering-general",
			Roles: []model.Role{model.RoleMember}, JoinedAt: at, Alert: true, Favorite: true,
		}
		sub.Room = &model.SubscriptionRoom{
			SiteID: "site-a", Name: "engineering-general", UserCount: 42, LastMsgAt: &at,
			PreviewMessage: &model.PreviewMessage{
				MessageID: "01970a4f8c2d7c9aBB",
				Sender:    model.Participant{UserID: "01970a4f8c2d7c9a", Account: "alice", DisplayName: "Alice"},
				Content:   "morning team, pushing the release branch after standup — please hold merges until then",
				CreatedAt: at,
			},
		}
		items[i] = &model.ChannelSubscription{Subscription: sub}
	}
	return &models.PagedSubscriptionListResponse{Subscriptions: items, HasMore: true}
}

// BenchmarkPageMarshal measures the JSON cost per page size.
func BenchmarkPageMarshal(b *testing.B) {
	for _, n := range []int{40, 200, 400} {
		b.Run(pageName(n), func(b *testing.B) {
			page := benchPage(n)
			b.ReportAllocs()
			var size int
			for b.Loop() {
				out, err := json.Marshal(page)
				if err != nil {
					b.Fatal(err)
				}
				size = len(out)
			}
			b.ReportMetric(float64(size), "bytes/page")
		})
	}
}

// BenchmarkPageMarshalGzip measures marshal plus compression, the real response path.
func BenchmarkPageMarshalGzip(b *testing.B) {
	for _, n := range []int{40, 200, 400} {
		b.Run(pageName(n), func(b *testing.B) {
			page := benchPage(n)
			b.ReportAllocs()
			var raw, compressed int
			for b.Loop() {
				out, err := json.Marshal(page)
				if err != nil {
					b.Fatal(err)
				}
				counter := &countingWriter{}
				zw := gzip.NewWriter(counter)
				if _, err := zw.Write(out); err != nil {
					b.Fatal(err)
				}
				if err := zw.Close(); err != nil {
					b.Fatal(err)
				}
				raw, compressed = len(out), counter.n
			}
			b.ReportMetric(float64(raw), "bytes/raw")
			b.ReportMetric(float64(compressed), "bytes/gzip")
		})
	}
}

func pageName(n int) string {
	switch n {
	case 40:
		return "rows40_nats_default"
	case 200:
		return "rows200_frontend_default"
	default:
		return "rows400_http_max"
	}
}

type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) { w.n += len(p); return len(p), nil }

var _ io.Writer = (*countingWriter)(nil)
