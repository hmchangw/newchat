package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

// benchPages are the page sizes that matter: the NATS default, what the frontend
// is told to send, and the HTTP ceiling.
var benchPages = []struct {
	rows int
	name string
}{
	{40, "rows40_nats_default"},
	{200, "rows200_frontend_default"},
	{400, "rows400_http_max"},
}

// benchPage builds a page of realistic weight: each row carries the nested room
// and a full previewMessage, which is what dominates the payload in production.
//
// Row content varies per row on purpose. Identical rows compress ~70x, which
// would make any compression measurement taken here meaningless.
func benchPage(n int) *models.PagedSubscriptionListResponse {
	at := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	items := make([]model.SubscriptionItem, n)
	for i := range items {
		sub := &model.Subscription{
			ID:     fmt.Sprintf("01970a4f8c2d7c9a01970a4f8c2d%04x", i),
			RoomID: fmt.Sprintf("01970a4f8c2d7c9a%04x", i),
			SiteID: "site-a", RoomType: model.RoomTypeChannel,
			Name:  fmt.Sprintf("%s-%s-%d", benchWords[i%len(benchWords)], benchWords[(i*7)%len(benchWords)], i),
			Roles: []model.Role{model.RoleMember}, JoinedAt: at, Alert: i%3 == 0, Favorite: i%5 == 0,
		}
		sub.Room = &model.SubscriptionRoom{
			SiteID: "site-a", Name: sub.Name, UserCount: 3 + i%97, LastMsgAt: &at,
			PreviewMessage: &model.PreviewMessage{
				MessageID: fmt.Sprintf("01970a4f8c2d7c9a%04xBB", i),
				Sender: model.Participant{
					UserID:  fmt.Sprintf("01970a4f8c2d7c9a%04x", i*3),
					Account: benchWords[(i*5)%len(benchWords)], DisplayName: benchWords[(i*11)%len(benchWords)],
				},
				Content:   benchContent(i),
				CreatedAt: at,
			},
		}
		items[i] = &model.ChannelSubscription{Subscription: sub}
	}
	return &models.PagedSubscriptionListResponse{Subscriptions: items, HasMore: true}
}

var benchWords = []string{
	"engineering", "platform", "release", "incident", "design", "infra", "mobile",
	"payments", "search", "identity", "billing", "support", "growth", "data",
}

// benchContent builds a message body whose length and wording both vary.
func benchContent(i int) string {
	var b strings.Builder
	for k := 0; k <= i%9; k++ {
		fmt.Fprintf(&b, "%s update %d: rolling the %s change out to %s, ",
			benchWords[(i+k)%len(benchWords)], i+k,
			benchWords[(i*k+3)%len(benchWords)], benchWords[(i+k*2)%len(benchWords)])
	}
	return b.String()
}

// BenchmarkPageMarshal measures the JSON cost per page size.
func BenchmarkPageMarshal(b *testing.B) {
	for _, tc := range benchPages {
		b.Run(tc.name, func(b *testing.B) {
			page := benchPage(tc.rows)
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
	for _, tc := range benchPages {
		b.Run(tc.name, func(b *testing.B) {
			page := benchPage(tc.rows)
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

type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) { w.n += len(p); return len(p), nil }
