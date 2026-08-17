package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestRemotePeers(t *testing.T) {
	tests := []struct {
		name string
		self string
		all  []string
		want []string
	}{
		{name: "drops self", self: "a", all: []string{"a", "b", "c"}, want: []string{"b", "c"}},
		{name: "single site has no peers", self: "a", all: []string{"a"}, want: nil},
		{name: "unset disables", self: "a", all: nil, want: nil},
		{name: "tolerates blanks and self repeated", self: "a", all: []string{"a", "", "b", "a"}, want: []string{"b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, remotePeers(tt.self, tt.all))
		})
	}
}

type capturePublisher struct {
	subjects []string
	payloads [][]byte
	err      error
}

func (c *capturePublisher) Publish(_ context.Context, subj string, data []byte) error {
	if c.err != nil {
		return c.err
	}
	c.subjects = append(c.subjects, subj)
	c.payloads = append(c.payloads, data)
	return nil
}

func TestRoomActivityPublisher_OneSmallEventPerPeer(t *testing.T) {
	pub := &capturePublisher{}
	send := roomActivityPublisher(pub, "site-a", []string{"site-b", "site-c"})

	at := time.UnixMilli(1740000000000).UTC()
	require.NoError(t, send(context.Background(), roomActivityRefresh{roomID: "r1", at: at}))

	// One message per destination, each carrying a single room — payload size is
	// bounded by construction, so no batch can outgrow max_payload.
	require.Len(t, pub.subjects, 2)
	assert.Equal(t, subject.RoomActivity("site-b"), pub.subjects[0])
	assert.Equal(t, subject.RoomActivity("site-c"), pub.subjects[1])

	var evt model.RoomActivityEvent
	require.NoError(t, json.Unmarshal(pub.payloads[0], &evt))
	assert.Equal(t, "r1", evt.RoomID)
	assert.Equal(t, "site-a", evt.SiteID, "the room's home site, so the destination can attribute it")
	assert.Equal(t, at.UnixMilli(), evt.LastMsgAt)
	assert.NotZero(t, evt.Timestamp)
}

func TestRoomActivityPublisher_PropagatesPublishError(t *testing.T) {
	send := roomActivityPublisher(&capturePublisher{err: errors.New("no route")}, "site-a", []string{"site-b"})
	require.Error(t, send(context.Background(), roomActivityRefresh{roomID: "r1", at: time.Now()}))
}

type fakeMetaStore struct {
	meta roommetacache.Meta
	err  error
}

func (f *fakeMetaStore) GetRoomMeta(_ context.Context, _ string) (roommetacache.Meta, error) {
	return f.meta, f.err
}

func TestCrossSiteChecker(t *testing.T) {
	yes, no := true, false
	tests := []struct {
		name string
		meta roommetacache.Meta
		err  error
		want bool
	}{
		{name: "cross-site room refreshes", meta: roommetacache.Meta{CrossSite: &yes}, want: true},
		{name: "same-site room does not", meta: roommetacache.Meta{CrossSite: &no}, want: false},
		// Unclassified is fail-safe global elsewhere; here the cost of a wrong
		// guess is one wasted publish, and the destination ignores rooms it has
		// no subscription for — so treat it as cross-site.
		{name: "unclassified refreshes", meta: roommetacache.Meta{CrossSite: nil}, want: true},
		{name: "meta read failure does not refresh", err: errors.New("down"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := crossSiteChecker(&fakeMetaStore{meta: tt.meta, err: tt.err})
			assert.Equal(t, tt.want, check(context.Background(), "r1"))
		})
	}
}
