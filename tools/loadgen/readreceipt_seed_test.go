package main

import (
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func subs(n int) []model.Subscription {
	out := make([]model.Subscription, n)
	for i := 0; i < n; i++ {
		out[i] = model.Subscription{ID: string(rune('a' + i))}
	}
	return out
}

func TestSelectReaders_Count(t *testing.T) {
	in := subs(10)
	got := selectReaders(in, 0.7, rand.New(rand.NewSource(1)))
	assert.Len(t, got, 7) // ceil(0.7 * 10) == 7
}

func TestSelectReaders_RatioOneSelectsAll(t *testing.T) {
	in := subs(5)
	got := selectReaders(in, 1.0, rand.New(rand.NewSource(1)))
	assert.Len(t, got, 5)
}

func TestSelectReaders_Deterministic(t *testing.T) {
	in := subs(20)
	a := selectReaders(in, 0.5, rand.New(rand.NewSource(99)))
	b := selectReaders(in, 0.5, rand.New(rand.NewSource(99)))
	assert.Equal(t, a, b)
}

func TestSelectReaders_EmptyAndTiny(t *testing.T) {
	assert.Empty(t, selectReaders(nil, 0.7, rand.New(rand.NewSource(1))))
	assert.Len(t, selectReaders(subs(1), 0.7, rand.New(rand.NewSource(1))), 1) // ceil(0.7 * 1) == 1
}

func TestLatestTopLevelByRoom(t *testing.T) {
	t0 := time.Unix(1000, 0).UTC()
	plan := &MessagePlan{Messages: []plannedMessage{
		{RoomID: "r1", MessageID: "m1", CreatedAt: t0, ThreadParentID: ""},
		{RoomID: "r1", MessageID: "m2", CreatedAt: t0.Add(time.Hour), ThreadParentID: ""},
		{RoomID: "r1", MessageID: "rep", CreatedAt: t0.Add(2 * time.Hour), ThreadParentID: "m1"}, // reply ignored
		{RoomID: "r2", MessageID: "m3", CreatedAt: t0.Add(30 * time.Minute), ThreadParentID: ""},
	}}
	got := latestTopLevelByRoom(plan)
	assert.Equal(t, t0.Add(time.Hour), got["r1"])
	assert.Equal(t, t0.Add(30*time.Minute), got["r2"])
	assert.NotContains(t, got, "rep")
}
