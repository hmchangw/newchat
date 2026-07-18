package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDedupID_DeterministicAndOrderInsensitive(t *testing.T) {
	a := dedupID("site-a", []string{"c1", "c2", "c3"})
	b := dedupID("site-a", []string{"c3", "c1", "c2"}) // different order, same set
	assert.Equal(t, a, b, "dedup id must not depend on chat-id order")
	assert.Contains(t, a, "teamroom:site-a:")
}

func TestDedupID_DistinctSets(t *testing.T) {
	assert.NotEqual(t, dedupID("site-a", []string{"c1"}), dedupID("site-a", []string{"c2"}))
	assert.NotEqual(t, dedupID("site-a", []string{"c1"}), dedupID("site-b", []string{"c1"}))
}
