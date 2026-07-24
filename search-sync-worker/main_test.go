package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/searchengine"
)

// fakeEngine implements searchengine.SearchEngine, recording UpdateMapping calls.
type fakeEngine struct {
	mappingPatterns []string
	mappingErr      error
}

func (f *fakeEngine) Ping(context.Context) error { return nil }
func (f *fakeEngine) Bulk(context.Context, []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	return nil, nil
}
func (f *fakeEngine) UpsertTemplate(context.Context, string, json.RawMessage) error { return nil }
func (f *fakeEngine) PutScript(context.Context, string, json.RawMessage) error      { return nil }
func (f *fakeEngine) UpdateMapping(_ context.Context, pattern string, _ json.RawMessage) error {
	f.mappingPatterns = append(f.mappingPatterns, pattern)
	return f.mappingErr
}
func (f *fakeEngine) GetIndexMapping(context.Context, string) (json.RawMessage, error) {
	return nil, nil
}
func (f *fakeEngine) GetDoc(context.Context, string, string) (json.RawMessage, bool, error) {
	return nil, false, nil
}
func (f *fakeEngine) Search(context.Context, []string, json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}

// Only collections that expose a mapping (messages) are pushed; the fixed-
// index collections (spotlight, spotlight-org, user-room) are skipped.
func TestPushMappings_PushesOnlyCollectionsWithMappings(t *testing.T) {
	eng := &fakeEngine{}
	collections := []Collection{
		newMessageCollection("messages-x-v1", "site-x", time.Time{}, false),
		newSpotlightCollection("spotlight-x", false),
		newSpotlightOrgCollection("spotlight-org-x", "s1", "hr", false),
		newUserRoomCollection("user-room-x"),
	}

	require.NoError(t, pushMappings(context.Background(), eng, collections))
	assert.Equal(t, []string{"messages-x-*"}, eng.mappingPatterns,
		"pattern is version-stripped so pre-bump indices are covered too")
}

// A non-nil but zero-length body is still "no update" — it must never
// reach the mapping endpoint as an empty PUT.
type emptyBodyMappingCollection struct{ stubCollection }

func (emptyBodyMappingCollection) MappingUpdate() (string, json.RawMessage) {
	return "messages-x-*", json.RawMessage{}
}

func TestPushMappings_SkipsEmptyBody(t *testing.T) {
	eng := &fakeEngine{}
	require.NoError(t, pushMappings(context.Background(), eng, []Collection{emptyBodyMappingCollection{}}))
	assert.Empty(t, eng.mappingPatterns, "zero-length body must be skipped")
}

func TestPushMappings_PropagatesEngineError(t *testing.T) {
	engineErr := errors.New("es down")
	eng := &fakeEngine{mappingErr: engineErr}
	collections := []Collection{newMessageCollection("messages-x-v1", "site-x", time.Time{}, false)}

	err := pushMappings(context.Background(), eng, collections)
	require.ErrorIs(t, err, engineErr, "engine failure must stay on the %w chain")
	assert.Contains(t, err.Error(), "messages-x-*", "error names the failing pattern")
	assert.Equal(t, []string{"messages-x-*"}, eng.mappingPatterns)
}
