package failure

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureEvent_JSONContract(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 2, 3, 0, time.UTC)
	event := Event{
		SchemaVersion: WALSchemaVersion,
		Type:          EventObserved,
		OperationID:   "operation-1",
		Observer:      ObserverHistory,
		Observation:   ObservationGood,
		At:            now,
	}

	encoded, err := json.Marshal(event)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"schemaVersion": 2,
		"type": "observed",
		"operationId": "operation-1",
		"observer": "cassandra_history",
		"observation": "good",
		"at": "2026-09-09T01:02:03Z"
	}`, string(encoded))
}
