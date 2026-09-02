package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestUpsertModel_BuildsUpdateOneModelWithUpsert(t *testing.T) {
	filter := bson.M{"_id": "x"}
	update := bson.M{"$set": bson.M{"name": "alice"}}

	uo := UpsertModel(filter, update)
	require.NotNil(t, uo)

	assert.Equal(t, filter, uo.Filter)
	assert.Equal(t, update, uo.Update)
	require.NotNil(t, uo.Upsert)
	assert.True(t, *uo.Upsert)
}

func TestDeleteModel_BuildsDeleteOneModel(t *testing.T) {
	filter := bson.M{"_id": "x"}

	d := DeleteModel(filter)
	require.NotNil(t, d)

	assert.Equal(t, filter, d.Filter)
}

func TestFromDriverResult_NilInput(t *testing.T) {
	result := fromDriverResult(nil)
	assert.Nil(t, result)
}

func TestFromDriverResult_ConvertsBulkWriteResult(t *testing.T) {
	input := &mongo.BulkWriteResult{
		InsertedCount: 1,
		MatchedCount:  2,
		ModifiedCount: 3,
		DeletedCount:  4,
		UpsertedCount: 5,
		UpsertedIDs:   map[int64]any{0: "id1", 2: "id2"},
		Acknowledged:  true,
	}

	result := fromDriverResult(input)

	require.NotNil(t, result)
	assert.Equal(t, int64(2), result.Matched)
	assert.Equal(t, int64(3), result.Modified)
	assert.Equal(t, int64(5), result.Upserted)
	assert.Equal(t, int64(1), result.Inserted)
	assert.Equal(t, int64(4), result.Deleted)
	assert.Equal(t, map[int64]any{0: "id1", 2: "id2"}, result.UpsertedIDs)
	assert.True(t, result.Acknowledged)
}

func TestBsonSetWithoutID_StripsIDField(t *testing.T) {
	type doc struct {
		ID   string `bson:"_id"`
		Name string `bson:"name"`
		Age  int    `bson:"age"`
	}

	m, id, err := bsonSetWithoutID(doc{ID: "x", Name: "alice", Age: 30})
	require.NoError(t, err)

	_, hasID := m["_id"]
	assert.False(t, hasID, "_id must be stripped from $set")
	assert.Equal(t, "x", id, "stripped _id returned for $setOnInsert")
	assert.Equal(t, "alice", m["name"])
	assert.EqualValues(t, 30, m["age"])
}

func TestBsonSetWithoutID_NoIDField_NoOp(t *testing.T) {
	type doc struct {
		Name string `bson:"name"`
		Age  int    `bson:"age"`
	}

	m, id, err := bsonSetWithoutID(doc{Name: "bob", Age: 25})
	require.NoError(t, err)

	_, hasID := m["_id"]
	assert.False(t, hasID, "no _id key should be present")
	assert.Nil(t, id, "no _id field → nil id → no $setOnInsert")
	assert.Equal(t, "bob", m["name"])
	assert.EqualValues(t, 25, m["age"])
}

func TestBsonSetWithoutID_MarshalError(t *testing.T) {
	type doc struct {
		Ch chan int `bson:"ch"`
	}

	_, _, err := bsonSetWithoutID(doc{Ch: make(chan int)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bson marshal")
}

func TestChunkWriteModels(t *testing.T) {
	model := func(i int) mongo.WriteModel {
		return UpsertModel(bson.M{"_id": i}, bson.M{"$set": bson.M{"n": i}})
	}
	models := func(n int) []mongo.WriteModel {
		out := make([]mongo.WriteModel, 0, n)
		for i := range n {
			out = append(out, model(i))
		}
		return out
	}

	tests := []struct {
		name       string
		models     []mongo.WriteModel
		size       int
		wantChunks []int // length of each chunk
	}{
		{"empty input yields no chunks", nil, 10, nil},
		{"fewer than one chunk", models(3), 10, []int{3}},
		{"exact multiple", models(20), 10, []int{10, 10}},
		{"trailing partial chunk", models(25), 10, []int{10, 10, 5}},
		{"single element chunks", models(3), 1, []int{1, 1, 1}},
		{"zero size falls back to the default", models(2), 0, []int{2}},
		{"negative size falls back to the default", models(2), -5, []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkWriteModels(tt.models, tt.size)
			require.Len(t, got, len(tt.wantChunks))

			var seen int
			for i, chunk := range got {
				assert.Len(t, chunk, tt.wantChunks[i])
				seen += len(chunk)
			}
			assert.Equal(t, len(tt.models), seen, "chunking must not drop or duplicate models")
		})
	}
}

// A chunk never exceeds the cap, whatever the input size.
func TestChunkWriteModels_RespectsCap(t *testing.T) {
	models := make([]mongo.WriteModel, 0, MaxBulkChunk*2+7)
	for i := range MaxBulkChunk*2 + 7 {
		models = append(models, UpsertModel(bson.M{"_id": i}, bson.M{"$set": bson.M{"n": i}}))
	}
	chunks := chunkWriteModels(models, MaxBulkChunk)

	require.Len(t, chunks, 3)
	for _, c := range chunks {
		assert.LessOrEqual(t, len(c), MaxBulkChunk)
	}
	assert.Len(t, chunks[2], 7)
}

func TestBulkResult_Merge(t *testing.T) {
	acc := &BulkResult{Acknowledged: true}
	acc.merge(&mongo.BulkWriteResult{
		MatchedCount: 1, ModifiedCount: 2, UpsertedCount: 3, InsertedCount: 4, DeletedCount: 5,
		UpsertedIDs: map[int64]any{0: "a"}, Acknowledged: true,
	}, 0)
	acc.merge(&mongo.BulkWriteResult{
		MatchedCount: 10, ModifiedCount: 20, UpsertedCount: 30, InsertedCount: 40, DeletedCount: 50,
		UpsertedIDs: map[int64]any{0: "b"}, Acknowledged: true,
	}, 1000)

	assert.Equal(t, int64(11), acc.Matched)
	assert.Equal(t, int64(22), acc.Modified)
	assert.Equal(t, int64(33), acc.Upserted)
	assert.Equal(t, int64(44), acc.Inserted)
	assert.Equal(t, int64(55), acc.Deleted)
	// Rebased onto the whole input, so chunk two's index 0 does not overwrite.
	assert.Equal(t, map[int64]any{0: "a", 1000: "b"}, acc.UpsertedIDs)
}

// One unacknowledged chunk makes the whole result unacknowledged.
func TestBulkResult_MergeUnacknowledged(t *testing.T) {
	acc := &BulkResult{Acknowledged: true}
	acc.merge(&mongo.BulkWriteResult{Acknowledged: true}, 0)
	acc.merge(&mongo.BulkWriteResult{Acknowledged: false}, 10)
	assert.False(t, acc.Acknowledged)
}

// A size larger than the input must yield one chunk, not overflow the capacity
// computation into a negative make().
func TestChunkWriteModels_HugeSizeDoesNotOverflow(t *testing.T) {
	// Two or more models are needed: len+size-1 is what wraps past MaxInt.
	models := []mongo.WriteModel{
		UpsertModel(bson.M{"_id": 1}, bson.M{"$set": bson.M{"n": 1}}),
		UpsertModel(bson.M{"_id": 2}, bson.M{"$set": bson.M{"n": 2}}),
	}

	assert.NotPanics(t, func() {
		chunks := chunkWriteModels(models, math.MaxInt)
		require.Len(t, chunks, 1)
		assert.Len(t, chunks[0], 2)
	})
}

// Unordered BulkWrite keeps going past per-model write errors, so chunking must
// not turn one bad model into a barrier for every later chunk.
func TestTerminalBulkError(t *testing.T) {
	writeErr := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{}}}
	wcErr := mongo.BulkWriteException{
		WriteErrors:       []mongo.BulkWriteError{{}},
		WriteConcernError: &mongo.WriteConcernError{Name: "wc"},
	}

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"per-model write errors keep unordered semantics", context.Background(), writeErr, false},
		{"wrapped per-model write errors are still per-model", context.Background(), fmt.Errorf("chunk: %w", writeErr), false},
		{"a write-concern failure is terminal", context.Background(), wcErr, true},
		{"an exception carrying no write errors is terminal", context.Background(), mongo.BulkWriteException{}, true},
		{"a transport error is terminal", context.Background(), errors.New("connection reset"), true},
		{"a cancelled context is terminal whatever the error", canceledCtx(), writeErr, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, terminalBulkError(tt.ctx, tt.err))
		})
	}
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
