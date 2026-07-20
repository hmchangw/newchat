package mongoutil

import (
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
