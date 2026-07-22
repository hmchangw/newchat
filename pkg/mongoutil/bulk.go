package mongoutil

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// BulkResult mirrors mongo.BulkWriteResult; bulk methods return (nil, nil) on empty input.
type BulkResult struct {
	Matched      int64
	Modified     int64
	Upserted     int64
	Inserted     int64
	Deleted      int64
	UpsertedIDs  map[int64]any // ordinal -> _id; non-contiguous under unordered partial failures
	Acknowledged bool
}

func UpsertModel(filter, update any) *mongo.UpdateOneModel {
	return mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
}

func DeleteModel(filter any) *mongo.DeleteOneModel {
	return mongo.NewDeleteOneModel().SetFilter(filter)
}

func fromDriverResult(r *mongo.BulkWriteResult) *BulkResult {
	if r == nil {
		return nil
	}
	return &BulkResult{
		Matched:      r.MatchedCount,
		Modified:     r.ModifiedCount,
		Upserted:     r.UpsertedCount,
		Inserted:     r.InsertedCount,
		Deleted:      r.DeletedCount,
		UpsertedIDs:  r.UpsertedIDs,
		Acknowledged: r.Acknowledged,
	}
}

// bsonSetWithoutID marshals item into a $set payload with _id removed (MongoDB
// rejects updates that touch the immutable _id) and returns the stripped _id
// separately so an upsert can $setOnInsert it — otherwise a non-_id-filter
// insert lets Mongo auto-generate an ObjectID, breaking string-_id readers.
// id is nil when the item carries no _id (e.g. an omitempty field left unset).
func bsonSetWithoutID(item any) (set bson.M, id any, err error) {
	raw, err := bson.Marshal(item)
	if err != nil {
		return nil, nil, fmt.Errorf("bson marshal: %w", err)
	}
	var m bson.M
	if err := bson.Unmarshal(raw, &m); err != nil {
		return nil, nil, fmt.Errorf("bson unmarshal: %w", err)
	}
	id = m["_id"]
	delete(m, "_id")
	return m, id, nil
}
