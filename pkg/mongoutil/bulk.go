package mongoutil

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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

// MaxBulkChunk is the default number of write models sent in one BulkWrite,
// bounding what a caller with tens of thousands of models builds in memory.
const MaxBulkChunk = 1000

// chunkWriteModels splits models into ordered batches of size (MaxBulkChunk if <=0).
func chunkWriteModels(models []mongo.WriteModel, size int) [][]mongo.WriteModel {
	if size <= 0 {
		size = MaxBulkChunk
	}
	if len(models) == 0 {
		return nil
	}
	chunks := make([][]mongo.WriteModel, 0, (len(models)+size-1)/size)
	for start := 0; start < len(models); start += size {
		end := min(start+size, len(models))
		chunks = append(chunks, models[start:end])
	}
	return chunks
}

// merge folds a chunk's result into the accumulator, rebasing UpsertedIDs
// ordinals by base so each chunk's index 0 does not overwrite the last.
func (r *BulkResult) merge(res *mongo.BulkWriteResult, base int) {
	if res == nil {
		return
	}
	r.Matched += res.MatchedCount
	r.Modified += res.ModifiedCount
	r.Upserted += res.UpsertedCount
	r.Inserted += res.InsertedCount
	r.Deleted += res.DeletedCount
	if !res.Acknowledged {
		r.Acknowledged = false
	}
	for ordinal, id := range res.UpsertedIDs {
		if r.UpsertedIDs == nil {
			r.UpsertedIDs = make(map[int64]any, len(res.UpsertedIDs))
		}
		r.UpsertedIDs[int64(base)+ordinal] = id
	}
}

// ChunkedBulkWrite issues models as unordered BulkWrites of at most size each,
// merged into one result. Chunks are not atomic, so callers must be idempotent.
func ChunkedBulkWrite(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel, size int) (*BulkResult, error) {
	if len(models) == 0 {
		return nil, nil
	}
	opts := options.BulkWrite().SetOrdered(false)
	out := &BulkResult{Acknowledged: true}
	base := 0
	for _, chunk := range chunkWriteModels(models, size) {
		res, err := coll.BulkWrite(ctx, chunk, opts)
		if err != nil {
			return out, fmt.Errorf("bulk write models %d-%d of %d: %w", base, base+len(chunk), len(models), err)
		}
		out.merge(res, base)
		base += len(chunk)
	}
	return out, nil
}
