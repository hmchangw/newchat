//go:build integration

package mongoutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
)

func setupMongo(t *testing.T) *mongo.Database {
	return testutil.MongoDB(t, "mongoutil_test")
}

type testDoc struct {
	ID   string `bson:"_id"`
	Name string `bson:"name"`
	Age  int    `bson:"age"`
}

func TestCollection_FindOne_Success(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("test_docs"))

	_, err := db.Collection("test_docs").InsertOne(ctx, testDoc{ID: "d1", Name: "Alice", Age: 30})
	require.NoError(t, err)

	result, err := col.FindOne(ctx, bson.M{"name": "Alice"})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "d1", result.ID)
	assert.Equal(t, "Alice", result.Name)
}

func TestCollection_FindOne_NotFound_ReturnsNilNil(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("test_docs"))

	result, err := col.FindOne(ctx, bson.M{"name": "Nobody"})
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestCollection_FindByID(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("test_docs"))

	_, err := db.Collection("test_docs").InsertOne(ctx, testDoc{ID: "d1", Name: "Bob", Age: 25})
	require.NoError(t, err)

	result, err := col.FindByID(ctx, "d1")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "Bob", result.Name)
}

func TestCollection_FindByID_NotFound(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("test_docs"))

	result, err := col.FindByID(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestCollection_FindMany_Success(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("test_docs"))

	_, err := db.Collection("test_docs").InsertMany(ctx, []interface{}{
		testDoc{ID: "d1", Name: "Alice", Age: 30},
		testDoc{ID: "d2", Name: "Bob", Age: 25},
		testDoc{ID: "d3", Name: "Charlie", Age: 35},
	})
	require.NoError(t, err)

	results, err := col.FindMany(ctx, bson.M{"age": bson.M{"$gte": 30}})
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestCollection_FindMany_Empty_ReturnsEmptySlice(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("test_docs"))

	results, err := col.FindMany(ctx, bson.M{"name": "Nobody"})
	require.NoError(t, err)
	assert.NotNil(t, results)
	assert.Empty(t, results)
}

func TestCollection_Raw(t *testing.T) {
	db := setupMongo(t)
	col := NewCollection[testDoc](db.Collection("test_docs"))

	raw := col.Raw()
	assert.Equal(t, "test_docs", raw.Name())
}

func TestCollection_Aggregate_ReturnsMatchingDocs(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("agg_docs"))

	_, err := db.Collection("agg_docs").InsertMany(ctx, []interface{}{
		testDoc{ID: "d1", Name: "Alice", Age: 30},
		testDoc{ID: "d2", Name: "Bob", Age: 25},
		testDoc{ID: "d3", Name: "Charlie", Age: 35},
	})
	require.NoError(t, err)

	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.M{"age": bson.M{"$gte": 30}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "age", Value: 1}}}},
	}

	results, err := col.Aggregate(ctx, pipeline)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, "d1", results[0].ID)
	assert.Equal(t, "d3", results[1].ID)
}

func TestCollection_Aggregate_Empty_ReturnsEmptySlice(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("agg_docs_empty"))

	pipeline := bson.A{bson.D{{Key: "$match", Value: bson.M{"name": "Nobody"}}}}

	results, err := col.Aggregate(ctx, pipeline)
	require.NoError(t, err)
	assert.NotNil(t, results)
	assert.Empty(t, results)
}

func TestCollection_AggregatePaged_ReturnsDataAndTotal(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("paged_docs"))

	_, err := db.Collection("paged_docs").InsertMany(ctx, []interface{}{
		testDoc{ID: "d1", Name: "A", Age: 1},
		testDoc{ID: "d2", Name: "B", Age: 2},
		testDoc{ID: "d3", Name: "C", Age: 3},
		testDoc{ID: "d4", Name: "D", Age: 4},
		testDoc{ID: "d5", Name: "E", Age: 5},
	})
	require.NoError(t, err)

	pipeline := bson.A{
		bson.D{{Key: "$sort", Value: bson.D{{Key: "age", Value: 1}}}},
	}

	page, err := col.AggregatePaged(ctx, pipeline, NewOffsetPageRequest(0, 2))
	require.NoError(t, err)
	assert.Equal(t, int64(5), page.Total)
	require.Len(t, page.Data, 2)
	assert.Equal(t, "d1", page.Data[0].ID)
	assert.Equal(t, "d2", page.Data[1].ID)

	page2, err := col.AggregatePaged(ctx, pipeline, NewOffsetPageRequest(2, 2))
	require.NoError(t, err)
	assert.Equal(t, int64(5), page2.Total)
	require.Len(t, page2.Data, 2)
	assert.Equal(t, "d3", page2.Data[0].ID)
	assert.Equal(t, "d4", page2.Data[1].ID)
}

func TestCollection_AggregatePaged_EmptyCollection(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("paged_docs_empty"))

	pipeline := bson.A{bson.D{{Key: "$match", Value: bson.M{"name": "Nobody"}}}}

	page, err := col.AggregatePaged(ctx, pipeline, NewOffsetPageRequest(0, 10))
	require.NoError(t, err)
	assert.Equal(t, int64(0), page.Total)
	assert.NotNil(t, page.Data)
	assert.Empty(t, page.Data)
}

func TestCollection_AggregatePagedHasMore(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("hasmore_docs"))

	_, err := db.Collection("hasmore_docs").InsertMany(ctx, []interface{}{
		testDoc{ID: "d1", Name: "A", Age: 1},
		testDoc{ID: "d2", Name: "B", Age: 2},
		testDoc{ID: "d3", Name: "C", Age: 3},
		testDoc{ID: "d4", Name: "D", Age: 4},
		testDoc{ID: "d5", Name: "E", Age: 5},
	})
	require.NoError(t, err)

	pipeline := bson.A{bson.D{{Key: "$sort", Value: bson.D{{Key: "age", Value: 1}}}}}

	cases := []struct {
		name        string
		req         OffsetPageRequest
		wantHasMore bool
		wantIDs     []string
	}{
		{"middle page over-fetches +1 and trims, more remain", OffsetPageRequest{Offset: 0, Limit: 2}, true, []string{"d1", "d2"}},
		{"last partial page reports no more", OffsetPageRequest{Offset: 4, Limit: 2}, false, []string{"d5"}},
		{"exact-fit full page (nothing beyond) reports no more", OffsetPageRequest{Offset: 0, Limit: 5}, false, []string{"d1", "d2", "d3", "d4", "d5"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page, err := col.AggregatePagedHasMore(ctx, pipeline, tc.req)
			require.NoError(t, err)
			assert.Equal(t, tc.wantHasMore, page.HasMore)
			require.Len(t, page.Data, len(tc.wantIDs))
			for i, id := range tc.wantIDs {
				assert.Equal(t, id, page.Data[i].ID, "row %d", i)
			}
		})
	}
}

func TestCollection_AggregatePagedHasMore_EmptyCollection(t *testing.T) {
	db := setupMongo(t)
	ctx := context.Background()
	col := NewCollection[testDoc](db.Collection("hasmore_docs_empty"))

	pipeline := bson.A{bson.D{{Key: "$match", Value: bson.M{"name": "Nobody"}}}}

	page, err := col.AggregatePagedHasMore(ctx, pipeline, OffsetPageRequest{Offset: 0, Limit: 10})
	require.NoError(t, err)
	assert.False(t, page.HasMore)
	assert.NotNil(t, page.Data, "Data must be non-nil so JSON marshals to []")
	assert.Empty(t, page.Data)
}
