package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRawChangeDoc_ToChangeEvent_Valid(t *testing.T) {
	id, err := bson.Marshal(bson.M{"_data": "RT-ABC"})
	require.NoError(t, err)
	rt, err := bson.Marshal(bson.M{"_data": "RT-ABC"})
	require.NoError(t, err)
	docKey, err := bson.Marshal(bson.M{"_id": "doc1"})
	require.NoError(t, err)

	d := &rawChangeDoc{
		ID:            id,
		OperationType: "insert",
		DocumentKey:   docKey,
	}
	d.Ns.DB = "rocketchat"
	d.Ns.Coll = "rocketchat_message"
	d.ClusterTime = bson.Timestamp{T: 1718100000, I: 0}

	ce, err := d.toChangeEvent(bson.Raw(rt))
	require.NoError(t, err)
	assert.Equal(t, "RT-ABC", ce.EventID)
	assert.Equal(t, "insert", ce.Op)
	assert.Equal(t, "rocketchat", ce.DB)
	assert.Equal(t, "rocketchat_message", ce.Collection)
	assert.Equal(t, bson.Raw(docKey), ce.DocumentKey)
	assert.Equal(t, bson.Raw(rt), ce.ResumeToken)
	assert.Equal(t, int64(1718100000)*1000, ce.ClusterTimeMs)
}

func TestRawChangeDoc_ToChangeEvent_MissingData(t *testing.T) {
	id, err := bson.Marshal(bson.M{})
	require.NoError(t, err)

	d := &rawChangeDoc{ID: id, OperationType: "insert"}

	_, err = d.toChangeEvent(bson.Raw(id))
	require.Error(t, err, "empty _id._data must be surfaced as an error")
}

func TestRawChangeDoc_ToChangeEvent_Malformed(t *testing.T) {
	d := &rawChangeDoc{
		ID:            bson.Raw{0x05, 0x00, 0x00, 0x00, 0x01}, // invalid BSON
		OperationType: "insert",
	}

	_, err := d.toChangeEvent(bson.Raw{})
	require.Error(t, err, "malformed resume token _id must be surfaced as an error")
}
