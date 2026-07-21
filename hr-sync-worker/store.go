package main

import (
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/hrstore"
)

// Store is the write surface this worker persists into. The interface and
// its Mongo implementation live in pkg/hrstore, shared with teams-hr-sync's
// direct-write mode; this alias minimizes churn at existing call sites.
type Store = hrstore.Store

// MockStore is re-exported from hrstore so handler_test.go's NewMockStore
// calls keep working unchanged.
type MockStore = hrstore.MockStore

var NewMockStore = hrstore.NewMockStore

func newMongoStore(db *mongo.Database) *hrstore.MongoStore {
	return hrstore.NewMongoStore(db)
}
