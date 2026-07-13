//go:build integration

package testutil

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

var (
	mongoOnce      sync.Once
	mongoClient    *mongo.Client
	mongoContainer testcontainers.Container
	mongoURI       string
	mongoInitErr   error
)

func ensureMongoClient() (*mongo.Client, error) {
	mongoOnce.Do(func() {
		ctx := context.Background()
		container, err := mongodb.Run(ctx, testimages.Mongo)
		if err != nil {
			mongoInitErr = fmt.Errorf("start mongo: %w", err)
			return
		}
		uri, err := container.ConnectionString(ctx)
		if err != nil {
			_ = container.Terminate(ctx)
			mongoInitErr = fmt.Errorf("get mongo uri: %w", err)
			return
		}
		c, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			_ = container.Terminate(ctx)
			mongoInitErr = fmt.Errorf("connect mongo: %w", err)
			return
		}
		mongoClient = c
		mongoContainer = container
		mongoURI = uri
	})
	return mongoClient, mongoInitErr
}

// MongoURI returns the shared Mongo container's connection string, starting
// the container if needed. For tests that must dial their own client (e.g.
// exercising mongoutil.Connect variants) instead of using MongoDB's handle.
func MongoURI(t *testing.T) string {
	t.Helper()
	if _, err := ensureMongoClient(); err != nil {
		t.Fatalf("testutil.MongoURI: %v", err)
	}
	return mongoURI
}

// TerminateMongo disconnects the shared client and stops the shared
// container. Best-effort and idempotent — safe to call from any TestMain.
func TerminateMongo() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if mongoClient != nil {
		if err := mongoClient.Disconnect(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "disconnect shared mongo client: %v\n", err)
		}
		mongoClient = nil
	}
	if mongoContainer != nil {
		if err := mongoContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate shared mongo: %v\n", err)
		}
		mongoContainer = nil
	}
}

// EnsureMongo starts the shared Mongo container if not already started.
// No-t variant intended for TestMain pre-warming.
func EnsureMongo() error { _, err := ensureMongoClient(); return err }

// MongoDB returns an isolated Mongo database for the current test; dropped on t.Cleanup.
func MongoDB(t *testing.T, prefix string) *mongo.Database {
	t.Helper()
	c, err := ensureMongoClient()
	if err != nil {
		t.Fatalf("testutil.MongoDB: %v", err)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Name())) // hash.Hash.Write never returns an error.
	db := c.Database(fmt.Sprintf("%s_%x", prefix, h.Sum64()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = db.Drop(ctx)
	})
	return db
}
