//go:build integration

package testutil

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/url"
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

// A single-node replica-set Mongo, separate from the standalone shared Mongo
// (MongoDB). Only callers that need multi-document transactions use this — the
// rest of the suite stays on the cheaper standalone container.

var (
	mongoRSOnce      sync.Once
	mongoRSClient    *mongo.Client
	mongoRSContainer testcontainers.Container
	mongoRSInitErr   error
)

func ensureMongoRSClient() (*mongo.Client, error) {
	mongoRSOnce.Do(func() {
		ctx := context.Background()
		container, err := mongodb.Run(ctx, testimages.Mongo, mongodb.WithReplicaSet("rs0"))
		if err != nil {
			mongoRSInitErr = fmt.Errorf("start mongo replica set: %w", err)
			return
		}
		uri, err := container.ConnectionString(ctx)
		if err != nil {
			_ = container.Terminate(ctx) // best-effort cleanup after a URI failure
			mongoRSInitErr = fmt.Errorf("get mongo rs uri: %w", err)
			return
		}
		// The module initiates the replica set advertising the container's
		// internal Docker IP, which the driver can't reach from the host for
		// SDAM. Connect directly to the mapped port (directConnection) instead
		// of via replica-set discovery — the node is still a replica-set
		// primary, so transactions work.
		directURI, err := toDirectConnectionURI(uri)
		if err != nil {
			_ = container.Terminate(ctx) // best-effort cleanup after a URI failure
			mongoRSInitErr = fmt.Errorf("rewrite mongo rs uri: %w", err)
			return
		}
		c, err := mongo.Connect(options.Client().ApplyURI(directURI))
		if err != nil {
			_ = container.Terminate(ctx) // best-effort cleanup after a connect failure
			mongoRSInitErr = fmt.Errorf("connect mongo rs: %w", err)
			return
		}
		mongoRSClient = c
		mongoRSContainer = container
	})
	return mongoRSClient, mongoRSInitErr
}

// toDirectConnectionURI drops the replicaSet query param and forces
// directConnection=true so the driver talks straight to the mapped port.
func toDirectConnectionURI(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("parse uri: %w", err)
	}
	q := u.Query()
	q.Del("replicaSet")
	q.Set("directConnection", "true")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// TerminateMongoReplicaSet disconnects the client and stops the replica-set
// container. Best-effort and idempotent — safe from any TestMain.
func TerminateMongoReplicaSet() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if mongoRSClient != nil {
		if err := mongoRSClient.Disconnect(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "disconnect shared mongo rs client: %v\n", err)
		}
		mongoRSClient = nil
	}
	if mongoRSContainer != nil {
		if err := mongoRSContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate shared mongo rs: %v\n", err)
		}
		mongoRSContainer = nil
	}
}

// EnsureMongoReplicaSet starts the replica-set container if not already up.
// No-t variant intended for TestMain pre-warming.
func EnsureMongoReplicaSet() error { _, err := ensureMongoRSClient(); return err }

// MongoDBReplicaSet returns an isolated database on the replica-set Mongo for
// the current test; dropped on t.Cleanup. Use only when the code under test
// needs multi-document transactions.
func MongoDBReplicaSet(t *testing.T, prefix string) *mongo.Database {
	t.Helper()
	c, err := ensureMongoRSClient()
	if err != nil {
		t.Fatalf("testutil.MongoDBReplicaSet: %v", err)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Name())) // hash.Hash.Write never returns an error.
	db := c.Database(fmt.Sprintf("%s_%x", prefix, h.Sum64()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = db.Drop(ctx) // best-effort: the per-test DB is ephemeral
	})
	return db
}
