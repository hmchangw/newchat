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

	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const cassandraImage = "cassandra:5"

var (
	cassOnce      sync.Once
	cassContainer testcontainers.Container
	cassHost      string
	cassSession   *gocql.Session
	cassInitErr   error
)

func ensureCassandraSession() (string, *gocql.Session, error) {
	cassOnce.Do(func() {
		ctx := context.Background()
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        cassandraImage,
				ExposedPorts: []string{"9042/tcp"},
				Env:          map[string]string{"JVM_OPTS": "-Xms512m -Xmx512m"},
				WaitingFor: wait.ForLog("Starting listening for CQL clients").
					WithStartupTimeout(5 * time.Minute),
			},
			Started: true,
		})
		if err != nil {
			cassInitErr = fmt.Errorf("start cassandra: %w", err)
			return
		}
		host, err := container.Host(ctx)
		if err != nil {
			_ = container.Terminate(ctx)
			cassInitErr = fmt.Errorf("get cassandra host: %w", err)
			return
		}
		port, err := container.MappedPort(ctx, "9042")
		if err != nil {
			_ = container.Terminate(ctx)
			cassInitErr = fmt.Errorf("get cassandra port: %w", err)
			return
		}
		addr := fmt.Sprintf("%s:%s", host, port.Port())
		cluster := gocql.NewCluster(addr)
		cluster.Consistency = gocql.One
		cluster.ConnectTimeout = 30 * time.Second
		cluster.Timeout = 30 * time.Second
		// Cassandra inside Docker reports its rpc_address as the container's
		// internal IP via system.local. Skip discovery so gocql sticks with
		// the host:port we already obtained from the testcontainer.
		cluster.DisableInitialHostLookup = true
		s, err := cluster.CreateSession()
		if err != nil {
			_ = container.Terminate(ctx)
			cassInitErr = fmt.Errorf("create cassandra session: %w", err)
			return
		}
		cassHost = addr
		cassSession = s
		cassContainer = container
	})
	return cassHost, cassSession, cassInitErr
}

// TerminateCassandra closes the shared session and stops the shared
// container. Best-effort, idempotent.
func TerminateCassandra() {
	if cassSession != nil {
		cassSession.Close()
		cassSession = nil
	}
	if cassContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := cassContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "terminate shared cassandra: %v\n", err)
		}
		cassContainer = nil
	}
}

// EnsureCassandra starts the shared Cassandra container if not already
// started. No-t variant intended for TestMain pre-warming.
func EnsureCassandra() error { _, _, err := ensureCassandraSession(); return err }

// CassandraKeyspace creates an isolated keyspace for the test (SimpleStrategy, RF=1).
// Returns the keyspace name, an admin session for DDL, and the container host.
func CassandraKeyspace(t *testing.T, prefix string) (keyspace string, admin *gocql.Session, hostAddr string) {
	t.Helper()
	h, s, err := ensureCassandraSession()
	if err != nil {
		t.Fatalf("testutil.CassandraKeyspace: %v", err)
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(t.Name())) // hash.Hash.Write never returns an error.
	keyspace = fmt.Sprintf("%s_%x", prefix, hash.Sum64())
	if err := s.Query(fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
		keyspace,
	)).Exec(); err != nil {
		t.Fatalf("create keyspace %s: %v", keyspace, err)
	}
	t.Cleanup(func() {
		if err := s.Query(`DROP KEYSPACE IF EXISTS ` + keyspace).Exec(); err != nil {
			t.Errorf("drop keyspace %s: %v", keyspace, err)
		}
	})
	return keyspace, s, h
}
