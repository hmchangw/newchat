package cassutil

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	o11ycassandra "github.com/flywindy/o11y/cassandra"
	"github.com/gocql/gocql"
)

// defaultNumConns is the per-host connection count used when Config.NumConns
// is unset (zero or negative). gocql's own default is 2, which underprovisions
// any service issuing concurrent queries — history-service's bucket walks and
// message-worker's per-message inserts both push enough concurrency that two
// connections per host queue requests at the driver level.
const defaultNumConns = 8

// Config bundles the connection parameters for Connect. The caller's service
// owns the env binding (e.g. `CASSANDRA_NUM_CONNS`); cassutil only consumes
// the resolved struct.
type Config struct {
	Hosts    string // comma-separated "host:port,host:port"
	Keyspace string
	Username string
	Password string
	NumConns int // per-host connection count; 0 or negative → defaultNumConns
}

func Connect(cfg Config, opts ...Option) (*gocql.Session, error) {
	cluster := buildCluster(parseHosts(cfg.Hosts), cfg.Keyspace, cfg.Username, cfg.Password, cfg.NumConns)
	cc := newConnectConfig(opts...)

	var (
		session *gocql.Session
		err     error
	)
	if cc.obs != nil {
		// gocql attaches observers via the ClusterConfig, so o11y must build the
		// session rather than wrap a live one. Batch spans additionally require
		// o11ycassandra.ExecuteBatch at the call site (a Phase 3 concern).
		session, err = o11ycassandra.NewSession(cluster, cc.obs.TracerProvider(), cc.obs.MeterProvider())
	} else {
		session, err = cluster.CreateSession()
	}
	if err != nil {
		return nil, fmt.Errorf("cassandra connect: %w", err)
	}
	slog.Info("connected to Cassandra", "keyspace", cfg.Keyspace, "num_conns", cluster.NumConns)
	return session, nil
}

func Close(session *gocql.Session) {
	session.Close()
}

func parseHosts(s string) []string {
	var hosts []string
	for _, h := range strings.Split(s, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		hosts = append(hosts, h)
	}
	return hosts
}

func buildCluster(hosts []string, keyspace, username, password string, numConns int) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 10 * time.Second
	// Route single-partition queries straight to a replica instead of gocql's
	// default round-robin coordinator, removing a coordinator-forward hop.
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	if numConns > 0 {
		cluster.NumConns = numConns
	} else {
		cluster.NumConns = defaultNumConns
	}
	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	return cluster
}
