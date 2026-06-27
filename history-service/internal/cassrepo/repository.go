package cassrepo

import (
	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// Repository wraps a Cassandra session with the bucket sizer + read-walk
// configuration shared by all queries against bucketed message tables, plus
// an optional at-rest Cipher for encrypted message bodies.
type Repository struct {
	session *gocql.Session
	bucket  msgbucket.Sizer
	walkCfg walkConfig
	cipher  atrest.Cipher // nil when ATREST_ENABLED=false
}

// Option customizes a Repository at construction time.
type Option func(*Repository)

// WithReadConcurrency enables the parallel empty-bucket skip on paginated reads:
// after escalateAfter consecutive empty buckets, up to concurrency buckets are
// probed concurrently. Values <=1 / <=0 leave the walk strictly serial (the
// default), so this is opt-in and byte-compatible with the serial behavior.
func WithReadConcurrency(concurrency, escalateAfter int) Option {
	return func(r *Repository) {
		if concurrency > 1 {
			r.walkCfg.concurrency = concurrency
		}
		if escalateAfter > 0 {
			r.walkCfg.escalateAfter = escalateAfter
		}
	}
}

// NewRepository wires a session, bucket sizer, max-walk depth, and (optional)
// at-rest Cipher. maxBuckets caps how far a paginated read walks through empty
// buckets before returning a non-terminal cursor. cipher may be nil; when nil
// the read path treats encountered enc_payload rows as a configuration error
// and the write path uses legacy plaintext columns. Reads default to a serial
// bucket walk; pass WithReadConcurrency to enable parallel empty-bucket skips.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int, cipher atrest.Cipher, opts ...Option) *Repository {
	r := &Repository{
		session: session,
		bucket:  bucket,
		walkCfg: walkConfig{maxBuckets: maxBuckets, concurrency: 1, escalateAfter: 0},
		cipher:  cipher,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}
