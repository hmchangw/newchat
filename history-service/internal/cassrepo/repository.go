package cassrepo

import (
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/bucketcache"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// defaultWalkFanout caps how many buckets a bucketed-table read fetches
// concurrently once the walk fans out past its start bucket. It trades a bounded
// amount of speculative over-read for turning a long serial run of sparse/empty
// buckets into a few concurrent round-trips. It is kept at or below the per-host
// connection count in pkg/cassutil (default 8, CASSANDRA_NUMCONNS) so a single
// walk stays within one host's pool — if that connection count is lowered below
// this, revisit this value too.
const defaultWalkFanout = 8

// Repository wraps a Cassandra session with the bucket sizer + read-walk
// configuration shared by all queries against bucketed message tables, plus
// an optional at-rest Cipher for encrypted message bodies and an optional
// per-bucket read cache.
type Repository struct {
	session    *gocql.Session
	bucket     msgbucket.Sizer
	maxBuckets int
	walkFanout int
	cipher     atrest.Cipher // nil when ATREST_ENABLED=false

	// bucketCache serves sealed message buckets for the DESC LoadHistory reads;
	// nil disables per-bucket caching. maxCacheRows caps how many rows a bucket
	// may hold to be cacheable (larger buckets read live). now is the clock for
	// the sealed-bucket test, overridable in tests.
	bucketCache  *bucketcache.Cache
	maxCacheRows int
	now          func() time.Time
}

// Option configures optional Repository behavior.
type Option func(*Repository)

// WithBucketCache enables the per-bucket sealed-message cache. maxRows caps how
// many rows a bucket may hold to be cacheable; larger buckets are read live.
func WithBucketCache(c *bucketcache.Cache, maxRows int) Option {
	return func(r *Repository) {
		r.bucketCache = c
		r.maxCacheRows = maxRows
	}
}

// withClock overrides the sealed-test clock. Test-only.
func withClock(f func() time.Time) Option {
	return func(r *Repository) { r.now = f }
}

// NewRepository wires a session, bucket sizer, max-walk depth, and (optional)
// at-rest Cipher. maxBuckets caps how far a paginated read walks through empty
// buckets before returning a non-terminal cursor. cipher may be nil; when nil
// the read path treats encountered enc_payload rows as a configuration error
// and the write path uses legacy plaintext columns. Optional per-bucket caching
// is enabled via WithBucketCache.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int, cipher atrest.Cipher, opts ...Option) *Repository {
	r := &Repository{
		session:    session,
		bucket:     bucket,
		maxBuckets: maxBuckets,
		walkFanout: defaultWalkFanout,
		cipher:     cipher,
		now:        func() time.Time { return time.Now().UTC() },
	}
	for _, o := range opts {
		o(r)
	}
	return r
}
