package cassrepo

import (
	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

// Repository wraps a Cassandra session with the bucket sizer and read-walk
// configuration shared by all queries against bucketed message tables.
type Repository struct {
	session    *gocql.Session
	bucket     msgbucket.Sizer
	maxBuckets int
}

// NewRepository wires a session, bucket sizer, and max-walk depth.
// maxBuckets caps how far a paginated read walks through empty buckets before
// returning a non-terminal cursor.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets int) *Repository {
	return &Repository{
		session:    session,
		bucket:     bucket,
		maxBuckets: maxBuckets,
	}
}
