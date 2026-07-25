package cassrepo

import (
	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// Repository wraps a Cassandra session with the bucket sizer + read-walk config
// for bucketed message tables, plus an optional at-rest Cipher for message bodies.
type Repository struct {
	session             *gocql.Session
	bucket              msgbucket.Sizer
	maxBuckets          int
	previewLookbackRows int
	cipher              atrest.Cipher // nil when ATREST_ENABLED=false
}

// NewRepository wires a session, bucket sizer, read-walk depths, and optional
// at-rest Cipher (nil disables encryption). maxBuckets caps the empty-bucket walk.
func NewRepository(session *gocql.Session, bucket msgbucket.Sizer, maxBuckets, previewLookbackRows int, cipher atrest.Cipher) *Repository {
	return &Repository{
		session:             session,
		bucket:              bucket,
		maxBuckets:          maxBuckets,
		previewLookbackRows: previewLookbackRows,
		cipher:              cipher,
	}
}
