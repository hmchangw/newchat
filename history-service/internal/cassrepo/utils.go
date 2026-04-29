package cassrepo

import (
	"encoding/base64"
	"fmt"

	"github.com/gocql/gocql"
)

// Cursor wraps Cassandra's PageState as a base64-encoded pagination token.
type Cursor struct {
	state []byte
}

// NewCursor decodes a base64-encoded cursor string; empty string returns the first-page cursor.
func NewCursor(encoded string) (*Cursor, error) {
	if encoded == "" {
		return &Cursor{}, nil
	}
	state, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	return &Cursor{state: state}, nil
}

// Encode returns the base64 cursor string, or empty string when there are no more pages.
func (c *Cursor) Encode() string {
	if len(c.state) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(c.state)
}

func (c *Cursor) Raw() []byte { return c.state }

type Page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"nextCursor,omitempty"`
	HasNext    bool   `json:"hasNext"`
}

type PageRequest struct {
	Cursor   *Cursor
	PageSize int
}

const (
	defaultCassPageSize = 50
	maxPageSize         = 100
)

// ParsePageRequest validates and normalises cursor+pageSize. Default 50, max 100.
func ParsePageRequest(cursorStr string, pageSize int) (PageRequest, error) {
	cursor, err := NewCursor(cursorStr)
	if err != nil {
		return PageRequest{}, fmt.Errorf("parse page request cursor: %w", err)
	}
	if pageSize <= 0 {
		pageSize = defaultCassPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return PageRequest{Cursor: cursor, PageSize: pageSize}, nil
}

type QueryBuilder struct {
	query    *gocql.Query
	cursor   *Cursor
	pageSize int
}

func NewQueryBuilder(q *gocql.Query) *QueryBuilder {
	return &QueryBuilder{query: q, pageSize: defaultCassPageSize}
}

func (b *QueryBuilder) WithCursor(cursor *Cursor) *QueryBuilder {
	b.cursor = cursor
	return b
}

func (b *QueryBuilder) WithPageSize(size int) *QueryBuilder {
	b.pageSize = size
	return b
}

// Fetch executes the query; scan is called with the page iterator. Returns the encoded next-page cursor.
func (b *QueryBuilder) Fetch(scan func(iter *gocql.Iter)) (string, error) {
	if b.query == nil {
		return "", fmt.Errorf("execute paged query: nil query")
	}
	q := b.query.PageSize(b.pageSize)
	if b.cursor != nil {
		q = q.PageState(b.cursor.Raw())
	}

	iter := q.Iter()
	scan(iter)

	nextCursor := (&Cursor{state: iter.PageState()}).Encode()
	if err := iter.Close(); err != nil {
		return "", fmt.Errorf("close cassandra iterator: %w", err)
	}
	return nextCursor, nil
}
