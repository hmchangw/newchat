package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gocql/gocql"
)

type cassStore struct {
	session *gocql.Session
}

func newCassStore(session *gocql.Session) *cassStore { return &cassStore{session: session} }

var cqlIdent = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// buildSelect assembles a point-select. Identifiers come from the validated
// mapping file, but are re-checked here; values are always bound parameters.
func buildSelect(table string, key map[string]any, cols []string) (string, []any, error) {
	if !cqlIdent.MatchString(table) {
		return "", nil, fmt.Errorf("invalid table identifier %q", table)
	}
	for _, c := range cols {
		if !cqlIdent.MatchString(c) {
			return "", nil, fmt.Errorf("invalid column identifier %q", c)
		}
	}
	var conds []string
	var args []any
	for _, k := range sortedKeys(key) {
		if !cqlIdent.MatchString(k) {
			return "", nil, fmt.Errorf("invalid key column identifier %q", k)
		}
		conds = append(conds, k+" = ?")
		args = append(args, key[k])
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s", //nolint:gocritic
		strings.Join(cols, ", "), table, strings.Join(conds, " AND "))
	return q, args, nil
}

func (s *cassStore) SelectOne(ctx context.Context, table string, key map[string]any, cols []string) (map[string]any, error) {
	q, args, err := buildSelect(table, key, cols)
	if err != nil {
		return nil, fmt.Errorf("build select: %w", err)
	}
	// #nosec G201 -- identifiers validated against ^[a-zA-Z0-9_]+$ in buildSelect; values are bound parameters
	iter := s.session.Query(q, args...).WithContext(ctx).Iter()
	var rows []map[string]any
	for {
		row := map[string]any{}
		if !iter.MapScan(row) {
			break
		}
		rows = append(rows, row)
		if len(rows) > 1 {
			break
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select from %s: %w", table, err)
	}
	switch len(rows) {
	case 0:
		return nil, errNotFound
	case 1:
		return rows[0], nil
	default:
		return nil, errAmbiguous
	}
}

var _ CassStore = (*cassStore)(nil)
