package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// Store is the read-only persistence surface the diff needs: this producer
// never writes hr_employee — a downstream consumer persists the published
// batches.
type Store interface {
	// ListTeamsEmployees returns the persisted hr_employee rows the diff
	// compares each run against.
	ListTeamsEmployees(ctx context.Context) ([]model.Employee, error)
}
