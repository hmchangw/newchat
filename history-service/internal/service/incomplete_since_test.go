package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// degradationReaderFunc adapts a func to DegradationReader.
type degradationReaderFunc func(ctx context.Context, siteID string) (*int64, error)

func (f degradationReaderFunc) DegradedSince(ctx context.Context, siteID string) (*int64, error) {
	return f(ctx, siteID)
}

func TestHistoryService_IncompleteSince(t *testing.T) {
	t.Run("nil when no degradation reader is configured", func(t *testing.T) {
		s := &HistoryService{}
		assert.Nil(t, s.incompleteSince(context.Background()))
	})

	t.Run("nil when the site is healthy", func(t *testing.T) {
		s := &HistoryService{
			degradation: degradationReaderFunc(func(context.Context, string) (*int64, error) { return nil, nil }),
			siteID:      "site-a",
		}
		assert.Nil(t, s.incompleteSince(context.Background()))
	})

	t.Run("returns the marker timestamp when degraded", func(t *testing.T) {
		since := int64(1700000000000)
		s := &HistoryService{
			degradation: degradationReaderFunc(func(context.Context, string) (*int64, error) { return &since, nil }),
			siteID:      "site-a",
		}
		got := s.incompleteSince(context.Background())
		require.NotNil(t, got)
		assert.Equal(t, since, *got)
	})

	t.Run("degrades to nil when the reader errors — a read must never fail on this", func(t *testing.T) {
		s := &HistoryService{
			degradation: degradationReaderFunc(func(context.Context, string) (*int64, error) {
				return nil, errors.New("mongo unavailable")
			}),
			siteID: "site-a",
		}
		assert.Nil(t, s.incompleteSince(context.Background()))
	})
}
