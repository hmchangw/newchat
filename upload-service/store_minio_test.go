package main

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// spyReadCloser records whether Close was called and returns a preset error.
type spyReadCloser struct {
	closed   bool
	closeErr error
}

func (s *spyReadCloser) Read(p []byte) (int, error) { return 0, io.EOF }
func (s *spyReadCloser) Close() error {
	s.closed = true
	return s.closeErr
}

func TestCancelReadCloser_CloseClosesInnerAndCancels(t *testing.T) {
	inner := &spyReadCloser{}
	ctx, cancel := context.WithCancel(context.Background())

	rc := &cancelReadCloser{ReadCloser: inner, cancel: cancel}
	require.NoError(t, rc.Close())

	require.True(t, inner.closed, "inner reader should be closed")
	require.ErrorIs(t, ctx.Err(), context.Canceled, "context should be cancelled after Close")
}

func TestCancelReadCloser_ClosePropagatesInnerError(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &spyReadCloser{closeErr: wantErr}
	_, cancel := context.WithCancel(context.Background())

	rc := &cancelReadCloser{ReadCloser: inner, cancel: cancel}
	require.ErrorIs(t, rc.Close(), wantErr, "Close should propagate the inner Close error")
}
