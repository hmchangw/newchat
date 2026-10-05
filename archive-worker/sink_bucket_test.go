package main

import (
	"context"
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
)

type fakeLock struct {
	lock string
	mode *minio.RetentionMode
	err  error
}

func (f fakeLock) GetObjectLockConfig(context.Context, string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	return f.lock, f.mode, nil, nil, f.err
}

func TestCheckObjectLock(t *testing.T) {
	compliance, governance := minio.Compliance, minio.Governance
	notFound := minio.ErrorResponse{Code: "ObjectLockConfigurationNotFoundError", StatusCode: 404}
	cases := []struct {
		name    string
		lock    fakeLock
		require bool
		wantErr error
	}{
		{"compliance passes", fakeLock{lock: "Enabled", mode: &compliance}, true, nil},
		{"governance fails", fakeLock{lock: "Enabled", mode: &governance}, true, errObjectLockRequired},
		{"no lock fails", fakeLock{lock: "", mode: nil}, true, errObjectLockRequired},
		{"no lock allowed when not required", fakeLock{lock: ""}, false, nil},
		{"lock config not found is absent and fails when required", fakeLock{err: notFound}, true, errObjectLockRequired},
		{"lock config not found is absent and warns when not required", fakeLock{err: notFound}, false, nil},
		{"lookup error fails even when not required", fakeLock{err: errors.New("403")}, false, errors.New("403")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkObjectLock(context.Background(), tc.lock, "b", tc.require)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			if errors.Is(tc.wantErr, errObjectLockRequired) {
				assert.True(t, errors.Is(err, errObjectLockRequired))
			}
		})
	}
}
