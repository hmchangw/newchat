package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baseValid returns a Config with all four cache knobs at non-negative values so each
// test only varies the field under test; other fields are zeroed (validate ignores them).
func baseValid() Config {
	return Config{
		PreviewLookbackRows: 10,
		SubCacheSize:        100000,
		SubCacheTTL:         2 * time.Minute,
		RoomCacheSize:       50000,
		RoomCacheTTL:        10 * time.Second,
	}
}

func TestValidate_RejectsNonPositivePreviewLookbackRows(t *testing.T) {
	for _, v := range []int{0, -1} {
		cfg := baseValid()
		cfg.PreviewLookbackRows = v
		err := validate(&cfg)
		require.Error(t, err, "PreviewLookbackRows=%d must be rejected", v)
		assert.Contains(t, err.Error(), "MESSAGE_PREVIEW_LOOKBACK_ROWS")
	}
}

func TestValidate_AcceptsDefaults(t *testing.T) {
	cfg := baseValid()
	require.NoError(t, validate(&cfg))
}

func TestValidate_AcceptsZerosAsDisable(t *testing.T) {
	cfg := baseValid()
	cfg.SubCacheSize = 0
	cfg.SubCacheTTL = 0
	cfg.RoomCacheSize = 0
	cfg.RoomCacheTTL = 0
	require.NoError(t, validate(&cfg), "zero is the documented disable value")
}

func TestValidate_RejectsNegativeSubCacheSize(t *testing.T) {
	cfg := baseValid()
	cfg.SubCacheSize = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_SUB_CACHE_SIZE")
}

func TestValidate_RejectsNegativeSubCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.SubCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_SUB_CACHE_TTL")
}

func TestValidate_RejectsNegativeRoomCacheSize(t *testing.T) {
	cfg := baseValid()
	cfg.RoomCacheSize = -1
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_ROOM_CACHE_SIZE")
}

func TestValidate_RejectsNegativeRoomCacheTTL(t *testing.T) {
	cfg := baseValid()
	cfg.RoomCacheTTL = -1 * time.Second
	err := validate(&cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HISTORY_ROOM_CACHE_TTL")
}

// The last-message preview lookback defaults to 10 rows; change the default
// deliberately, not incidentally.
func TestPreviewLookbackRows_DefaultIsTen(t *testing.T) {
	f, ok := reflect.TypeOf(Config{}).FieldByName("PreviewLookbackRows")
	require.True(t, ok, "PreviewLookbackRows field must exist")
	assert.Equal(t, "10", f.Tag.Get("envDefault"))
	assert.Equal(t, "MESSAGE_PREVIEW_LOOKBACK_ROWS", f.Tag.Get("env"))
}
