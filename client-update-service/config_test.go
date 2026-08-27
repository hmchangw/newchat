package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Defaults(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("UPLOAD_TOKENS", "admin-service:0123456789abcdef")

	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, 4, cfg.CacheMaxEntries)
	assert.Equal(t, 24*time.Hour, cfg.CacheTTL)
	assert.Equal(t, int64(536870912), cfg.CacheMaxObjectBytes)
	assert.Equal(t, 5*time.Minute, cfg.MinioDownloadTimeout)
	assert.Equal(t, 10*time.Minute, cfg.HTTPWriteTimeout)
}

func TestConfig_RequiresEachRequiredVar(t *testing.T) {
	// env/v11 treats an empty string as "defined", so a missing-var test must
	// os.Unsetenv the target rather than rely on the host environment being clean.
	required := []string{"SITE_ID", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"}
	for _, missing := range required {
		t.Run("missing_"+missing, func(t *testing.T) {
			for _, k := range required {
				seed := "seed"
				if k == "UPLOAD_TOKENS" {
					seed = "seed:0123456789abcdef" // well-formed key:value for map parsing
				}
				t.Setenv(k, seed) // t.Setenv restores the original value on cleanup
			}
			require.NoError(t, os.Unsetenv(missing))
			_, err := loadConfig()
			assert.Error(t, err, "parse must fail when %s is unset", missing)
		})
	}
}

func TestConfig_ParsesUploadTokens(t *testing.T) {
	t.Setenv("SITE_ID", "site-local")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ACCESS_KEY", "k")
	t.Setenv("MINIO_SECRET_KEY", "s")
	t.Setenv("MINIO_BUCKET", "chat-updates")
	t.Setenv("UPLOAD_TOKENS", "admin-service:0123456789abcdef,ops-cli:fedcba9876543210")

	cfg, err := loadConfig()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"admin-service": "0123456789abcdef",
		"ops-cli":       "fedcba9876543210",
	}, cfg.UploadTokens)
}

func TestValidateUploadTokens(t *testing.T) {
	tests := []struct {
		name    string
		tokens  map[string]string
		wantErr bool
	}{
		{"one valid entry", map[string]string{"admin-service": "0123456789abcdef"}, false},
		{"two valid entries", map[string]string{"a": "0123456789abcdef", "b": "fedcba9876543210"}, false},
		// An empty table is valid and disables uploads: the service starts and
		// refuses every upload, rather than crash-looping a deployment that does
		// not publish client updates.
		{"empty map disables uploads", map[string]string{}, false},
		{"nil map disables uploads", nil, false},
		{"empty account name", map[string]string{"": "0123456789abcdef"}, true},
		{"empty token", map[string]string{"admin-service": ""}, true},
		{"token under 16 chars", map[string]string{"admin-service": "short"}, true},
		{"token exactly 16 chars", map[string]string{"admin-service": "0123456789abcdef"}, false},
		{"duplicate token across accounts", map[string]string{"a": "0123456789abcdef", "b": "0123456789abcdef"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUploadTokens(tt.tokens)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestValidateUploadTokens_ErrorNeverLeaksTheToken(t *testing.T) {
	const secret = "supersecrettoken0123"
	err := validateUploadTokens(map[string]string{"": secret})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret,
		"a config error must never carry the token value — it reaches the logs")
}

// A shared token makes attribution in the access log map-iteration-dependent
// (lookupAccount scans every entry and keeps the last match). The error must
// name both accounts so an operator can find the misconfiguration, but never
// the token value itself.
func TestValidateUploadTokens_DuplicateToken_NamesAccountsNotToken(t *testing.T) {
	const secret = "duplicatetoken01234"
	err := validateUploadTokens(map[string]string{"account-a": secret, "account-b": secret})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "account-a")
	assert.Contains(t, err.Error(), "account-b")
	assert.NotContains(t, err.Error(), secret,
		"a config error must never carry the token value — it reaches the logs")
}

// YAML tooling that round-trips a large unquoted integer through a float64 —
// Helm and several k8s manifest generators do — hands the process
// CACHE_MAX_OBJECT_BYTES=5.36870912e+08. strconv.ParseInt rejects that outright,
// so the service used to crash-loop on a value that is exactly 512 MiB.
func TestLoadConfig_AcceptsFloatFormattedByteSizes(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int64
		wantErr bool
	}{
		{name: "plain integer", value: "536870912", want: 536870912},
		{name: "scientific notation", value: "5.36870912e+08", want: 536870912},
		{name: "uppercase exponent", value: "5.36870912E+08", want: 536870912},
		{name: "no exponent sign", value: "1e3", want: 1000},
		{name: "trailing float zero", value: "536870912.0", want: 536870912},
		{name: "surrounding whitespace", value: " 536870912 ", want: 536870912},
		{name: "zero", value: "0", want: 0},
		// A fractional byte count is a real mistake, not a formatting artifact:
		// rounding it silently would hide the misconfiguration.
		{name: "fractional", value: "1.5", wantErr: true},
		{name: "not a number", value: "512MiB", wantErr: true},
		{name: "beyond int64", value: "1e30", wantErr: true},
		// env treats an empty variable as unset, so the envDefault still applies.
		{name: "empty falls back to the default", value: "", want: 536870912},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SITE_ID", "site-local")
			t.Setenv("MINIO_ENDPOINT", "minio:9000")
			t.Setenv("MINIO_ACCESS_KEY", "k")
			t.Setenv("MINIO_SECRET_KEY", "s")
			t.Setenv("MINIO_BUCKET", "chat-updates")
			t.Setenv("CACHE_MAX_OBJECT_BYTES", tt.value)

			cfg, err := loadConfig()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "CacheMaxObjectBytes",
					"the error must name the field an operator has to fix")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, cfg.CacheMaxObjectBytes)
		})
	}
}
