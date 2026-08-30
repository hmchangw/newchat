package main

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

type config struct {
	Port   string `env:"PORT" envDefault:"8080"`
	SiteID string `env:"SITE_ID,required"`

	MinioEndpoint        string        `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey       string        `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey       string        `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL          bool          `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket          string        `env:"MINIO_BUCKET,required"`
	MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`

	// UploadTokens authorizes POST /api/v1/version, as account->token. Optional:
	// unset or empty authorizes NOBODY, so the service starts and rejects every
	// upload rather than crash-looping a site that never publishes client
	// updates. It never leaves the endpoint open — requireServiceAccount matches
	// against this table, and an empty table matches nothing. Downloads are
	// unaffected; they are deliberately unauthenticated either way.
	//
	// A token must not contain ",", the pair separator: caarlos0/env splits the
	// whole env var on "," first, so a comma inside a token breaks the entry and
	// fails at parse time, before validateUploadTokens ever runs. A ":" inside a
	// token is fine — caarlos0/env v11.4.0 splits each entry on the first ":"
	// only (strings.SplitN(part, sep, 2)), so the rest survives intact.
	UploadTokens map[string]string `env:"UPLOAD_TOKENS" envSeparator:"," envKeyValSeparator:":"`

	HTTPWriteTimeout time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"10m"`

	// UploadMaxBytes caps one upload's request body. A guard rail on this pod's
	// ephemeral storage, not the artifact-size policy: c.FormFile spools
	// everything past MaxMultipartMemory to a temp file before HandleUpload's own
	// checks run, so without it one service account could fill the disk. The
	// default sits far above any real artifact.
	UploadMaxBytes int64 `env:"UPLOAD_MAX_BYTES" envDefault:"2147483648"`

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`
}

// loadConfig parses the environment into config.
//
// int64 fields go through parseByteSize rather than env's own strconv.ParseInt,
// so a byte count that arrives in float form still starts the service. Only
// CacheMaxObjectBytes is an int64 here; time.Duration is a distinct type and
// keeps env's duration parser.
func loadConfig() (config, error) {
	return env.ParseAsWithOptions[config](env.Options{
		FuncMap: map[reflect.Type]env.ParserFunc{
			reflect.TypeOf(int64(0)): func(v string) (interface{}, error) {
				return parseByteSize(v)
			},
		},
	})
}

// maxMultipartMemory caps how much of an upload gin keeps on the heap before
// spilling the rest to a temp file. Gin's default is 32 MiB, which this service
// would retain per concurrent upload for as long as the MinIO Put runs. The
// parts are streamed to MinIO from their *multipart.FileHeader either way, so
// the lower bound costs nothing but temp-file I/O on a large artifact.
const maxMultipartMemory = 1 << 20

// maxInt64AsFloat is 2^63 — one past math.MaxInt64, which float64 cannot hold
// exactly. A float at or above it does not fit in an int64.
const maxInt64AsFloat = float64(1 << 63)

// parseByteSize reads a byte count, accepting the float spelling that YAML
// tooling produces for a large unquoted integer (Helm and friends round-trip
// 536870912 through a float64 and emit "5.36870912e+08"). The value must still
// be a whole number of bytes: a genuinely fractional one is a misconfiguration,
// and rounding it would hide that.
func parseByteSize(v string) (int64, error) {
	s := strings.TrimSpace(v)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse byte count %q: %w", s, err)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
		return 0, fmt.Errorf("%q is not a whole number of bytes", s)
	}
	if f >= maxInt64AsFloat || f < -maxInt64AsFloat {
		return 0, fmt.Errorf("%q does not fit in a byte count", s)
	}
	return int64(f), nil
}

// minUploadTokenLen rejects a token short enough to be brute-forced or to be a
// placeholder left in a deploy manifest.
const minUploadTokenLen = 16

// validateUploadTokens fails fast on a table that would authorize the empty
// string, or that leaves two accounts sharing one token (lookupAccount has no
// early break, so which account gets attributed in the access log for a shared
// token is map-iteration-dependent). An EMPTY table is not an error — it means
// uploads are disabled. Error text names the account(s) only — never the token,
// which would reach the logs.
func validateUploadTokens(tokens map[string]string) error {
	// An empty table is a valid configuration meaning "uploads disabled", not an
	// error. main.go warns about it at startup so the state is visible.
	accountsByToken := make(map[string][]string, len(tokens))
	for account, token := range tokens {
		if account == "" {
			return fmt.Errorf("UPLOAD_TOKENS has an entry with an empty account name")
		}
		if len(token) < minUploadTokenLen {
			return fmt.Errorf("UPLOAD_TOKENS entry %q: token must be at least %d characters", account, minUploadTokenLen)
		}
		accountsByToken[token] = append(accountsByToken[token], account)
	}
	for _, accounts := range accountsByToken {
		if len(accounts) > 1 {
			sort.Strings(accounts)
			return fmt.Errorf("UPLOAD_TOKENS accounts %s share the same token", strings.Join(accounts, ", "))
		}
	}
	return nil
}
