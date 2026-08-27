package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
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

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`
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
