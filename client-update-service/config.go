package main

import (
	"fmt"
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

	// UploadTokens authorizes POST /api/v1/version, as account->token. Required:
	// an unset value would leave the upload endpoint open. Neither "," nor ":"
	// may appear in a token — both are separators, and a value containing one
	// splits into an entry that validateUploadTokens rejects.
	UploadTokens map[string]string `env:"UPLOAD_TOKENS,required" envSeparator:"," envKeyValSeparator:":"`

	HTTPWriteTimeout time.Duration `env:"HTTP_WRITE_TIMEOUT" envDefault:"10m"`

	CacheMaxEntries     int           `env:"CACHE_MAX_ENTRIES" envDefault:"4"`
	CacheTTL            time.Duration `env:"CACHE_TTL" envDefault:"24h"`
	CacheMaxObjectBytes int64         `env:"CACHE_MAX_OBJECT_BYTES" envDefault:"536870912"`
}

// minUploadTokenLen rejects a token short enough to be brute-forced or to be a
// placeholder left in a deploy manifest.
const minUploadTokenLen = 16

// validateUploadTokens fails fast on a token table that would authorize nothing
// or, worse, authorize the empty string. Error text names the account only —
// never the token, which would reach the logs.
func validateUploadTokens(tokens map[string]string) error {
	if len(tokens) == 0 {
		return fmt.Errorf("UPLOAD_TOKENS must define at least one account:token pair")
	}
	for account, token := range tokens {
		if account == "" {
			return fmt.Errorf("UPLOAD_TOKENS has an entry with an empty account name")
		}
		if len(token) < minUploadTokenLen {
			return fmt.Errorf("UPLOAD_TOKENS entry %q: token must be at least %d characters", account, minUploadTokenLen)
		}
	}
	return nil
}
