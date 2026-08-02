package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// Config is the job's environment configuration. Mongo is the global database
// holding teams_chat: the flagged-chat scan reads through a secondary-preferred
// client and the needVerify clear writes through a primary client, so they
// share one URI, DB and credential pair — only the read preference differs.
type Config struct {
	MongoURI      string `env:"MONGO_URI,required,notEmpty"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME" envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD" envDefault:""`

	// SiteURLs is the per-site inspector registry: a JSON object mapping siteId
	// to that site's teams-room-inspector base URL. A single template can't
	// express sites on different domains, so each site is listed explicitly —
	// same reasoning as portal-service's PORTAL_SITE_URLS.
	SiteURLs string `env:"TEAMS_VERIFY_SITE_URLS,required,notEmpty"`

	// BatchSize is the maximum number of chat ids sent in one inspector call.
	BatchSize int `env:"VERIFY_BATCH_SIZE" envDefault:"200"`
	// MaxWorkers bounds concurrent inspector calls across all site groups.
	MaxWorkers int `env:"MAX_WORKERS" envDefault:"8"`
}

// parseSiteURLs decodes TEAMS_VERIFY_SITE_URLS. An empty registry is rejected:
// with no destinations the job would clear nothing and report nothing, which
// looks identical to a healthy run.
func parseSiteURLs(raw string) (map[string]string, error) {
	var sites map[string]string
	if err := json.Unmarshal([]byte(raw), &sites); err != nil {
		return nil, fmt.Errorf("parse TEAMS_VERIFY_SITE_URLS: %w", err)
	}
	if len(sites) == 0 {
		return nil, fmt.Errorf("invalid config: TEAMS_VERIFY_SITE_URLS has no sites")
	}
	for site, url := range sites {
		if url == "" {
			return nil, fmt.Errorf("invalid config: TEAMS_VERIFY_SITE_URLS[%q] has an empty URL", site)
		}
	}
	return sites, nil
}

// validateConfig checks the parsed Config's numeric knobs. It isolates run()'s
// pure precondition checks so they are unit testable without wiring any real
// dependency; the site registry is validated separately by parseSiteURLs,
// whose parsed map run() needs.
//
//nolint:gocritic // hugeParam: cfg is passed by value once at startup; not a hot path
func validateConfig(cfg Config) error {
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("invalid config: VERIFY_BATCH_SIZE must be positive")
	}
	// Above the inspector's per-request cap every batch would 400, and the chats
	// would stay flagged forever — a failure that reads just like a healthy run.
	// Fail at startup instead.
	if cfg.BatchSize > model.TeamsRoomVerifyMaxChatIDs {
		return fmt.Errorf("invalid config: VERIFY_BATCH_SIZE must not exceed %d, the inspector's per-request cap",
			model.TeamsRoomVerifyMaxChatIDs)
	}
	if cfg.MaxWorkers <= 0 {
		return fmt.Errorf("invalid config: MAX_WORKERS must be positive")
	}
	return nil
}
