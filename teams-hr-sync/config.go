package main

import (
	"encoding/json"
	"fmt"
)

// config is teams-hr-sync's environment configuration. The binary runs
// exactly one sync per invocation — scheduling and overlap prevention are
// owned by the Kubernetes CronJob that triggers it (concurrencyPolicy:
// Forbid). Credentials and connection strings are required with no default
// (fail fast); operational knobs default to sane dev values.
type config struct {
	TeamsTenantID     string `env:"TEAMS_TENANT_ID,required,notEmpty"`
	TeamsClientID     string `env:"TEAMS_CLIENT_ID,required,notEmpty"`
	TeamsClientSecret string `env:"TEAMS_CLIENT_SECRET,required,notEmpty"`
	// GraphPageSize is Graph's $top per page (max 999).
	GraphPageSize int `env:"GRAPH_PAGE_SIZE" envDefault:"500"`
	// GraphBaseURL / GraphTokenURL override the Graph API and OAuth2 token
	// endpoints (integration tests, on-prem gateways); empty means the public
	// Microsoft Graph.
	GraphBaseURL  string `env:"GRAPH_BASE_URL" envDefault:""`
	GraphTokenURL string `env:"GRAPH_TOKEN_URL" envDefault:""`

	// SyncGroups is the JSON list of Graph groups to sync, each mapped to a
	// site: [{"groupId":"…","siteId":"…"}]. Parsed via parseSyncGroups.
	SyncGroups string `env:"SYNC_GROUPS,required,notEmpty"`
	// OrgType is the Employee.Org.Type stamped on every synced row.
	OrgType string `env:"ORG_TYPE" envDefault:"group"`
	// CentralSiteID scopes the two upsert subjects (chat.hr.{central}.…).
	CentralSiteID string `env:"CENTRAL_SITE_ID,required,notEmpty"`
	// SiteOverrides optionally pins specific accounts to a site regardless of
	// their group's default: JSON [{"account":"…","siteId":"…"}]. Parsed via
	// parseSiteOverrides.
	SiteOverrides string `env:"SITE_OVERRIDES" envDefault:""`

	MongoReadURI      string `env:"MONGO_READ_URI,required,notEmpty"`
	MongoReadUsername string `env:"MONGO_READ_USERNAME" envDefault:""`
	MongoReadPassword string `env:"MONGO_READ_PASSWORD" envDefault:""`
	MongoReadDB       string `env:"MONGO_READ_DB" envDefault:"chat"`

	NatsURL       string `env:"NATS_URL,required,notEmpty"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
}

// syncGroup maps one Graph group to the site its members belong to.
type syncGroup struct {
	GroupID string `json:"groupId"`
	SiteID  string `json:"siteId"`
}

// parseSyncGroups decodes and validates SYNC_GROUPS: non-empty, every entry
// carries both ids, and groupIds are unique (a duplicate would double-publish
// and make the site assignment ambiguous).
func parseSyncGroups(raw string) ([]syncGroup, error) {
	var groups []syncGroup
	if err := json.Unmarshal([]byte(raw), &groups); err != nil {
		return nil, fmt.Errorf("decode SYNC_GROUPS: %w", err)
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("SYNC_GROUPS must list at least one group")
	}
	seen := make(map[string]struct{}, len(groups))
	for i, g := range groups {
		if g.GroupID == "" || g.SiteID == "" {
			return nil, fmt.Errorf("SYNC_GROUPS[%d]: groupId and siteId are both required", i)
		}
		if _, dup := seen[g.GroupID]; dup {
			return nil, fmt.Errorf("SYNC_GROUPS: duplicate groupId %q", g.GroupID)
		}
		seen[g.GroupID] = struct{}{}
	}
	return groups, nil
}

// siteOverride pins one account to a site, overriding its group default.
type siteOverride struct {
	Account string `json:"account"`
	SiteID  string `json:"siteId"`
}

// parseSiteOverrides decodes SITE_OVERRIDES into an account→siteId map; a
// duplicate account errors (ambiguous).
func parseSiteOverrides(raw string) (map[string]string, error) {
	out := map[string]string{}
	if raw == "" {
		return out, nil
	}
	var overrides []siteOverride
	if err := json.Unmarshal([]byte(raw), &overrides); err != nil {
		return nil, fmt.Errorf("decode SITE_OVERRIDES: %w", err)
	}
	for i, o := range overrides {
		if o.Account == "" || o.SiteID == "" {
			return nil, fmt.Errorf("SITE_OVERRIDES[%d]: account and siteId are both required", i)
		}
		if _, dup := out[o.Account]; dup {
			return nil, fmt.Errorf("SITE_OVERRIDES: duplicate account %q", o.Account)
		}
		out[o.Account] = o.SiteID
	}
	return out, nil
}
