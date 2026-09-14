// Package drive is a client for the internal Drive file-storage API.
package drive

import (
	"encoding/json"
	"log/slog"
	"os"
)

// Config holds Drive connection settings parsed from environment variables.
type Config struct {
	URL               string `env:"URL"`
	Token             string `env:"API_TOKEN"`
	BaseURLConfigPath string `env:"BASE_URL_CONFIG_PATH" envDefault:"etc/config/baseurls.json"`
	// BaseURLMap maps a room-origin siteID to a Drive base URL. Populated by LoadBaseURLs.
	BaseURLMap map[string]string
}

// LoadBaseURLs reads and JSON-parses the file at BaseURLConfigPath into
// BaseURLMap. On a missing file or invalid JSON it logs a warning and falls
// back to an empty map so the service still starts.
func (c *Config) LoadBaseURLs() {
	// #nosec G304 -- path is operator-supplied configuration, not user input.
	// #nosec G304 -- developer-supplied path in dev tooling, not attacker-controlled
	// nosemgrep: gosec.G304-1
	data, err := os.ReadFile(c.BaseURLConfigPath)
	if err != nil {
		slog.Warn("drive: could not read base URL config; using empty map",
			"path", c.BaseURLConfigPath, "error", err)
		c.BaseURLMap = map[string]string{}
		return
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		slog.Warn("drive: invalid base URL config JSON; using empty map",
			"path", c.BaseURLConfigPath, "error", err)
		c.BaseURLMap = map[string]string{}
		return
	}
	c.BaseURLMap = m
}
