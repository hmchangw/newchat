package main

import "strings"

// ConnInfo is the read-only connection panel's payload. Built once at startup
// from config with every URI redacted — no secret may ever reach the browser.
type ConnInfo struct {
	SiteID string `json:"siteId"`
	Stream string `json:"stream"`

	NATS struct {
		URL          string `json:"url"`
		CredsFileSet bool   `json:"credsFileSet"`
	} `json:"nats"`

	SourceMongo struct {
		URI            string `json:"uri"`
		DB             string `json:"db"`
		AuthSet        bool   `json:"authSet"`
		ReadPreference string `json:"readPreference"`
	} `json:"sourceMongo"`

	TargetMongo struct {
		URI     string `json:"uri"`
		DB      string `json:"db"`
		AuthSet bool   `json:"authSet"`
	} `json:"targetMongo"`

	Cassandra struct {
		InUse    bool   `json:"inUse"`
		Hosts    string `json:"hosts,omitempty"`
		Keyspace string `json:"keyspace,omitempty"`
		AuthSet  bool   `json:"authSet"`
	} `json:"cassandra"`

	MappingFile string `json:"mappingFile"`
}

// redactURI replaces any userinfo (credentials) ahead of the host with "***@".
// Only the authority part is inspected — an "@" after the first path slash is
// data, not credentials.
func redactURI(uri string) string {
	rest, ok := strings.CutPrefix(uri, schemeOf(uri))
	if !ok || rest == uri {
		return uri
	}
	authorityEnd := strings.IndexByte(rest, '/')
	authority := rest
	if authorityEnd >= 0 {
		authority = rest[:authorityEnd]
	}
	at := strings.LastIndexByte(authority, '@')
	if at < 0 {
		return uri
	}
	return schemeOf(uri) + "***@" + rest[at+1:]
}

func schemeOf(uri string) string {
	i := strings.Index(uri, "://")
	if i < 0 {
		return ""
	}
	return uri[:i+3]
}

// buildConnInfo assembles the panel payload from the resolved config.
func buildConnInfo(cfg *config, streamName string, cassandraInUse bool) ConnInfo {
	var info ConnInfo
	info.SiteID = cfg.SiteID
	info.Stream = streamName
	info.MappingFile = cfg.MappingFile

	info.NATS.URL = redactURI(cfg.NATSURL)
	info.NATS.CredsFileSet = cfg.CredsFile != ""

	info.SourceMongo.URI = redactURI(cfg.SourceMongoURI)
	info.SourceMongo.DB = cfg.SourceDB
	info.SourceMongo.AuthSet = cfg.SourceMongoUsername != "" || cfg.SourceMongoPassword != ""
	info.SourceMongo.ReadPreference = "primaryPreferred"

	info.TargetMongo.URI = redactURI(cfg.TargetMongoURI)
	info.TargetMongo.DB = cfg.TargetDB
	info.TargetMongo.AuthSet = cfg.TargetMongoUsername != "" || cfg.TargetMongoPassword != ""

	info.Cassandra.InUse = cassandraInUse
	if cassandraInUse {
		info.Cassandra.Hosts = cfg.CassandraHosts
		info.Cassandra.Keyspace = cfg.CassandraKeyspace
	}
	info.Cassandra.AuthSet = cfg.CassandraUsername != "" || cfg.CassandraPassword != ""
	return info
}
