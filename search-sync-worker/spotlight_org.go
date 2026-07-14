package main

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

// spotlightOrgCollection maintains the spotlight-org ES index, one
// document per sectId. Many employees collapse to one document via
// dedup in BuildAction.
//
// hr-syncer publishes into HR_{centralSiteID} at one central site;
// every fab site's search-sync-worker consumes from that same stream,
// so stream config and subject filter target the central siteID
// while the durable consumer name is scoped by localSiteID (so each
// fab site keeps its own cursor).
type spotlightOrgCollection struct {
	indexName       string
	localSiteID     string
	hrCentralSiteID string
	devMode         bool
}

func newSpotlightOrgCollection(indexName, localSiteID, hrCentralSiteID string, devMode bool) *spotlightOrgCollection {
	return &spotlightOrgCollection{
		indexName:       indexName,
		localSiteID:     localSiteID,
		hrCentralSiteID: hrCentralSiteID,
		devMode:         devMode,
	}
}

func (c *spotlightOrgCollection) StreamConfig(_ string) jetstream.StreamConfig {
	cfg := stream.OrgSyncStream(c.hrCentralSiteID)
	return jetstream.StreamConfig{Name: cfg.Name, Subjects: cfg.Subjects}
}

func (c *spotlightOrgCollection) ConsumerName() string {
	return "spotlight-org-sync-" + c.localSiteID
}

func (c *spotlightOrgCollection) FilterSubjects(_ string) []string {
	return []string{subject.OrgSyncEmployeesUpsert(c.hrCentralSiteID)}
}

func (c *spotlightOrgCollection) TemplateName() string {
	return fmt.Sprintf("%s_template", searchindex.StripVersionBase(c.indexName))
}

func (c *spotlightOrgCollection) TemplateBody() json.RawMessage {
	return spotlightOrgTemplateBody(c.indexName, c.devMode)
}

func (c *spotlightOrgCollection) StoredScripts() map[string]json.RawMessage {
	return nil
}

// hrSyncEmployeeBatch decodes hr-syncer's employees.upsert wire payload.
// Each employee element is a full Employee struct from hr-syncer's
// internal repo; we decode straight into SpotlightOrgIndex to pick up
// only the nine org fields and drop the rest.
type hrSyncEmployeeBatch struct {
	Timestamp int64               `json:"timestamp"`
	Employees []SpotlightOrgIndex `json:"employees"`
}

// BuildAction parses an HR employees batch, dedupes by SectID (last-wins),
// and emits one ES _update per unique sectId with doc_as_upsert:true.
// Doc-merge + omitempty on SpotlightOrgIndex means partial-field publishes
// preserve stored values for unset fields.
func (c *spotlightOrgCollection) BuildAction(data []byte) ([]searchengine.BulkAction, error) {
	var batch hrSyncEmployeeBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("unmarshal hr batch: %w", err)
	}
	if batch.Timestamp <= 0 {
		return nil, fmt.Errorf("build spotlight-org action: missing timestamp")
	}
	if len(batch.Employees) == 0 {
		return nil, nil
	}

	// Dedup by SectID, last-wins; rows without a SectID are skipped
	// (employees not yet assigned to a section).
	deduped := make(map[string]*SpotlightOrgIndex, len(batch.Employees))
	for i := range batch.Employees {
		emp := &batch.Employees[i]
		if emp.SectID == "" {
			continue
		}
		deduped[emp.SectID] = emp
	}
	if len(deduped) == 0 {
		return nil, nil
	}

	actions := make([]searchengine.BulkAction, 0, len(deduped))
	for sectID, row := range deduped {
		body, err := buildSpotlightOrgUpdateBody(row)
		if err != nil {
			return nil, err
		}
		actions = append(actions, searchengine.BulkAction{
			Action: searchengine.ActionUpdate,
			Index:  c.indexName,
			DocID:  sectID,
			Doc:    body,
		})
	}
	return actions, nil
}

func buildSpotlightOrgUpdateBody(row *SpotlightOrgIndex) (json.RawMessage, error) {
	body := map[string]any{
		"doc":           row,
		"doc_as_upsert": true,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal spotlight-org update body: %w", err)
	}
	return data, nil
}

// SpotlightOrgIndex is the wire row, ES doc body, and ES mapping source
// for spotlight-org. omitempty drives partial-update semantics: absent
// fields don't overwrite stored values via doc-merge.
type SpotlightOrgIndex struct {
	SectID          string `json:"sectId,omitempty"          es:"search_as_you_type,custom_analyzer"`
	SectTCName      string `json:"sectTCName,omitempty"      es:"search_as_you_type,custom_analyzer"`
	SectName        string `json:"sectName,omitempty"        es:"search_as_you_type,custom_analyzer"`
	SectDescription string `json:"sectDescription,omitempty" es:"search_as_you_type,custom_analyzer"`
	DeptID          string `json:"deptId,omitempty"          es:"search_as_you_type,custom_analyzer"`
	DeptTCName      string `json:"deptTCName,omitempty"      es:"search_as_you_type,custom_analyzer"`
	DeptName        string `json:"deptName,omitempty"        es:"search_as_you_type,custom_analyzer"`
	DeptDescription string `json:"deptDescription,omitempty" es:"search_as_you_type,custom_analyzer"`
	DivisionID      string `json:"divisionId,omitempty"      es:"search_as_you_type,custom_analyzer"`
}

func spotlightOrgTemplateBody(indexName string, devMode bool) json.RawMessage {
	shards := 3
	replicas := 1
	if devMode {
		shards = 1
		replicas = 0
	}
	tmpl := map[string]any{
		"index_patterns": []string{fmt.Sprintf("%s-*", searchindex.StripVersionBase(indexName))},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
				},
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"custom_analyzer": map[string]any{
							"type":      "custom",
							"tokenizer": "custom_tokenizer",
							"filter":    []string{"lowercase"},
						},
					},
					"tokenizer": map[string]any{
						"custom_tokenizer": map[string]any{
							"type":        "whitespace",
							"token_chars": []string{"letter", "digit", "punctuation", "symbol"},
						},
					},
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": esPropertiesFromStruct[SpotlightOrgIndex](),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
