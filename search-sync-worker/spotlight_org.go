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

// spotlightOrgUpsertScriptID is the stored-script id for the last-write-wins guard.
// Bump the suffix if the source ever changes incompatibly, so a rolling deploy doesn't
// leave old and new pods sharing one mutated definition.
const spotlightOrgUpsertScriptID = "search-sync-spotlight-org-upsert-v1"

// spotlightOrgUpsertScript applies one HR org row under a last-write-wins guard keyed on
// params.seq — the source message's JetStream stream sequence, a strict total order
// assigned server-side at publish time.
//
// The HR employees.upsert payload is a bare array carrying no timestamp of its own, so
// the stream sequence is the only ordering token available. Without it the write is a
// plain doc_as_upsert with no ordering semantics: a redelivered batch, or a batch from a
// second pod sharing the durable consumer, can land after a newer one and leave the older
// row stored.
//
// Merging params.doc key-by-key reproduces doc-merge semantics — the row is marshalled
// with omitempty, so fields absent from a partial publish keep their stored values.
//
// Runs under scripted_upsert so a brand-new sectId also records its sequence;
// ctx._source starts as the empty upsert doc and stored reads 0.
//
// Operational note: the guard compares sequences from ONE stream. If the HR stream is
// ever recreated, sequences restart at 1 and will lose to the stored values, silently
// dropping every subsequent update — clear orgSeq (or reindex) as part of any rebuild.
const spotlightOrgUpsertScript = `long stored = ctx._source.orgSeq == null ? 0L : ((Number)ctx._source.orgSeq).longValue(); ` +
	`long incoming = ((Number)params.seq).longValue(); ` +
	`if (incoming > stored) { ` +
	`for (entry in params.doc.entrySet()) { ctx._source[entry.getKey()] = entry.getValue(); } ` +
	`ctx._source.orgSeq = incoming; ` +
	`} else { ctx.op = 'none'; }`

// spotlightOrgCollection maintains the spotlight-org ES index, one
// document per sectId. Many employees collapse to one document via
// dedup in BuildAction.
//
// hr-syncer publishes into HR-{centralSiteID} at one central site;
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

// StoredScripts registers the last-write-wins guard; BuildAction references it by id
// so the source isn't repeated in every fan-out action.
func (c *spotlightOrgCollection) StoredScripts() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		spotlightOrgUpsertScriptID: searchindex.StoredScriptBody(spotlightOrgUpsertScript),
	}
}

// MappingUpdate returns no update — spotlight-org writes to one fixed index.
func (c *spotlightOrgCollection) MappingUpdate() (string, json.RawMessage) {
	return "", nil
}

// BuildAction decodes the employees.upsert bare array (already decompressed by
// the framework) straight into SpotlightOrgIndex — the shared org json tags mean
// each employee object yields its nine org fields and ignores the rest, so this
// stays self-contained (no HR-feed type dependency). It dedupes by SectID
// (last-wins) and emits one ES _update per unique sectId with doc_as_upsert:true.
// Doc-merge + omitempty means partial-field publishes preserve stored values for
// unset fields.
// BuildAction satisfies Collection but cannot be used: without a stream sequence the
// guard rejects every write — including the insert of a brand-new sectId, since the
// upsert seeds stored=0 and the guard needs incoming > stored — and ES reports each one
// as a successful no-op. Failing loudly beats a fleet of silently discarded writes;
// Handler always routes through BuildActionSeq.
func (c *spotlightOrgCollection) BuildAction([]byte) ([]searchengine.BulkAction, error) {
	return nil, fmt.Errorf("spotlight-org requires the source stream sequence: use BuildActionSeq")
}

// BuildActionSeq is BuildAction plus the source message's JetStream stream sequence,
// which the last-write-wins guard compares against the stored one.
func (c *spotlightOrgCollection) BuildActionSeq(data []byte, streamSeq uint64) ([]searchengine.BulkAction, error) {
	var rows []SpotlightOrgIndex
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("unmarshal hr employees: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	// Dedup by SectID, last-wins; rows without a SectID are skipped
	// (employees not yet assigned to a section).
	deduped := make(map[string]*SpotlightOrgIndex, len(rows))
	for i := range rows {
		if rows[i].SectID == "" {
			continue
		}
		deduped[rows[i].SectID] = &rows[i]
	}
	if len(deduped) == 0 {
		return nil, nil
	}

	actions := make([]searchengine.BulkAction, 0, len(deduped))
	for sectID, row := range deduped {
		body, err := buildSpotlightOrgUpdateBody(row, streamSeq)
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

func buildSpotlightOrgUpdateBody(row *SpotlightOrgIndex, streamSeq uint64) (json.RawMessage, error) {
	// scripted_upsert + an empty seed: the script owns both the insert and the merge, so a
	// new sectId records its sequence instead of landing without one.
	body := spotlightOrgUpdateBody{ScriptedUpsert: true}
	body.Script.ID = spotlightOrgUpsertScriptID
	body.Script.Params.Doc = row
	body.Script.Params.Seq = streamSeq

	data, err := json.Marshal(&body)
	if err != nil {
		return nil, fmt.Errorf("marshal spotlight-org update body: %w", err)
	}
	return data, nil
}

// spotlightOrgUpdateBody is the ES _update body for one org row. Typed rather than
// nested map[string]any so marshalling stays allocation-cheap on the fan-out path.
type spotlightOrgUpdateBody struct {
	ScriptedUpsert bool     `json:"scripted_upsert"`
	Upsert         struct{} `json:"upsert"`
	Script         struct {
		ID     string `json:"id"`
		Params struct {
			Doc *SpotlightOrgIndex `json:"doc"`
			Seq uint64             `json:"seq"`
		} `json:"params"`
	} `json:"script"`
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
		"index_patterns": []string{searchindex.IndexPattern(indexName)},
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
				"properties": searchindex.EsPropertiesFromStruct[SpotlightOrgIndex](),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}

// spotlightOrgCollection needs the stream sequence for its ordering guard; the assertion
// keeps that wiring compile-checked rather than silently lost to a renamed method.
var _ sequencedCollection = (*spotlightOrgCollection)(nil)
