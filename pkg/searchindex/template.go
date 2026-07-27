package searchindex

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// EsPropertiesFromStruct reflects over struct T's fields to build an
// Elasticsearch mapping properties map from `es` struct tags. Fields are
// keyed by the `json` tag name.
//
// The `es` tag grammar is "type[,analyzer]" — e.g. `es:"keyword"` or
// `es:"text,custom_analyzer"`. Fields are skipped when:
//   - the `es` tag is missing or `-`
//   - the `json` tag is missing, empty, or `-` (fail closed: we never emit
//     a mapping entry under the empty string, which would silently corrupt
//     the template for any future struct that adds an `es`-tagged field
//     without a matching `json` tag)
func EsPropertiesFromStruct[T any]() map[string]any {
	var zero T
	t := reflect.TypeOf(zero)
	props := make(map[string]any, t.NumField())
	for i := range t.NumField() {
		field := t.Field(i)
		esTag := field.Tag.Get("es")
		if esTag == "" || esTag == "-" {
			continue
		}
		jsonTag := field.Tag.Get("json")
		name, _, _ := strings.Cut(jsonTag, ",")
		if name == "" || name == "-" {
			continue
		}

		esType, analyzer, _ := strings.Cut(esTag, ",")
		// object_disabled → stored in _source for rendering, never indexed.
		if esType == "object_disabled" {
			props[name] = map[string]any{"type": "object", "enabled": false}
			continue
		}
		prop := map[string]any{"type": esType}
		if analyzer != "" {
			prop["analyzer"] = analyzer
		}
		props[name] = prop
	}
	return props
}

// MessageTemplateName returns the ES index template name for the messages
// index at prefix, stripping any version suffix so the template survives a
// reindex rollover to a new version.
func MessageTemplateName(prefix string) string {
	return fmt.Sprintf("%s_template", StripVersionBase(prefix))
}

// MessageTemplateBody builds the ES index template for the messages index.
func MessageTemplateBody(prefix string, devMode bool) json.RawMessage {
	shards := 4
	replicas := 2
	if devMode {
		shards = 1
		replicas = 0
	}
	tmpl := map[string]any{
		"index_patterns": []string{IndexPattern(prefix)},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   shards,
					"number_of_replicas": replicas,
					"refresh_interval":   "30s",
				},
				"analysis": map[string]any{
					"analyzer": map[string]any{
						"custom_analyzer": map[string]any{
							"type":        "custom",
							"tokenizer":   "underscore_preserving",
							"filter":      []string{"underscore_subword", "cjk_bigram", "lowercase"},
							"char_filter": []string{"html_strip"},
						},
					},
					"tokenizer": map[string]any{
						"underscore_preserving": map[string]any{
							"type":    "pattern",
							"pattern": `[\s,;!?()\[\]{}"'<>]+`,
						},
					},
					"filter": map[string]any{
						"underscore_subword": map[string]any{
							"type":                 "word_delimiter_graph",
							"split_on_case_change": false,
							"split_on_numerics":    false,
							"preserve_original":    true,
						},
					},
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": EsPropertiesFromStruct[MessageDoc](),
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}

// SpotlightTemplateName returns the ES index template name for the spotlight
// index at indexName, stripping any version suffix.
func SpotlightTemplateName(indexName string) string {
	return fmt.Sprintf("%s_template", StripVersionBase(indexName))
}

// SpotlightTemplateBody builds the ES index template. The wildcard
// index_patterns lets a single template cover the current versioned
// index and any future reindex targets.
func SpotlightTemplateBody(indexName string, devMode bool) json.RawMessage {
	shards := 3
	replicas := 1
	if devMode {
		shards = 1
		replicas = 0
	}
	tmpl := map[string]any{
		"index_patterns": []string{IndexPattern(indexName)},
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
						// Whitespace tokenizer only supports max_token_length
						// (default 255). `token_chars` is valid on ngram /
						// edge_ngram tokenizers, not whitespace — sending it
						// here would reject the UpsertTemplate request.
						"custom_tokenizer": map[string]any{
							"type": "whitespace",
						},
					},
				},
			},
			"mappings": map[string]any{
				"dynamic":    false,
				"properties": EsPropertiesFromStruct[SpotlightDoc](),
			},
		},
	}
	// tmpl is built entirely from map/slice/string/int literals that are
	// always JSON-marshalable, so the error cannot occur in practice.
	data, _ := json.Marshal(tmpl)
	return data
}

// UserRoomTemplateName returns the ES index template name for the
// user-room index. Unlike messages/spotlight, user-room index names are
// unversioned (a single fixed index), so no version-stripping applies.
func UserRoomTemplateName(indexName string) string {
	return fmt.Sprintf("%s_template", indexName)
}

// UserRoomTemplateBody builds the ES index template for user-room; index_patterns is the exact
// configured index name so a custom USER_ROOM_INDEX still maps correctly, and roomTimestamps is `flattened` to avoid per-key mapping bloat.
func UserRoomTemplateBody(indexName string) json.RawMessage {
	tmpl := map[string]any{
		"index_patterns": []string{indexName},
		"template": map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_shards":   1,
					"number_of_replicas": 1,
				},
			},
			"mappings": map[string]any{
				"dynamic": false,
				"properties": map[string]any{
					"userAccount": map[string]any{"type": "keyword"},
					"rooms": map[string]any{
						"type": "text",
						"fields": map[string]any{
							"keyword": map[string]any{"type": "keyword", "ignore_above": 256},
						},
					},
					// restrictedRooms is a rid→historySharedSince map; `flattened` keeps the mapping stable regardless of rid count — same approach as roomTimestamps.
					"restrictedRooms": map[string]any{"type": "flattened"},
					"roomTimestamps":  map[string]any{"type": "flattened"},
					"createdAt":       map[string]any{"type": "date"},
					"updatedAt":       map[string]any{"type": "date"},
				},
			},
		},
	}
	data, _ := json.Marshal(tmpl)
	return data
}
