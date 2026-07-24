package main

import (
	"reflect"
	"strings"
)

// esPropertiesFromStruct reflects over struct T's fields to build an
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
func esPropertiesFromStruct[T any]() map[string]any {
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
