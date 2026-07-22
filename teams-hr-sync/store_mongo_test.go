package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// TestEmployeeProjection_CoversEveryBsonTag fails if any model.Employee bson
// tag (incl. the inline Org fields) is missing from the derived projection —
// so a rename can't silently drop a field from the read.
func TestEmployeeProjection_CoversEveryBsonTag(t *testing.T) {
	want := map[string]struct{}{}
	var collect func(reflect.Type)
	collect = func(tp reflect.Type) {
		for i := range tp.NumField() {
			f := tp.Field(i)
			if strings.Contains(f.Tag.Get("bson"), "inline") && f.Type.Kind() == reflect.Struct {
				collect(f.Type)
				continue
			}
			tag, _, _ := strings.Cut(f.Tag.Get("bson"), ",")
			if tag != "" && tag != "-" {
				want[tag] = struct{}{}
			}
		}
	}
	collect(reflect.TypeOf(model.Employee{}))

	for tag := range want {
		_, ok := employeeProjection[tag]
		assert.True(t, ok, "projection missing bson field %q", tag)
	}
	assert.Len(t, employeeProjection, len(want), "projection has an extra/stale field")
	// spot-check a flat org field made it in
	assert.Contains(t, employeeProjection, "sectId")
	assert.Contains(t, employeeProjection, "employeeId")
}
