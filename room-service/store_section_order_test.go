package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreSectionOrder_SectionMidpoint(t *testing.T) {
	cases := []struct {
		name          string
		prev, next    float64
		wantExhausted bool
	}{
		{"wide gap", 1, 2, false},
		{"integer gap", 4, 8, false},
		{"tight but ok", 1, 1.0000001, false},
		{"exhausted equal neighbors", 5, 5, true},
		{"exhausted adjacent floats", 1, 1 + 1e-17, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mid, exhausted := sectionMidpoint(tc.prev, tc.next)
			require.Equal(t, tc.wantExhausted, exhausted, "mid=%v", mid)
			if !exhausted {
				assert.Greater(t, mid, tc.prev)
				assert.Less(t, mid, tc.next)
			}
		})
	}
}
