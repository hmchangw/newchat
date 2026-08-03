package main

import "testing"

func TestSectionMidpoint(t *testing.T) {
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
			if exhausted != tc.wantExhausted {
				t.Fatalf("exhausted = %v, want %v (mid=%v)", exhausted, tc.wantExhausted, mid)
			}
			if !exhausted && (mid <= tc.prev || mid >= tc.next) {
				t.Fatalf("mid %v not strictly between %v and %v", mid, tc.prev, tc.next)
			}
		})
	}
}
