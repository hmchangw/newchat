package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSemver(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want semver
		ok   bool
	}{
		{name: "valid", in: "1.2.3", want: semver{1, 2, 3}, ok: true},
		{name: "zeros", in: "0.0.0", want: semver{0, 0, 0}, ok: true},
		{name: "large", in: "10.20.30", want: semver{10, 20, 30}, ok: true},
		{name: "empty", in: ""},
		{name: "two parts", in: "1.2"},
		{name: "four parts", in: "1.2.3.4"},
		{name: "non-numeric", in: "1.2.x"},
		{name: "empty part", in: "1..3"},
		{name: "negative", in: "-1.0.0"},
		{name: "plus sign", in: "+1.0.0"},
		{name: "prerelease", in: "1.2.3-beta"},
		{name: "leading v", in: "v1.2.3"},
		{name: "leading zero major", in: "01.0.0"},
		{name: "leading zero minor", in: "1.02.3"},
		{name: "leading zero patch", in: "1.2.03"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseSemver(tt.in)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSemverGreater(t *testing.T) {
	tests := []struct {
		name    string
		a, b    string
		greater bool
	}{
		{name: "patch", a: "1.0.1", b: "1.0.0", greater: true},
		{name: "minor beats patch", a: "1.1.0", b: "1.0.9", greater: true},
		{name: "major beats minor", a: "2.0.0", b: "1.9.9", greater: true},
		{name: "equal", a: "1.2.3", b: "1.2.3", greater: false},
		{name: "lower", a: "1.0.0", b: "1.0.1", greater: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, ok := parseSemver(tt.a)
			assert.True(t, ok)
			b, ok := parseSemver(tt.b)
			assert.True(t, ok)
			assert.Equal(t, tt.greater, a.greater(b))
		})
	}
}

func TestIsHighest(t *testing.T) {
	tests := []struct {
		name     string
		v        string
		existing []string
		want     bool
	}{
		{name: "first for a path", v: "1.0.0", existing: nil, want: true},
		{name: "strictly higher", v: "1.0.1", existing: []string{"1.0.0", "0.9.9"}, want: true},
		{name: "equal is not higher", v: "1.0.0", existing: []string{"1.0.0"}, want: false},
		{name: "lower", v: "1.0.0", existing: []string{"1.0.1"}, want: false},
		{name: "non-semver existing ignored", v: "1.0.1", existing: []string{"1.0.0", "bogus", "v2"}, want: true},
		{name: "v itself invalid", v: "1.2", existing: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isHighest(tt.v, tt.existing))
		})
	}
}
