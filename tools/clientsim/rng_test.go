package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecureIntN(t *testing.T) {
	cases := []struct {
		name string
		n    int
	}{
		{"zero is not a valid bound", 0},
		{"negative is not a valid bound", -5},
		{"single value", 1},
		{"typical range", 100},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 50; i++ {
				got := secureIntN(tt.n)
				assert.GreaterOrEqual(t, got, 0)
				if tt.n > 0 {
					assert.Less(t, got, tt.n)
					continue
				}
				assert.Equal(t, 0, got, "a non-positive bound yields the zero value, never a panic")
			}
		})
	}
}

func TestSecureFloat64_StaysInUnitInterval(t *testing.T) {
	for i := 0; i < 200; i++ {
		got := secureFloat64()
		assert.GreaterOrEqual(t, got, 0.0)
		assert.Less(t, got, 1.0)
	}
}
