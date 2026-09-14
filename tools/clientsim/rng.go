package main

import (
	cryptorand "crypto/rand"
	"math/big"
)

// The jitter and random-pick sites below are not security-sensitive (they
// only de-lockstep a fleet of fake clients), but the repo's SAST policy
// requires crypto/rand over math/rand everywhere — same call shape as
// auth-service's cryptoRandFloat. These helpers keep that in one place.

// secureFloat64 returns a uniform float in [0,1). On the practically
// impossible read error it returns the no-skew midpoint.
func secureFloat64() float64 {
	const denom = 1 << 53
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(denom))
	if err != nil {
		return 0.5
	}
	return float64(n.Int64()) / float64(denom)
}

// secureIntN returns a uniform int in [0,n); n <= 0 yields 0.
func secureIntN(n int) int {
	if n <= 0 {
		return 0
	}
	v, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}
	return int(v.Int64())
}
