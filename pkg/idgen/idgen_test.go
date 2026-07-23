package idgen_test

import (
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
)

func isBase62(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		default:
			return false
		}
	}
	return true
}

func TestGenerateID_LengthAndAlphabet(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := idgen.GenerateID()
		assert.Len(t, id, 17)
		assert.True(t, isBase62(id), "id %q contains non-base62 characters", id)
	}
}

func TestGenerateID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := idgen.GenerateID()
		_, dup := seen[id]
		assert.False(t, dup, "duplicate ID %q at iteration %d", id, i)
		seen[id] = struct{}{}
	}
}

func TestDeterministicID(t *testing.T) {
	a := idgen.DeterministicID([]byte("graph-1"))
	assert.Len(t, a, 17)
	assert.True(t, isBase62(a), "id %q contains non-base62 characters", a)
	assert.Equal(t, a, idgen.DeterministicID([]byte("graph-1")), "same seed → same id")
	assert.NotEqual(t, a, idgen.DeterministicID([]byte("graph-2")), "distinct seeds → distinct ids")
}

func TestGenerateMessageID_LengthAndAlphabet(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := idgen.GenerateMessageID()
		assert.Len(t, id, 20)
		assert.True(t, isBase62(id), "id %q contains non-base62 characters", id)
	}
}

func TestGenerateMessageID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := idgen.GenerateMessageID()
		_, dup := seen[id]
		assert.False(t, dup, "duplicate message ID %q at iteration %d", id, i)
		seen[id] = struct{}{}
	}
}

func TestGenerateUUIDv7_LengthAndHex(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := idgen.GenerateUUIDv7()
		assert.Len(t, id, 32, "UUIDv7 hex must be 32 chars (no hyphens)")
		_, err := hex.DecodeString(id)
		assert.NoError(t, err, "id %q must be valid lowercase hex", id)
	}
}

func TestGenerateUUIDv7_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := idgen.GenerateUUIDv7()
		_, dup := seen[id]
		assert.False(t, dup, "duplicate UUIDv7 %q at iteration %d", id, i)
		seen[id] = struct{}{}
	}
}

func TestGenerateUUIDv7_VersionAndVariantBits(t *testing.T) {
	// UUIDv7 (RFC 9562): hex index 12 must be '7' (version), index 16 must be 8/9/a/b (variant).
	id := idgen.GenerateUUIDv7()
	require.Len(t, id, 32)
	assert.Equal(t, byte('7'), id[12], "version nibble must be 7, got %q", string(id[12]))
	assert.Contains(t, "89ab", string(id[16]), "variant nibble must be 8,9,a,b — got %q", string(id[16]))
}

func TestGenerateUUIDv7_TimeOrdered(t *testing.T) {
	// First 12 hex chars (48-bit Unix-ms timestamp) should increase once the timestamp prefix advances.
	a := idgen.GenerateUUIDv7()
	deadline := time.Now().Add(50 * time.Millisecond)
	for {
		b := idgen.GenerateUUIDv7()
		if a[:12] != b[:12] {
			assert.Less(t, a[:12], b[:12], "later UUIDv7 must have a larger timestamp prefix")
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("UUIDv7 timestamp prefix did not advance before deadline")
		}
	}
}

func TestGenerateUUIDv7_ConcurrentSafe(t *testing.T) {
	const goroutines = 50
	const perGoroutine = 200
	var (
		mu   sync.Mutex
		seen = make(map[string]struct{}, goroutines*perGoroutine)
		wg   sync.WaitGroup
	)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			local := make([]string, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				local[i] = idgen.GenerateUUIDv7()
			}
			mu.Lock()
			for _, id := range local {
				_, dup := seen[id]
				assert.False(t, dup, "duplicate UUIDv7 under concurrency: %q", id)
				seen[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
}

func TestIsValidMessageID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"valid 20-char base62", "AbCdEfGhIjKlMnOpQrSt", true},
		{"valid all digits", "01234567890123456789", true},
		{"valid mixed", "0aZ1bY2cX3dW4eV5fU6g", true},
		{"empty string", "", false},
		{"too short (19)", "AbCdEfGhIjKlMnOpQrS", false},
		{"too long (21)", "AbCdEfGhIjKlMnOpQrStU", false},
		{"hyphen char", "AbCdEfGhIjKlMnOpQr-t", false},
		{"underscore char", "AbCdEfGhIjKlMnOpQr_t", false},
		{"unicode char", "AbCdEfGhIjKlMnOpQrSé", false},
		{"UUIDv4 with hyphens (36)", "550e8400-e29b-41d4-a716-446655440000", false},
		{"UUIDv7 hex no hyphens (32)", "01893f8b1c4a7000b8e2d4f6a1c3e5b7", false},
		{"17-char base62 (legacy, accepted for backward compat)", "AbCdEfGhIjKlMnOpQ", true},
		{"too short (16)", "AbCdEfGhIjKlMnOp", false},
		{"between legacy and current (18)", "AbCdEfGhIjKlMnOpQR", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, idgen.IsValidMessageID(tc.in))
		})
	}
}

func TestIsValidMessageID_AcceptsGenerateMessageIDOutput(t *testing.T) {
	for i := 0; i < 50; i++ {
		assert.True(t, idgen.IsValidMessageID(idgen.GenerateMessageID()))
	}
}

func TestBuildDMRoomID_DeterministicRegardlessOfOrder(t *testing.T) {
	a := idgen.BuildDMRoomID("u-alice", "u-bob")
	b := idgen.BuildDMRoomID("u-bob", "u-alice")
	assert.Equal(t, a, b, "DM room ID must be the same regardless of caller argument order")
}

func TestBuildDMRoomID_SortedConcat(t *testing.T) {
	// Lexicographically smaller user ID comes first; no separator.
	id := idgen.BuildDMRoomID("u-bob", "u-alice")
	assert.Equal(t, "u-aliceu-bob", id)
}

func TestBuildDMRoomID_DifferentPairsDifferentIDs(t *testing.T) {
	ab := idgen.BuildDMRoomID("u-alice", "u-bob")
	ac := idgen.BuildDMRoomID("u-alice", "u-carol")
	assert.NotEqual(t, ab, ac)
}

func TestBuildDMRoomID_SelfDM(t *testing.T) {
	// Self-DMs are allowed at the idgen level; caller policy decides whether to permit them.
	id := idgen.BuildDMRoomID("u-alice", "u-alice")
	assert.Equal(t, "u-aliceu-alice", id)
}

func TestMessageIDFromRequestID_DeterministicForSameReqIDAndSuffix(t *testing.T) {
	a := idgen.MessageIDFromRequestID("req-abc", "rmindiv")
	b := idgen.MessageIDFromRequestID("req-abc", "rmindiv")
	assert.Equal(t, a, b)
	assert.Len(t, a, 20)
	assert.True(t, isBase62(a))
}

func TestMessageIDFromRequestID_DifferentSuffixesYieldDifferentIDs(t *testing.T) {
	a := idgen.MessageIDFromRequestID("req-abc", "rmindiv")
	b := idgen.MessageIDFromRequestID("req-abc", "rmorg")
	assert.NotEqual(t, a, b)
}

func TestMessageIDFromRequestID_DifferentReqIDsYieldDifferentIDs(t *testing.T) {
	a := idgen.MessageIDFromRequestID("req-abc", "rmindiv")
	b := idgen.MessageIDFromRequestID("req-def", "rmindiv")
	assert.NotEqual(t, a, b)
}

func TestMessageIDFromRequestID_OutputPassesValidator(t *testing.T) {
	id := idgen.MessageIDFromRequestID("req-abc", "addmembers")
	assert.True(t, idgen.IsValidMessageID(id))
}

func TestIsValidUUIDv7(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"valid UUIDv7 (matches GenerateUUIDv7 output)", idgen.GenerateUUIDv7(), true},
		{"empty", "", false},
		{"too short (31)", "01893f8b1c4a7000b8e2d4f6a1c3e5b", false},
		{"too long (33)", "01893f8b1c4a7000b8e2d4f6a1c3e5b77", false},
		{"uppercase hex", "01893F8B1C4A7000B8E2D4F6A1C3E5B7", false},
		{"contains hyphen", "01893f8b-1c4a-7000-b8e2-d4f6a1c3e5b7", false},
		{"non-hex char", "01893f8b1c4a7000b8e2d4f6a1c3e5bz", false},
		{"wrong version nibble (4)", "01893f8b1c4a4000b8e2d4f6a1c3e5b7", false},
		{"wrong variant nibble (c)", "01893f8b1c4a7000c8e2d4f6a1c3e5b7", false},
		{"variant 8 valid", "01893f8b1c4a70008abcdef012345678", true},
		{"variant 9 valid", "01893f8b1c4a70009abcdef012345678", true},
		{"variant a valid", "01893f8b1c4a7000abcdef0123456789", true},
		{"variant b valid", "01893f8b1c4a7000babcdef012345678", true},
		{"20-char base62 message ID", "AbCdEfGhIjKlMnOpQrSt", false},
		{"17-char base62 (legacy)", "AbCdEfGhIjKlMnOpQ", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, idgen.IsValidUUIDv7(tc.in))
		})
	}
}

func TestIsValidUUIDv7_AcceptsGenerateUUIDv7Output(t *testing.T) {
	for i := 0; i < 50; i++ {
		assert.True(t, idgen.IsValidUUIDv7(idgen.GenerateUUIDv7()))
	}
}

func TestGenerateRequestID_LengthAndShape(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := idgen.GenerateRequestID()
		assert.Len(t, id, 36, "request ID must be hyphenated UUID (36 chars)")
		// Hyphens at canonical UUID positions.
		assert.Equal(t, byte('-'), id[8], "hyphen at position 8")
		assert.Equal(t, byte('-'), id[13], "hyphen at position 13")
		assert.Equal(t, byte('-'), id[18], "hyphen at position 18")
		assert.Equal(t, byte('-'), id[23], "hyphen at position 23")
		// Version nibble at hex position 14 (after the second hyphen) is '7' for v7.
		assert.Equal(t, byte('7'), id[14], "GenerateRequestID must mint UUIDv7")
		assert.True(t, idgen.IsValidUUID(id), "GenerateRequestID output must satisfy IsValidUUID")
	}
}

func TestGenerateRequestID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := idgen.GenerateRequestID()
		_, dup := seen[id]
		assert.False(t, dup, "duplicate request ID %q at iteration %d", id, i)
		seen[id] = struct{}{}
	}
}

func TestIsValidUUID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"valid v4 lowercase", "550e8400-e29b-41d4-a716-446655440000", true},
		{"valid v7 lowercase", "01970a4f-8c2d-7c9a-abcd-e0123456789f", true},
		{"valid v7 uppercase", "01970A4F-8C2D-7C9A-ABCD-E0123456789F", true},
		{"valid v7 mixed case", "01970A4f-8C2d-7C9a-aBcD-e0123456789F", true},
		{"empty", "", false},
		{"missing all hyphens (32)", "01970a4f8c2d7c9aabcde0123456789f", false},
		{"too short (35)", "01970a4f-8c2d-7c9a-abcd-e0123456789", false},
		{"too long (37)", "01970a4f-8c2d-7c9a-abcd-e0123456789ff", false},
		{"non-hex char g", "01970a4g-8c2d-7c9a-abcd-e0123456789f", false},
		{"non-hex char z", "01970a4f-8c2d-7c9a-abcd-e012345678zf", false},
		{"hyphen in wrong place", "01970a4-f8c2d-7c9a-abcd-e0123456789f", false},
		{"underscore instead of hyphen", "01970a4f_8c2d_7c9a_abcd_e0123456789f", false},
		{"20-char base62 message ID", "AbCdEfGhIjKlMnOpQrSt", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, idgen.IsValidUUID(tc.in))
		})
	}
}

func TestIsValidUUID_AcceptsGenerateRequestIDOutput(t *testing.T) {
	for i := 0; i < 50; i++ {
		assert.True(t, idgen.IsValidUUID(idgen.GenerateRequestID()))
	}
}

func TestResolveRequestID(t *testing.T) {
	cases := []struct {
		name         string
		inbound      string
		wantID       string // "" means "any minted UUID, just not the inbound"
		wantReplaced bool
	}{
		{
			name:         "valid_uuid_passes_through",
			inbound:      "01970a4f-8c2d-7c9a-abcd-e0123456789f",
			wantID:       "01970a4f-8c2d-7c9a-abcd-e0123456789f",
			wantReplaced: false,
		},
		{
			name:         "empty_mints_fresh_not_replaced",
			inbound:      "",
			wantID:       "",
			wantReplaced: false, // empty inbound is "missing", not "replaced"
		},
		{
			name:         "malformed_mints_fresh_and_reports_replaced",
			inbound:      "not-a-uuid",
			wantID:       "",
			wantReplaced: true,
		},
		{
			name:         "wrong_length_mints_fresh_and_reports_replaced",
			inbound:      "01970a4f-8c2d-7c9a-abcd",
			wantID:       "",
			wantReplaced: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, replaced := idgen.ResolveRequestID(tc.inbound)
			assert.Equal(t, tc.wantReplaced, replaced)
			if tc.wantID != "" {
				assert.Equal(t, tc.wantID, id)
			} else {
				assert.True(t, idgen.IsValidUUID(id), "minted id must be a valid UUID, got %q", id)
				assert.NotEqual(t, tc.inbound, id)
			}
		})
	}
}
