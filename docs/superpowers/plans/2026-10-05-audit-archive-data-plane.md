# Audit Archive Data Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `archive-worker`, the per-site JetStream worker that copies every canonical message event, membership event, and attachment into an Object Lock bucket and an append-only Elasticsearch index, encrypted with an audit-only Vault key, plus the shared `pkg/auditarchive` format package the later `audit-service` PR will read with.

**Architecture:** Three pull consumers in one process. The events lane (MESSAGES-CANONICAL) and the members lane (INBOX) batch messages, seal each batch into one encrypted segment, PUT it once, bulk-create one immutable Elasticsearch document per event with `op_type: create`, then ack. The blobs lane (MESSAGES-CANONICAL `created` only) copies each Drive-hosted attachment into the same bucket as a chunk-encrypted object and records it in an `audit-blobs` index. The segment format, frame and blob ciphers, record hash, index documents, templates and lifecycle policy live in `pkg/auditarchive` so the reader side can be built against the same code. `audit-service` and `audit-frontend` are a separate later PR.

**Tech Stack:** Go 1.25, `nats.go/jetstream` via `pkg/natsutil`, `pkg/stream`, `pkg/jsretry`, `pkg/loopguard`, `pkg/searchengine` (Elasticsearch 8), `minio-go/v7` via `pkg/minioutil`, `pkg/atrest` Vault transit wrapper, `pkg/drive`, `pkg/testutil` containers (NATS, Elasticsearch, MinIO, Vault), `encoding/json`, `crypto/aes` + `crypto/cipher` AES-256-GCM.

**Spec:** `docs/superpowers/specs/2026-09-29-message-audit-access-design.md` (§1, §2, §4, §7 worker half, §9 worker config, §10, §11, §12 steps 1 to 4)

## Global Constraints

- Branch: `claude/bold-knuth-sitjie`. Never push elsewhere.
- TDD is mandatory: write the failing test, run it, watch it fail, then implement. Never write implementation before its test.
- Use `make` targets only: `make test SERVICE=<name>`, `make test-integration SERVICE=<name>`, `make lint`, `make fmt`, `make generate SERVICE=<name>`, `make sast`. For a `pkg/` package, `SERVICE=pkg/<name>`.
- Service layout is the repo's flat convention: `archive-worker/` at the repo root with `main.go`, `config.go`, `bootstrap.go`, `store.go` (interfaces + mockgen directive), `*_test.go`, `integration_test.go`, `deploy/{Dockerfile,docker-compose.yml,azure-pipelines.yml}`. Tests are `package main`.
- Integration tests use `//go:build integration`, containers from `pkg/testutil` only, and a `TestMain` that calls `testutil.RunTests(m)` or `testutil.RunTestsWithPrewarm`.
- Errors wrap with context: `fmt.Errorf("short description: %w", err)`. Never bare `err`. Compare with `errors.Is`.
- Logging is `log/slog` with key-value fields. Never log message bodies, DEKs, tokens, or credentials.
- Codec is `encoding/json` (archive-worker is not a hot-path worker; CLAUDE.md §6).
- Never call a bare `Nak()`; use `jsretry.Settle`, `jsretry.SettleQuiet`, or `jsretry.Nak`.
- Every consume loop is guarded by `pkg/loopguard`; `shutdown.Signals()` is armed before any loop starts and `shutdown.WaitOn` is used, with `guard.BeginShutdown()` as the first hook.
- Stream creation is gated by `BOOTSTRAP_STREAMS` and only ever creates MESSAGES-CANONICAL in dev; INBOX is owned by `inbox-worker` and is never created here.
- No new third-party dependencies. `minio-go/v7 v7.2.0` and `go-elasticsearch/v8` are already in `go.mod`.
- Coverage floor 80%, target 90% for `pkg/auditarchive` and the worker's handlers.
- Do not create a pull request unless explicitly asked.

## Review Focus

1. **A redelivered batch after a successful PUT but failed bulk** must not double-count: the segment exists twice in the bucket, the index creates conflict on the same ids, and every message is acked. Pinned in Task 14.
2. **An INBOX member event with several accounts** must produce one document per account and one frame, with ids that cannot collide across accounts. Pinned in Task 13.
3. **A canonical event whose subject is the Teams batch envelope** (`.teams.batch`, not matched by the `*` filter but present if the filter is ever widened) and any payload that fails to unmarshal must be terminated, not NAKed forever. Pinned in Task 12.
4. **A bucket whose Object Lock is in governance mode or absent** must stop the worker at startup, not silently archive into an unlocked bucket. Pinned in Task 10.
5. **A worker that cannot reach Vault at start** must exit rather than run with no DEK, and a worker that finds an existing `audit-keys` document must unwrap it rather than generate a second key. Pinned in Task 8.

---

### Task 1: `pkg/searchengine` gains `ActionCreate` and `EnsureLifecyclePolicy`

**Files:**
- Modify: `pkg/searchengine/searchengine.go:15-60` (ActionType, SearchEngine interface)
- Modify: `pkg/searchengine/adapter.go:115-150` (Bulk), append `EnsureLifecyclePolicy`
- Modify: `pkg/searchengine/classify.go:26-45` (IsBulkItemSuccess)
- Test: `pkg/searchengine/adapter_test.go`, `pkg/searchengine/classify_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `const ActionCreate ActionType = "create"`; `SearchEngine.EnsureLifecyclePolicy(ctx context.Context, name string, body json.RawMessage) (created bool, err error)`; `IsBulkItemSuccess(ActionCreate, BulkResult{Status: 409})` returns `true`.

- [ ] **Step 1: Write the failing classify test**

Append to `pkg/searchengine/classify_test.go`:

```go
func TestIsBulkItemSuccess_Create(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   bool
	}{
		{"201 created", 201, true},
		{"409 conflict is already archived", 409, true},
		{"400 is a failure", 400, false},
		{"429 is a failure", 429, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsBulkItemSuccess(ActionCreate, BulkResult{Status: tc.status})
			assert.Equal(t, tc.want, got)
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=pkg/searchengine`
Expected: FAIL, `undefined: ActionCreate`.

- [ ] **Step 3: Add the action type and classify rule**

In `pkg/searchengine/searchengine.go` add after `ActionUpdate`:

```go
	// ActionCreate is `op_type: create`: the item fails with 409 when the id
	// already exists. It never carries a version; ES rejects a version on create.
	ActionCreate ActionType = "create"
```

In `pkg/searchengine/classify.go`, in the `result.Status == 409` switch, change `case ActionIndex, ActionDelete:` to `case ActionIndex, ActionDelete, ActionCreate:`.

- [ ] **Step 4: Write the failing adapter test for the bulk create line**

Find how `adapter_test.go` captures the request body for `Bulk` (it uses a fake `Transporter` whose `Perform` records `req.Body`). Add:

```go
func TestBulk_CreateActionOmitsVersion(t *testing.T) {
	tr := &captureTransport{status: 200, body: `{"items":[{"create":{"status":201}}]}`}
	a := newHTTPAdapterForTest(tr)
	_, err := a.Bulk(context.Background(), []BulkAction{{
		Action: ActionCreate, Index: "audit-events-a-2026.10.05", DocID: "a-7", Version: 99, Doc: json.RawMessage(`{"seq":7}`),
	}})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(tr.lastBody), "\n")
	require.Len(t, lines, 2)
	assert.JSONEq(t, `{"create":{"_index":"audit-events-a-2026.10.05","_id":"a-7"}}`, lines[0])
	assert.JSONEq(t, `{"seq":7}`, lines[1])
}
```

If `adapter_test.go` names its fake transport differently, use its name; the assertion is what matters: no `version` and no `version_type` keys on the create line.

- [ ] **Step 5: Run it to verify it fails**

Run: `make test SERVICE=pkg/searchengine`
Expected: FAIL, the switch in `Bulk` has no `ActionCreate` case so the buffer is empty or the meta carries `version_type`.

- [ ] **Step 6: Implement the create line and the lifecycle method**

In `pkg/searchengine/adapter.go`, inside the `switch action.Action` in `Bulk`, add:

```go
		case ActionCreate:
			line, _ := json.Marshal(map[string]createActionMeta{"create": {Index: action.Index, ID: action.DocID}})
			buf.Write(line)
			buf.WriteByte('\n')
			buf.Write(action.Doc)
			buf.WriteByte('\n')
```

and next to `bulkActionMeta` define:

```go
// createActionMeta is the op_type=create header: no version fields, because
// Elasticsearch rejects external versioning on create.
type createActionMeta struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}
```

Append to `adapter.go`:

```go
// EnsureLifecyclePolicy creates the ILM policy only when it does not exist, so
// an operator's later edits to the policy are never overwritten by a restart.
// Returns created=true when this call wrote it.
func (a *httpAdapter) EnsureLifecyclePolicy(ctx context.Context, name string, body json.RawMessage) (bool, error) {
	path := "/_ilm/policy/" + name
	getResp, err := a.do(ctx, "ilm.get_lifecycle", "", func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	})
	if err != nil {
		return false, fmt.Errorf("get lifecycle policy: %w", err)
	}
	_, _ = io.Copy(io.Discard, getResp.Body)
	getResp.Body.Close()
	switch {
	case getResp.StatusCode == http.StatusOK:
		return false, nil
	case getResp.StatusCode != http.StatusNotFound:
		return false, fmt.Errorf("get lifecycle policy: status %d", getResp.StatusCode)
	}
	putResp, err := a.do(ctx, "ilm.put_lifecycle", "", func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return false, fmt.Errorf("put lifecycle policy: %w", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(putResp.Body)
		return false, fmt.Errorf("put lifecycle policy: status %d, body: %s", putResp.StatusCode, respBody)
	}
	return true, nil
}
```

Add to the `SearchEngine` interface in `searchengine.go`:

```go
	// EnsureLifecyclePolicy creates the ILM policy if absent; never overwrites.
	EnsureLifecyclePolicy(ctx context.Context, name string, body json.RawMessage) (created bool, err error)
```

- [ ] **Step 7: Write the lifecycle adapter test**

```go
func TestEnsureLifecyclePolicy(t *testing.T) {
	t.Run("absent policy is created", func(t *testing.T) {
		tr := &scriptedTransport{responses: []scripted{{404, `{}`}, {200, `{"acknowledged":true}`}}}
		a := newHTTPAdapterForTest(tr)
		created, err := a.EnsureLifecyclePolicy(context.Background(), "audit-archive", json.RawMessage(`{"policy":{}}`))
		require.NoError(t, err)
		assert.True(t, created)
		assert.Equal(t, http.MethodPut, tr.requests[1].Method)
	})
	t.Run("existing policy is left alone", func(t *testing.T) {
		tr := &scriptedTransport{responses: []scripted{{200, `{"audit-archive":{}}`}}}
		a := newHTTPAdapterForTest(tr)
		created, err := a.EnsureLifecyclePolicy(context.Background(), "audit-archive", json.RawMessage(`{}`))
		require.NoError(t, err)
		assert.False(t, created)
		assert.Len(t, tr.requests, 1)
	})
}
```

If `adapter_test.go` has no scripted transport, add one in the test file: a `Transporter` whose `Perform` pops the next `(status, body)` and appends the request to `requests`.

- [ ] **Step 8: Run the package tests and lint**

Run: `make test SERVICE=pkg/searchengine && make lint`
Expected: PASS. `search-sync-worker` and `data-migration/es-index-migrator` still compile because they consume narrower interfaces or the full engine value; run `make test SERVICE=search-sync-worker` to confirm.

- [ ] **Step 9: Commit**

```bash
git add pkg/searchengine
git commit -m "feat(searchengine): add create bulk action and ensure-only ILM policy"
```

---

### Task 2: `pkg/auditarchive` record and hash

**Files:**
- Create: `pkg/auditarchive/doc.go`, `pkg/auditarchive/record.go`
- Test: `pkg/auditarchive/record_test.go`

**Interfaces:**
- Produces:

```go
package auditarchive

type Record struct {
	Site    string          `json:"site"`
	Stream  string          `json:"stream"`
	Seq     uint64          `json:"seq"`
	Subject string          `json:"subject"`
	EventAt int64           `json:"eventAt"` // epoch millis from the event's Timestamp
	Payload json.RawMessage `json:"payload"` // the canonical event bytes, verbatim
}
func (r Record) Marshal() ([]byte, error)         // canonical bytes: json.Marshal(r); RawMessage is compacted by encoding/json
func (r Record) Hash() (string, error)            // "sha256:" + hex of sha256(Marshal())
func HashBytes(b []byte) string                   // "sha256:" + hex
```

- [ ] **Step 1: Write the failing test**

`pkg/auditarchive/record_test.go`:

```go
package auditarchive

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord_Hash(t *testing.T) {
	r := Record{Site: "site-a", Stream: "MESSAGES-CANONICAL-site-a", Seq: 42, Subject: "chat.msg.canonical.site-a.created", EventAt: 1700000000000, Payload: json.RawMessage(`{ "event": "created" , "x":1}`)}
	h1, err := r.Hash()
	require.NoError(t, err)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, h1)

	t.Run("whitespace in payload does not change the hash", func(t *testing.T) {
		r2 := r
		r2.Payload = json.RawMessage(`{"event":"created","x":1}`)
		h2, err := r2.Hash()
		require.NoError(t, err)
		assert.Equal(t, h1, h2)
	})
	t.Run("a different seq changes the hash", func(t *testing.T) {
		r3 := r
		r3.Seq = 43
		h3, err := r3.Hash()
		require.NoError(t, err)
		assert.NotEqual(t, h1, h3)
	})
	t.Run("invalid payload is an error", func(t *testing.T) {
		r4 := r
		r4.Payload = json.RawMessage(`{not json`)
		_, err := r4.Hash()
		assert.Error(t, err)
	})
	t.Run("HashBytes matches Hash", func(t *testing.T) {
		b, err := r.Marshal()
		require.NoError(t, err)
		assert.Equal(t, h1, HashBytes(b))
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=pkg/auditarchive`
Expected: FAIL to build, `undefined: Record`.

- [ ] **Step 3: Implement**

`pkg/auditarchive/doc.go`:

```go
// Package auditarchive is the on-the-wire and at-rest format shared by
// archive-worker (writer) and audit-service (reader): the archive record and
// its hash, the frame and blob ciphers, the sealed segment layout and object
// keys, and the Elasticsearch documents, templates and lifecycle policy.
//
// Nothing here talks to NATS, Vault, a bucket or a cluster. It is pure data
// so both sides can be tested against the same bytes.
package auditarchive
```

`pkg/auditarchive/record.go`:

```go
package auditarchive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Record is one archived event: the canonical payload plus where it came from.
// Marshal is the canonical form that is hashed, encrypted into a segment
// frame, and verified later; encoding/json compacts Payload, so whitespace in
// the original bytes does not affect the hash.
type Record struct {
	Site    string          `json:"site"`
	Stream  string          `json:"stream"`
	Seq     uint64          `json:"seq"`
	Subject string          `json:"subject"`
	EventAt int64           `json:"eventAt"`
	Payload json.RawMessage `json:"payload"`
}

// Marshal returns the canonical JSON of the record.
func (r Record) Marshal() ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal archive record: %w", err)
	}
	return b, nil
}

// Hash returns "sha256:<hex>" over Marshal().
func (r Record) Hash() (string, error) {
	b, err := r.Marshal()
	if err != nil {
		return "", err
	}
	return HashBytes(b), nil
}

// HashBytes is the hash form used everywhere in the archive: "sha256:<hex>".
func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=pkg/auditarchive`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/auditarchive
git commit -m "feat(auditarchive): add archive record and canonical hash"
```

---

### Task 3: `pkg/auditarchive` frame cipher

**Files:**
- Create: `pkg/auditarchive/cipher.go`
- Test: `pkg/auditarchive/cipher_test.go`

**Interfaces:**
- Produces:

```go
const DEKSize = 32
var ErrAuthFailed = errors.New("auditarchive: authentication failed")
type Cipher struct{ /* aead cipher.AEAD, rand io.Reader */ }
func NewCipher(dek []byte) (*Cipher, error)                 // len must be DEKSize
func (c *Cipher) Seal(plaintext, aad []byte) ([]byte, error) // nonce(12) || ciphertext+tag
func (c *Cipher) Open(sealed, aad []byte) ([]byte, error)    // ErrAuthFailed on tag or aad mismatch
func FrameAAD(site string, seq uint64) []byte                // "frame|site|seq"
func BodyAAD(site string, seq uint64) []byte                 // "body|site|seq"
func ChunkAAD(fileID string, index uint32, final bool) []byte // "chunk|fileID|index|0or1"
```

- [ ] **Step 1: Write the failing test**

`pkg/auditarchive/cipher_test.go`:

```go
package auditarchive

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDEK() []byte { return bytes.Repeat([]byte{0x4b}, DEKSize) }

func TestCipher_SealOpen(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	aad := FrameAAD("site-a", 7)
	sealed, err := c.Seal([]byte("hello"), aad)
	require.NoError(t, err)
	assert.Len(t, sealed, 12+5+16)

	got, err := c.Open(sealed, aad)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)

	t.Run("fresh nonce per call", func(t *testing.T) {
		again, err := c.Seal([]byte("hello"), aad)
		require.NoError(t, err)
		assert.NotEqual(t, sealed, again)
	})
	t.Run("wrong aad fails", func(t *testing.T) {
		_, err := c.Open(sealed, FrameAAD("site-a", 8))
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("tampered byte fails", func(t *testing.T) {
		bad := append([]byte(nil), sealed...)
		bad[len(bad)-1] ^= 1
		_, err := c.Open(bad, aad)
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("short input fails", func(t *testing.T) {
		_, err := c.Open([]byte{1, 2, 3}, aad)
		assert.Error(t, err)
	})
	t.Run("wrong key size", func(t *testing.T) {
		_, err := NewCipher([]byte("short"))
		assert.Error(t, err)
	})
}

func TestAADs(t *testing.T) {
	assert.Equal(t, "frame|site-a|7", string(FrameAAD("site-a", 7)))
	assert.Equal(t, "body|site-a|7", string(BodyAAD("site-a", 7)))
	assert.Equal(t, "chunk|f1|3|0", string(ChunkAAD("f1", 3, false)))
	assert.Equal(t, "chunk|f1|3|1", string(ChunkAAD("f1", 3, true)))
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=pkg/auditarchive`
Expected: FAIL to build, `undefined: NewCipher`.

- [ ] **Step 3: Implement**

`pkg/auditarchive/cipher.go`:

```go
package auditarchive

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// DEKSize is the AES-256 key length every archive DEK has.
const DEKSize = 32

// ErrAuthFailed means the GCM tag did not validate: wrong key, wrong AAD, or
// altered bytes. Callers use errors.Is.
var ErrAuthFailed = errors.New("auditarchive: authentication failed")

// Cipher seals and opens frames with one site's archive DEK. Safe for
// concurrent use.
type Cipher struct {
	aead cipher.AEAD
	rand io.Reader
}

// NewCipher wraps a 32-byte DEK in AES-256-GCM.
func NewCipher(dek []byte) (*Cipher, error) {
	if len(dek) != DEKSize {
		return nil, fmt.Errorf("auditarchive: DEK must be %d bytes, got %d", DEKSize, len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("auditarchive: aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("auditarchive: gcm: %w", err)
	}
	return &Cipher{aead: aead, rand: rand.Reader}, nil
}

// Seal returns nonce || ciphertext || tag with a fresh random nonce.
func (c *Cipher) Seal(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(c.rand, nonce); err != nil {
		return nil, fmt.Errorf("auditarchive: nonce: %w", err)
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+c.aead.Overhead())
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plaintext, aad), nil
}

// Open reverses Seal. A tag or AAD mismatch is ErrAuthFailed.
func (c *Cipher) Open(sealed, aad []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(sealed) < ns+c.aead.Overhead() {
		return nil, fmt.Errorf("auditarchive: sealed frame too short (%d bytes)", len(sealed))
	}
	pt, err := c.aead.Open(nil, sealed[:ns], sealed[ns:], aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAuthFailed, err)
	}
	return pt, nil
}

// FrameAAD binds a segment frame to its site and stream sequence.
func FrameAAD(site string, seq uint64) []byte {
	return []byte("frame|" + site + "|" + strconv.FormatUint(seq, 10))
}

// BodyAAD binds an index document's encBody to its site and sequence.
func BodyAAD(site string, seq uint64) []byte {
	return []byte("body|" + site + "|" + strconv.FormatUint(seq, 10))
}

// ChunkAAD binds a blob chunk to its file, position and whether it is the
// terminating chunk, so chunks cannot be reordered, dropped or truncated.
func ChunkAAD(fileID string, index uint32, final bool) []byte {
	f := "0"
	if final {
		f = "1"
	}
	return []byte("chunk|" + fileID + "|" + strconv.FormatUint(uint64(index), 10) + "|" + f)
}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=pkg/auditarchive`
Expected: PASS.

```bash
git add pkg/auditarchive
git commit -m "feat(auditarchive): add AES-GCM frame cipher with bound AADs"
```

---

### Task 4: `pkg/auditarchive` segment format and object keys

**Files:**
- Create: `pkg/auditarchive/segment.go`
- Test: `pkg/auditarchive/segment_test.go`

**Interfaces:**
- Produces:

```go
const FormatVersion uint16 = 1
var ErrBadSegment = errors.New("auditarchive: malformed segment")
type Header struct { Version uint16; Site string; Lane string; FirstSeq, LastSeq uint64; Count uint32 }
// WriteSegment writes header, length-prefixed frames, and a SHA-256 trailer.
// offsets[i] is the byte offset of frame i's length prefix within the object.
func WriteSegment(w io.Writer, h Header, frames [][]byte) (offsets []int64, trailer [32]byte, err error)
// ReadSegment parses a whole segment and verifies the trailer.
func ReadSegment(r io.Reader) (Header, [][]byte, error)
// ReadFrameAt reads one length-prefixed frame at offset (for ranged GETs).
func ReadFrameAt(ra io.ReaderAt, offset int64) ([]byte, error)
func SegmentKey(site, lane string, at time.Time, firstSeq, lastSeq uint64) string // "{site}/{yyyy}/{mm}/{dd}/{hh}/{lane}-{first}-{last}.seg"
func BlobKey(site, fileID string) string                                         // "{site}/blobs/{fileID}"
```

Layout (all integers big-endian): magic `AUDSEG` (6 bytes), `Version` u16, `len(Site)` u16 + site bytes, `len(Lane)` u16 + lane bytes, `FirstSeq` u64, `LastSeq` u64, `Count` u32; then `Count` frames each `u32 length + bytes`; then 32-byte SHA-256 over everything before it.

The spec's key had no lane segment; two lanes read two streams with independent sequence spaces, so the lane name is added to keep keys unique. Task 22 records this in the spec.

- [ ] **Step 1: Write the failing test**

`pkg/auditarchive/segment_test.go`:

```go
package auditarchive

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegment_RoundTrip(t *testing.T) {
	h := Header{Version: FormatVersion, Site: "site-a", Lane: "events", FirstSeq: 10, LastSeq: 12, Count: 3}
	frames := [][]byte{[]byte("one"), []byte(""), bytes.Repeat([]byte{0xff}, 1000)}
	var buf bytes.Buffer
	offsets, trailer, err := WriteSegment(&buf, h, frames)
	require.NoError(t, err)
	require.Len(t, offsets, 3)
	assert.NotEqual(t, [32]byte{}, trailer)

	gotH, gotFrames, err := ReadSegment(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, h, gotH)
	assert.Equal(t, frames, gotFrames)

	for i, off := range offsets {
		f, err := ReadFrameAt(bytes.NewReader(buf.Bytes()), off)
		require.NoError(t, err, "frame %d", i)
		assert.Equal(t, frames[i], f)
	}
}

func TestSegment_CountMismatchIsRejected(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", Count: 2}, [][]byte{[]byte("x")})
	assert.Error(t, err)
}

func TestSegment_CorruptTrailerIsRejected(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", FirstSeq: 1, LastSeq: 1, Count: 1}, [][]byte{[]byte("x")})
	require.NoError(t, err)
	b := buf.Bytes()
	b[len(b)-1] ^= 1
	_, _, err = ReadSegment(bytes.NewReader(b))
	assert.True(t, errors.Is(err, ErrBadSegment))
}

func TestSegment_CorruptFrameIsRejected(t *testing.T) {
	var buf bytes.Buffer
	offsets, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", FirstSeq: 1, LastSeq: 1, Count: 1}, [][]byte{[]byte("xyz")})
	require.NoError(t, err)
	b := buf.Bytes()
	b[offsets[0]+4] ^= 1 // first payload byte
	_, _, err = ReadSegment(bytes.NewReader(b))
	assert.True(t, errors.Is(err, ErrBadSegment))
}

func TestSegment_TruncatedIsRejected(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := WriteSegment(&buf, Header{Site: "s", Lane: "events", FirstSeq: 1, LastSeq: 1, Count: 1}, [][]byte{[]byte("xyz")})
	require.NoError(t, err)
	_, _, err = ReadSegment(bytes.NewReader(buf.Bytes()[:buf.Len()-10]))
	assert.True(t, errors.Is(err, ErrBadSegment))
}

func TestKeys(t *testing.T) {
	at := time.Date(2026, 10, 5, 14, 3, 0, 0, time.UTC)
	assert.Equal(t, "site-a/2026/10/05/14/events-100-250.seg", SegmentKey("site-a", "events", at, 100, 250))
	assert.Equal(t, "site-a/blobs/f1", BlobKey("site-a", "f1"))
	t.Run("non-UTC time is normalised", func(t *testing.T) {
		loc := time.FixedZone("x", 3600)
		assert.Equal(t, "site-a/2026/10/05/14/events-1-1.seg", SegmentKey("site-a", "events", at.In(loc), 1, 1))
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=pkg/auditarchive`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`pkg/auditarchive/segment.go`:

```go
package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"
)

// FormatVersion is written into every segment header; readers refuse others.
const FormatVersion uint16 = 1

// ErrBadSegment covers every structural failure: bad magic, version, count,
// truncation, or a trailer that does not match.
var ErrBadSegment = errors.New("auditarchive: malformed segment")

var segmentMagic = []byte("AUDSEG")

const maxFrameBytes = 64 << 20 // sanity bound on one frame's length prefix

// Header is the plaintext prefix of a segment. It exposes only site, lane,
// sequence range and count; which messages are inside stays in the frames.
type Header struct {
	Version  uint16
	Site     string
	Lane     string
	FirstSeq uint64
	LastSeq  uint64
	Count    uint32
}

type countingWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.h.Write(p[:n])
	c.n += int64(n)
	return n, err
}

// WriteSegment emits header, frames and trailer. h.Count must equal
// len(frames); h.Version is forced to FormatVersion.
func WriteSegment(w io.Writer, h Header, frames [][]byte) ([]int64, [32]byte, error) {
	var zero [32]byte
	if int(h.Count) != len(frames) {
		return nil, zero, fmt.Errorf("auditarchive: header count %d != %d frames", h.Count, len(frames))
	}
	h.Version = FormatVersion
	cw := &countingWriter{w: w, h: sha256.New()}
	be := binary.BigEndian
	var hdr bytes.Buffer
	hdr.Write(segmentMagic)
	_ = binary.Write(&hdr, be, h.Version)
	_ = binary.Write(&hdr, be, uint16(len(h.Site)))
	hdr.WriteString(h.Site)
	_ = binary.Write(&hdr, be, uint16(len(h.Lane)))
	hdr.WriteString(h.Lane)
	_ = binary.Write(&hdr, be, h.FirstSeq)
	_ = binary.Write(&hdr, be, h.LastSeq)
	_ = binary.Write(&hdr, be, h.Count)
	if _, err := cw.Write(hdr.Bytes()); err != nil {
		return nil, zero, fmt.Errorf("auditarchive: write header: %w", err)
	}
	offsets := make([]int64, len(frames))
	var lenBuf [4]byte
	for i, f := range frames {
		offsets[i] = cw.n
		be.PutUint32(lenBuf[:], uint32(len(f)))
		if _, err := cw.Write(lenBuf[:]); err != nil {
			return nil, zero, fmt.Errorf("auditarchive: write frame %d length: %w", i, err)
		}
		if _, err := cw.Write(f); err != nil {
			return nil, zero, fmt.Errorf("auditarchive: write frame %d: %w", i, err)
		}
	}
	var trailer [32]byte
	copy(trailer[:], cw.h.Sum(nil))
	if _, err := w.Write(trailer[:]); err != nil {
		return nil, zero, fmt.Errorf("auditarchive: write trailer: %w", err)
	}
	return offsets, trailer, nil
}

// ReadSegment parses and verifies a whole segment.
func ReadSegment(r io.Reader) (Header, [][]byte, error) {
	all, err := io.ReadAll(r)
	if err != nil {
		return Header{}, nil, fmt.Errorf("auditarchive: read segment: %w", err)
	}
	if len(all) < len(segmentMagic)+32 {
		return Header{}, nil, fmt.Errorf("%w: too short", ErrBadSegment)
	}
	body, trailer := all[:len(all)-32], all[len(all)-32:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:], trailer) {
		return Header{}, nil, fmt.Errorf("%w: trailer mismatch", ErrBadSegment)
	}
	rd := bytes.NewReader(body)
	var h Header
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(rd, magic); err != nil || !bytes.Equal(magic, segmentMagic) {
		return Header{}, nil, fmt.Errorf("%w: bad magic", ErrBadSegment)
	}
	be := binary.BigEndian
	readStr := func() (string, error) {
		var n uint16
		if err := binary.Read(rd, be, &n); err != nil {
			return "", err
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(rd, b); err != nil {
			return "", err
		}
		return string(b), nil
	}
	if err := binary.Read(rd, be, &h.Version); err != nil || h.Version != FormatVersion {
		return Header{}, nil, fmt.Errorf("%w: unsupported version", ErrBadSegment)
	}
	if h.Site, err = readStr(); err != nil {
		return Header{}, nil, fmt.Errorf("%w: site: %w", ErrBadSegment, err)
	}
	if h.Lane, err = readStr(); err != nil {
		return Header{}, nil, fmt.Errorf("%w: lane: %w", ErrBadSegment, err)
	}
	if err := binary.Read(rd, be, &h.FirstSeq); err != nil {
		return Header{}, nil, fmt.Errorf("%w: firstSeq", ErrBadSegment)
	}
	if err := binary.Read(rd, be, &h.LastSeq); err != nil {
		return Header{}, nil, fmt.Errorf("%w: lastSeq", ErrBadSegment)
	}
	if err := binary.Read(rd, be, &h.Count); err != nil {
		return Header{}, nil, fmt.Errorf("%w: count", ErrBadSegment)
	}
	frames := make([][]byte, 0, h.Count)
	for i := uint32(0); i < h.Count; i++ {
		var n uint32
		if err := binary.Read(rd, be, &n); err != nil || n > maxFrameBytes {
			return Header{}, nil, fmt.Errorf("%w: frame %d length", ErrBadSegment, i)
		}
		f := make([]byte, n)
		if _, err := io.ReadFull(rd, f); err != nil {
			return Header{}, nil, fmt.Errorf("%w: frame %d truncated", ErrBadSegment, i)
		}
		frames = append(frames, f)
	}
	if rd.Len() != 0 {
		return Header{}, nil, fmt.Errorf("%w: %d trailing bytes", ErrBadSegment, rd.Len())
	}
	return h, frames, nil
}

// ReadFrameAt reads the frame whose length prefix starts at offset. It does
// not verify the trailer; callers verify the frame itself with Cipher.Open.
func ReadFrameAt(ra io.ReaderAt, offset int64) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := ra.ReadAt(lenBuf[:], offset); err != nil {
		return nil, fmt.Errorf("auditarchive: read frame length at %d: %w", offset, err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxFrameBytes {
		return nil, fmt.Errorf("%w: frame length %d", ErrBadSegment, n)
	}
	f := make([]byte, n)
	if _, err := ra.ReadAt(f, offset+4); err != nil && !(errors.Is(err, io.EOF) && n == 0) {
		return nil, fmt.Errorf("auditarchive: read frame at %d: %w", offset, err)
	}
	return f, nil
}

// SegmentKey is the object key for a sealed batch, time-ordered by prefix.
func SegmentKey(site, lane string, at time.Time, firstSeq, lastSeq uint64) string {
	u := at.UTC()
	return fmt.Sprintf("%s/%04d/%02d/%02d/%02d/%s-%d-%d.seg", site, u.Year(), int(u.Month()), u.Day(), u.Hour(), lane, firstSeq, lastSeq)
}

// BlobKey is the object key for an archived attachment.
func BlobKey(site, fileID string) string {
	return site + "/blobs/" + fileID
}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=pkg/auditarchive && make lint`
Expected: PASS. If `gocritic` or `errorlint` complains about the double `%w`, keep it: Go 1.20+ supports multiple `%w`.

```bash
git add pkg/auditarchive
git commit -m "feat(auditarchive): add sealed segment format and object keys"
```

---

### Task 5: `pkg/auditarchive` chunked blob cipher

**Files:**
- Create: `pkg/auditarchive/blob.go`
- Test: `pkg/auditarchive/blob_test.go`

**Interfaces:**
- Produces:

```go
const DefaultChunkBytes = 4 << 20
var ErrBlobTruncated = errors.New("auditarchive: blob ended before its final chunk")
// EncryptBlob streams src through chunked AES-GCM. Returns the plaintext
// SHA-256 ("sha256:<hex>") and plaintext byte count. The output is:
// magic "AUDBLB", u16 version, u32 chunkBytes, then frames (u32 len + sealed
// chunk) with ChunkAAD(fileID, i, false), and a terminating sealed empty
// chunk with ChunkAAD(fileID, n, true).
func EncryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string, chunkBytes int) (plainSHA256 string, plainBytes int64, err error)
// DecryptBlob reverses it, verifying every chunk and the terminator.
func DecryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string) (plainSHA256 string, plainBytes int64, err error)
```

- [ ] **Step 1: Write the failing test**

`pkg/auditarchive/blob_test.go`:

```go
package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlob_RoundTrip(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	for _, size := range []int{0, 1, 1000, 4096, 4097, 3*4096 + 5} {
		plain := bytes.Repeat([]byte{0xab}, size)
		var enc bytes.Buffer
		sum, n, err := EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", 4096)
		require.NoError(t, err, "size %d", size)
		assert.Equal(t, int64(size), n)
		want := sha256.Sum256(plain)
		assert.Equal(t, "sha256:"+hex.EncodeToString(want[:]), sum)

		var dec bytes.Buffer
		sum2, n2, err := DecryptBlob(&dec, bytes.NewReader(enc.Bytes()), c, "f1")
		require.NoError(t, err, "size %d", size)
		assert.Equal(t, plain, dec.Bytes())
		assert.Equal(t, sum, sum2)
		assert.Equal(t, n, n2)
	}
}

func TestBlob_Tampering(t *testing.T) {
	c, err := NewCipher(testDEK())
	require.NoError(t, err)
	plain := bytes.Repeat([]byte{1}, 10000)
	var enc bytes.Buffer
	_, _, err = EncryptBlob(&enc, bytes.NewReader(plain), c, "f1", 4096)
	require.NoError(t, err)

	t.Run("wrong file id", func(t *testing.T) {
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(enc.Bytes()), c, "f2")
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("truncated before terminator", func(t *testing.T) {
		b := enc.Bytes()
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b[:len(b)-40]), c, "f1")
		assert.True(t, errors.Is(err, ErrBlobTruncated))
	})
	t.Run("flipped byte", func(t *testing.T) {
		b := append([]byte(nil), enc.Bytes()...)
		b[100] ^= 1
		_, _, err := DecryptBlob(&bytes.Buffer{}, bytes.NewReader(b), c, "f1")
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})
	t.Run("bad chunk size", func(t *testing.T) {
		_, _, err := EncryptBlob(&bytes.Buffer{}, bytes.NewReader(plain), c, "f1", 0)
		assert.Error(t, err)
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=pkg/auditarchive`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`pkg/auditarchive/blob.go`:

```go
package auditarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// DefaultChunkBytes is the plaintext size of one blob chunk.
const DefaultChunkBytes = 4 << 20

// ErrBlobTruncated means the stream ended before the terminating chunk.
var ErrBlobTruncated = errors.New("auditarchive: blob ended before its final chunk")

var blobMagic = []byte("AUDBLB")

// EncryptBlob encrypts src chunk by chunk so attachments of any size stream
// through bounded memory. Each chunk's AAD carries its index; the empty
// terminating chunk carries final=true, so dropping, reordering or
// truncating chunks fails on decrypt.
func EncryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string, chunkBytes int) (string, int64, error) {
	if chunkBytes <= 0 {
		return "", 0, fmt.Errorf("auditarchive: chunk size must be positive, got %d", chunkBytes)
	}
	be := binary.BigEndian
	var hdr bytes.Buffer
	hdr.Write(blobMagic)
	_ = binary.Write(&hdr, be, FormatVersion)
	_ = binary.Write(&hdr, be, uint32(chunkBytes))
	if _, err := dst.Write(hdr.Bytes()); err != nil {
		return "", 0, fmt.Errorf("auditarchive: write blob header: %w", err)
	}
	sum := sha256.New()
	buf := make([]byte, chunkBytes)
	var total int64
	var idx uint32
	writeFrame := func(sealed []byte) error {
		var l [4]byte
		be.PutUint32(l[:], uint32(len(sealed)))
		if _, err := dst.Write(l[:]); err != nil {
			return err
		}
		_, err := dst.Write(sealed)
		return err
	}
	for {
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			sum.Write(buf[:n])
			total += int64(n)
			sealed, serr := c.Seal(buf[:n], ChunkAAD(fileID, idx, false))
			if serr != nil {
				return "", 0, serr
			}
			if werr := writeFrame(sealed); werr != nil {
				return "", 0, fmt.Errorf("auditarchive: write chunk %d: %w", idx, werr)
			}
			idx++
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return "", 0, fmt.Errorf("auditarchive: read blob source: %w", err)
		}
	}
	final, err := c.Seal(nil, ChunkAAD(fileID, idx, true))
	if err != nil {
		return "", 0, err
	}
	if err := writeFrame(final); err != nil {
		return "", 0, fmt.Errorf("auditarchive: write final chunk: %w", err)
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), total, nil
}

// DecryptBlob streams the plaintext to dst, verifying every chunk.
func DecryptBlob(dst io.Writer, src io.Reader, c *Cipher, fileID string) (string, int64, error) {
	be := binary.BigEndian
	magic := make([]byte, len(blobMagic))
	if _, err := io.ReadFull(src, magic); err != nil || !bytes.Equal(magic, blobMagic) {
		return "", 0, fmt.Errorf("%w: bad blob magic", ErrBadSegment)
	}
	var version uint16
	var chunkBytes uint32
	if err := binary.Read(src, be, &version); err != nil || version != FormatVersion {
		return "", 0, fmt.Errorf("%w: unsupported blob version", ErrBadSegment)
	}
	if err := binary.Read(src, be, &chunkBytes); err != nil {
		return "", 0, fmt.Errorf("%w: blob chunk size", ErrBadSegment)
	}
	sum := sha256.New()
	var total int64
	for idx := uint32(0); ; idx++ {
		var n uint32
		if err := binary.Read(src, be, &n); err != nil {
			return "", 0, fmt.Errorf("%w: chunk %d", ErrBlobTruncated, idx)
		}
		if n > maxFrameBytes {
			return "", 0, fmt.Errorf("%w: chunk %d length %d", ErrBadSegment, idx, n)
		}
		sealed := make([]byte, n)
		if _, err := io.ReadFull(src, sealed); err != nil {
			return "", 0, fmt.Errorf("%w: chunk %d", ErrBlobTruncated, idx)
		}
		pt, err := c.Open(sealed, ChunkAAD(fileID, idx, false))
		if err != nil {
			// Not a data chunk: it must be the terminator, or it is tampered.
			if _, ferr := c.Open(sealed, ChunkAAD(fileID, idx, true)); ferr == nil {
				return "sha256:" + hex.EncodeToString(sum.Sum(nil)), total, nil
			}
			return "", 0, err
		}
		sum.Write(pt)
		total += int64(len(pt))
		if _, err := dst.Write(pt); err != nil {
			return "", 0, fmt.Errorf("auditarchive: write plaintext chunk %d: %w", idx, err)
		}
	}
}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=pkg/auditarchive && make lint`
Expected: PASS.

```bash
git add pkg/auditarchive
git commit -m "feat(auditarchive): add chunked blob encryption with bound chunk order"
```

---

### Task 6: `pkg/auditarchive` index documents, templates and lifecycle policy

**Files:**
- Create: `pkg/auditarchive/index.go`
- Test: `pkg/auditarchive/index_test.go`

**Interfaces:**
- Produces:

```go
// Document types (json + es tags, built with searchindex.EsPropertiesFromStruct)
type EventDoc struct {
	Seq             uint64    `json:"seq"                       es:"long"`
	EventType       string    `json:"eventType"                 es:"keyword"`
	EventAt         time.Time `json:"eventAt"                   es:"date"`
	MessageID       string    `json:"messageId"                 es:"keyword"`
	RoomID          string    `json:"roomId"                    es:"keyword"`
	SiteID          string    `json:"siteId"                    es:"keyword"`
	SenderAccount   string    `json:"senderAccount"             es:"keyword"`
	SenderID        string    `json:"senderId"                  es:"keyword"`
	CreatedAt       time.Time `json:"createdAt"                 es:"date"`
	ThreadParentID  string    `json:"threadParentId,omitempty"  es:"keyword"`
	AttachmentCount int       `json:"attachmentCount"           es:"integer"`
	AttachmentTypes []string  `json:"attachmentTypes,omitempty" es:"keyword"`
	ActorAccount    string    `json:"actorAccount,omitempty"    es:"keyword"`
	SegmentKey      string    `json:"segmentKey"                es:"keyword"`
	FrameOffset     int64     `json:"frameOffset"               es:"long"`
	ContentHash     string    `json:"contentHash"               es:"keyword"`
	EncBody         []byte    `json:"encBody,omitempty"         es:"binary"`
}
type MemberDoc struct {
	Seq         uint64    `json:"seq"         es:"long"`
	EventType   string    `json:"eventType"   es:"keyword"`
	EventAt     time.Time `json:"eventAt"     es:"date"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	RoomSiteID  string    `json:"roomSiteId"  es:"keyword"`
	Account     string    `json:"account,omitempty" es:"keyword"`
	RoomType    string    `json:"roomType"    es:"keyword"`
	RoomName    string    `json:"roomName"    es:"keyword"`
	SegmentKey  string    `json:"segmentKey"  es:"keyword"`
	FrameOffset int64     `json:"frameOffset" es:"long"`
	ContentHash string    `json:"contentHash" es:"keyword"`
}
type BlobDoc struct {
	FileID      string    `json:"fileId"      es:"keyword"`
	MessageID   string    `json:"messageId"   es:"keyword"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	SiteID      string    `json:"siteId"      es:"keyword"`
	FileName    string    `json:"fileName"    es:"keyword"`
	ContentType string    `json:"contentType" es:"keyword"`
	SizeBytes   int64     `json:"sizeBytes"   es:"long"`
	BlobKey     string    `json:"blobKey,omitempty"     es:"keyword"`
	PlainSHA256 string    `json:"plainSha256,omitempty" es:"keyword"`
	ChunkBytes  int       `json:"chunkBytes,omitempty"  es:"integer"`
	Skipped     string    `json:"skipped,omitempty"     es:"keyword"` // "", "size", "missing", "legacy"
	ArchivedAt  time.Time `json:"archivedAt"  es:"date"`
}
type KeyDoc struct {
	SiteID     string    `json:"siteId"     es:"keyword"`
	WrappedDek []byte    `json:"wrappedDek" es:"binary"`
	CreatedAt  time.Time `json:"createdAt"  es:"date"`
}
func (d *EventDoc) SetLocation(segmentKey string, frameOffset int64)
func (d *MemberDoc) SetLocation(segmentKey string, frameOffset int64)

// Index naming
func EventsIndex(site string, at time.Time) string   // "audit-events-{site}-2026.10.05"
func MembersIndex(site string, at time.Time) string  // "audit-members-{site}-2026.10.05"
func BlobsIndex(site string) string                  // "audit-blobs-{site}"
func KeysIndex(site string) string                   // "audit-keys-{site}"
const KeyDocID = "current"
func EventDocID(site string, seq uint64) string              // "{site}-{seq}"
func MemberDocID(site string, seq uint64, i int) string      // "{site}-{seq}-{i}"
func BlobDocID(site, fileID string) string                   // "{site}-{fileID}"

// Templates and policy
const LifecyclePolicyName = "audit-archive"
func LifecyclePolicyBody(retention string) json.RawMessage   // warm readonly at 1d, delete at retention (e.g. "2555d")
type Template struct { Name string; Body json.RawMessage }
func Templates(site string, devMode bool) []Template         // four templates; events+members carry index.lifecycle.name
```

- [ ] **Step 1: Write the failing test**

`pkg/auditarchive/index_test.go`:

```go
package auditarchive

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexNames(t *testing.T) {
	at := time.Date(2026, 10, 5, 23, 59, 0, 0, time.FixedZone("x", -3600))
	assert.Equal(t, "audit-events-site-a-2026.10.06", EventsIndex("site-a", at), "UTC day")
	assert.Equal(t, "audit-members-site-a-2026.10.06", MembersIndex("site-a", at))
	assert.Equal(t, "audit-blobs-site-a", BlobsIndex("site-a"))
	assert.Equal(t, "audit-keys-site-a", KeysIndex("site-a"))
	assert.Equal(t, "site-a-77", EventDocID("site-a", 77))
	assert.Equal(t, "site-a-77-2", MemberDocID("site-a", 77, 2))
	assert.Equal(t, "site-a-f1", BlobDocID("site-a", "f1"))
}

func TestTemplates(t *testing.T) {
	tpls := Templates("site-a", true)
	require.Len(t, tpls, 4)
	names := map[string]json.RawMessage{}
	for _, tp := range tpls {
		names[tp.Name] = tp.Body
	}
	for _, name := range []string{"audit-events-site-a", "audit-members-site-a", "audit-blobs-site-a", "audit-keys-site-a"} {
		require.Contains(t, names, name)
	}
	var ev struct {
		IndexPatterns []string `json:"index_patterns"`
		Template      struct {
			Settings map[string]any `json:"settings"`
			Mappings struct {
				Dynamic    bool                      `json:"dynamic"`
				Properties map[string]map[string]any `json:"properties"`
			} `json:"mappings"`
		} `json:"template"`
	}
	require.NoError(t, json.Unmarshal(names["audit-events-site-a"], &ev))
	assert.Equal(t, []string{"audit-events-site-a-*"}, ev.IndexPatterns)
	assert.Equal(t, LifecyclePolicyName, ev.Template.Settings["index.lifecycle.name"])
	assert.False(t, ev.Template.Mappings.Dynamic)
	assert.Equal(t, "binary", ev.Template.Mappings.Properties["encBody"]["type"])
	assert.Equal(t, "long", ev.Template.Mappings.Properties["seq"]["type"])
	assert.Equal(t, float64(0), ev.Template.Settings["number_of_replicas"], "dev mode has no replicas")

	var keys struct {
		Template struct {
			Settings map[string]any `json:"settings"`
		} `json:"template"`
	}
	require.NoError(t, json.Unmarshal(names["audit-keys-site-a"], &keys))
	assert.NotContains(t, keys.Template.Settings, "index.lifecycle.name", "keys never expire")
}

func TestLifecyclePolicyBody(t *testing.T) {
	var p struct {
		Policy struct {
			Phases map[string]struct {
				MinAge  string         `json:"min_age"`
				Actions map[string]any `json:"actions"`
			} `json:"phases"`
		} `json:"policy"`
	}
	require.NoError(t, json.Unmarshal(LifecyclePolicyBody("2555d"), &p))
	assert.Contains(t, p.Policy.Phases["warm"].Actions, "readonly")
	assert.Equal(t, "1d", p.Policy.Phases["warm"].MinAge)
	assert.Equal(t, "2555d", p.Policy.Phases["delete"].MinAge)
	assert.Contains(t, p.Policy.Phases["delete"].Actions, "delete")
}

func TestSetLocation(t *testing.T) {
	e := &EventDoc{}
	e.SetLocation("k", 12)
	assert.Equal(t, "k", e.SegmentKey)
	assert.Equal(t, int64(12), e.FrameOffset)
	m := &MemberDoc{}
	m.SetLocation("k2", 7)
	assert.Equal(t, "k2", m.SegmentKey)
	assert.Equal(t, int64(7), m.FrameOffset)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=pkg/auditarchive`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`pkg/auditarchive/index.go` (the struct definitions are exactly the ones in the Interfaces block above; the functions follow):

```go
package auditarchive

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/hmchangw/chat/pkg/searchindex"
)

// (EventDoc, MemberDoc, BlobDoc, KeyDoc structs as listed in the task's Interfaces block)

// SetLocation records where the sealed record of this event lives.
func (d *EventDoc) SetLocation(segmentKey string, frameOffset int64) {
	d.SegmentKey, d.FrameOffset = segmentKey, frameOffset
}

// SetLocation records where the sealed record of this event lives.
func (d *MemberDoc) SetLocation(segmentKey string, frameOffset int64) {
	d.SegmentKey, d.FrameOffset = segmentKey, frameOffset
}

func dayIndex(prefix, site string, at time.Time) string {
	return fmt.Sprintf("%s-%s-%s", prefix, site, at.UTC().Format("2006.01.02"))
}

// EventsIndex is the daily events index a message event lands in.
func EventsIndex(site string, at time.Time) string { return dayIndex("audit-events", site, at) }

// MembersIndex is the daily members index a membership event lands in.
func MembersIndex(site string, at time.Time) string { return dayIndex("audit-members", site, at) }

// BlobsIndex holds one document per archived attachment.
func BlobsIndex(site string) string { return "audit-blobs-" + site }

// KeysIndex holds the site's single wrapped-DEK document.
func KeysIndex(site string) string { return "audit-keys-" + site }

// KeyDocID is the only document id in KeysIndex.
const KeyDocID = "current"

// EventDocID is "{site}-{seq}": one immutable document per stream sequence.
func EventDocID(site string, seq uint64) string { return site + "-" + strconv.FormatUint(seq, 10) }

// MemberDocID adds the account index because one INBOX event can name
// several accounts and each gets its own document.
func MemberDocID(site string, seq uint64, i int) string {
	return site + "-" + strconv.FormatUint(seq, 10) + "-" + strconv.Itoa(i)
}

// BlobDocID is "{site}-{fileID}": one document per archived file.
func BlobDocID(site, fileID string) string { return site + "-" + fileID }

// LifecyclePolicyName is the ILM policy every daily index carries.
const LifecyclePolicyName = "audit-archive"

// LifecyclePolicyBody makes each daily index read-only a day after creation
// and deletes it at retention. Body stripping (spec §4) is not an ILM
// action; it is a scheduled reindex owned by the audit-service PR.
func LifecyclePolicyBody(retention string) json.RawMessage {
	body := map[string]any{
		"policy": map[string]any{
			"phases": map[string]any{
				"hot": map[string]any{
					"min_age": "0ms",
					"actions": map[string]any{"set_priority": map[string]any{"priority": 100}},
				},
				"warm": map[string]any{
					"min_age": "1d",
					"actions": map[string]any{
						"readonly":     map[string]any{},
						"set_priority": map[string]any{"priority": 50},
					},
				},
				"delete": map[string]any{
					"min_age": retention,
					"actions": map[string]any{"delete": map[string]any{}},
				},
			},
		},
	}
	b, _ := json.Marshal(body)
	return b
}

// Template is one composable index template to upsert.
type Template struct {
	Name string
	Body json.RawMessage
}

func templateBody(pattern string, props map[string]any, lifecycle bool, devMode bool) json.RawMessage {
	settings := map[string]any{
		"number_of_shards":   1,
		"number_of_replicas": 1,
		"refresh_interval":   "5s",
	}
	if devMode {
		settings["number_of_replicas"] = 0
	}
	if lifecycle {
		settings["index.lifecycle.name"] = LifecyclePolicyName
	}
	body := map[string]any{
		"index_patterns": []string{pattern},
		"template": map[string]any{
			"settings": settings,
			"mappings": map[string]any{"dynamic": false, "properties": props},
		},
	}
	b, _ := json.Marshal(body)
	return b
}

// Templates returns the four index templates for one site. Only the daily
// event and member indices carry the lifecycle policy.
func Templates(site string, devMode bool) []Template {
	return []Template{
		{Name: "audit-events-" + site, Body: templateBody("audit-events-"+site+"-*", searchindex.EsPropertiesFromStruct[EventDoc](), true, devMode)},
		{Name: "audit-members-" + site, Body: templateBody("audit-members-"+site+"-*", searchindex.EsPropertiesFromStruct[MemberDoc](), true, devMode)},
		{Name: "audit-blobs-" + site, Body: templateBody(BlobsIndex(site), searchindex.EsPropertiesFromStruct[BlobDoc](), false, devMode)},
		{Name: "audit-keys-" + site, Body: templateBody(KeysIndex(site), searchindex.EsPropertiesFromStruct[KeyDoc](), false, devMode)},
	}
}
```

Check that `searchindex.EsPropertiesFromStruct` emits `{"type":"integer"}` for `es:"integer"` and `{"type":"binary"}` for `es:"binary"` (it passes any unknown type string through verbatim, per `pkg/searchindex/template.go`). If the test for `number_of_replicas` fails because the value is an `int` in the map, the JSON round-trip gives `float64(0)`, which the test expects.

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=pkg/auditarchive && make lint`
Expected: PASS, coverage above 90% for the package (`go tool cover` is wrapped by `make test`; check the printed percentage).

```bash
git add pkg/auditarchive
git commit -m "feat(auditarchive): add index documents, templates and lifecycle policy"
```

---

### Task 7: `archive-worker` scaffold: config, validation, deploy files

**Files:**
- Create: `archive-worker/config.go`, `archive-worker/config_test.go`
- Create: `archive-worker/deploy/Dockerfile`, `archive-worker/deploy/docker-compose.yml`, `archive-worker/deploy/azure-pipelines.yml`, `archive-worker/README.md`
- Create: `archive-worker/main.go` as a stub that only parses config (replaced in Task 17)

**Interfaces:**
- Produces:

```go
package main

type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

type config struct {
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`
	SiteID        string `env:"SITE_ID,required"`

	SearchURL           string `env:"ARCHIVE_SEARCH_URL,required"`
	SearchBackend       string `env:"ARCHIVE_SEARCH_BACKEND"         envDefault:"elasticsearch"`
	SearchUsername      string `env:"ARCHIVE_SEARCH_USERNAME"        envDefault:""`
	SearchPassword      string `env:"ARCHIVE_SEARCH_PASSWORD"        envDefault:""`
	SearchTLSSkipVerify bool   `env:"ARCHIVE_SEARCH_TLS_SKIP_VERIFY" envDefault:"false"`
	IndexRetention      string `env:"ARCHIVE_INDEX_RETENTION"        envDefault:"2555d"`

	S3Endpoint        string `env:"ARCHIVE_S3_ENDPOINT,required"`
	S3AccessKey       string `env:"ARCHIVE_S3_ACCESS_KEY,required"`
	S3SecretKey       string `env:"ARCHIVE_S3_SECRET_KEY,required"`
	S3UseSSL          bool   `env:"ARCHIVE_S3_USE_SSL"          envDefault:"false"`
	Bucket            string `env:"ARCHIVE_BUCKET,required"`
	RequireObjectLock bool   `env:"ARCHIVE_REQUIRE_OBJECT_LOCK" envDefault:"true"`

	FillInterval  time.Duration `env:"ARCHIVE_FILL_INTERVAL"  envDefault:"10s"`
	BatchEvents   int           `env:"ARCHIVE_BATCH_EVENTS"   envDefault:"2000"`
	BatchBytes    int           `env:"ARCHIVE_BATCH_BYTES"    envDefault:"8388608"`
	PutTimeout    time.Duration `env:"ARCHIVE_PUT_TIMEOUT"    envDefault:"10s"`
	BulkTimeout   time.Duration `env:"ARCHIVE_BULK_TIMEOUT"   envDefault:"10s"`
	WriteAttempts int           `env:"ARCHIVE_WRITE_ATTEMPTS" envDefault:"2"`
	FetchBatch    int           `env:"ARCHIVE_FETCH_BATCH"    envDefault:"100"`
	Replicas      int           `env:"ARCHIVE_REPLICAS"       envDefault:"1"` // for the ack-pending check only

	BlobsEnabled bool          `env:"ARCHIVE_BLOBS_ENABLED"   envDefault:"true"`
	BlobMaxBytes int64         `env:"ARCHIVE_BLOB_MAX_BYTES"  envDefault:"104857600"`
	BlobAckWait  time.Duration `env:"ARCHIVE_BLOB_ACK_WAIT"   envDefault:"10m"`
	BlobWorkers  int           `env:"ARCHIVE_BLOB_WORKERS"    envDefault:"4"`
	Drive        drive.Config  `envPrefix:"DRIVE_"`

	Vault     atrest.VaultConfig
	Consumer  stream.ConsumerSettings `envPrefix:"CONSUMER_"`
	Bootstrap bootstrapConfig         `envPrefix:"BOOTSTRAP_"`

	DevMode      bool   `env:"DEV_MODE"      envDefault:"false"`
	HealthAddr   string `env:"HEALTH_ADDR"   envDefault:":8081"`
	PProfEnabled bool   `env:"PPROF_ENABLED" envDefault:"false"`
}

func (c config) validate() error
// checkBatchAckCoupling returns "" or a warning: needed = replicas * 2 * batchEvents.
func checkBatchAckCoupling(batchEvents, maxAckPending, replicas int) string
```

- [ ] **Step 1: Write the failing test**

`archive-worker/config_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

func validConfig() config {
	return config{
		SiteID: "site-a", FillInterval: 10 * time.Second, BatchEvents: 2000, BatchBytes: 8 << 20,
		PutTimeout: 10 * time.Second, BulkTimeout: 10 * time.Second, WriteAttempts: 2, FetchBatch: 100,
		Replicas: 1, BlobMaxBytes: 100 << 20, BlobAckWait: 10 * time.Minute, BlobWorkers: 4,
		Consumer: stream.ConsumerSettings{AckWait: 60 * time.Second, MaxAckPending: 12000},
		IndexRetention: "2555d",
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		require.NoError(t, validConfig().validate())
	})
	cases := []struct {
		name   string
		mutate func(*config)
		want   string
	}{
		{"batch time exceeds ack wait", func(c *config) { c.Consumer.AckWait = 40 * time.Second }, "ACK_WAIT"},
		{"zero batch events", func(c *config) { c.BatchEvents = 0 }, "ARCHIVE_BATCH_EVENTS"},
		{"zero batch bytes", func(c *config) { c.BatchBytes = 0 }, "ARCHIVE_BATCH_BYTES"},
		{"zero attempts", func(c *config) { c.WriteAttempts = 0 }, "ARCHIVE_WRITE_ATTEMPTS"},
		{"zero fetch", func(c *config) { c.FetchBatch = 0 }, "ARCHIVE_FETCH_BATCH"},
		{"fetch larger than batch", func(c *config) { c.FetchBatch = 3000 }, "ARCHIVE_FETCH_BATCH"},
		{"zero blob workers", func(c *config) { c.BlobWorkers = 0 }, "ARCHIVE_BLOB_WORKERS"},
		{"bad retention", func(c *config) { c.IndexRetention = "forever" }, "ARCHIVE_INDEX_RETENTION"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(&c)
			err := c.validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestCheckBatchAckCoupling(t *testing.T) {
	assert.Equal(t, "", checkBatchAckCoupling(2000, 12000, 3))
	w := checkBatchAckCoupling(2000, 1000, 3)
	assert.Contains(t, w, "12000")
	assert.Contains(t, w, "CONSUMER_MAX_ACK_PENDING")
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement config.go and the stub main**

`archive-worker/config.go` holds the struct from the Interfaces block plus:

```go
var retentionRe = regexp.MustCompile(`^[1-9][0-9]*(d|h|ms|s|m)$`)

func (c config) validate() error {
	if c.BatchEvents <= 0 {
		return fmt.Errorf("ARCHIVE_BATCH_EVENTS must be > 0, got %d", c.BatchEvents)
	}
	if c.BatchBytes <= 0 {
		return fmt.Errorf("ARCHIVE_BATCH_BYTES must be > 0, got %d", c.BatchBytes)
	}
	if c.WriteAttempts <= 0 {
		return fmt.Errorf("ARCHIVE_WRITE_ATTEMPTS must be > 0, got %d", c.WriteAttempts)
	}
	if c.FetchBatch <= 0 || c.FetchBatch > c.BatchEvents {
		return fmt.Errorf("ARCHIVE_FETCH_BATCH must be in 1..ARCHIVE_BATCH_EVENTS, got %d", c.FetchBatch)
	}
	if c.BlobWorkers <= 0 {
		return fmt.Errorf("ARCHIVE_BLOB_WORKERS must be > 0, got %d", c.BlobWorkers)
	}
	if !retentionRe.MatchString(c.IndexRetention) {
		return fmt.Errorf("ARCHIVE_INDEX_RETENTION must be an ES duration such as 2555d, got %q", c.IndexRetention)
	}
	worst := c.FillInterval + time.Duration(c.WriteAttempts)*(c.PutTimeout+c.BulkTimeout)
	if worst >= c.Consumer.AckWait {
		return fmt.Errorf("CONSUMER_ACK_WAIT (%s) must exceed ARCHIVE_FILL_INTERVAL + ARCHIVE_WRITE_ATTEMPTS x (ARCHIVE_PUT_TIMEOUT + ARCHIVE_BULK_TIMEOUT) = %s", c.Consumer.AckWait, worst)
	}
	return nil
}

// checkBatchAckCoupling mirrors search-sync-worker's check: one batch
// uploading plus one filling per pod, across every replica of the durable.
func checkBatchAckCoupling(batchEvents, maxAckPending, replicas int) string {
	needed := replicas * 2 * batchEvents
	if needed > maxAckPending {
		return fmt.Sprintf("ARCHIVE_BATCH_EVENTS (%d) at ARCHIVE_REPLICAS %d needs CONSUMER_MAX_ACK_PENDING >= %d but it is %d: the server will stop delivering before a batch fills, so segments seal on the timer undersized", batchEvents, replicas, needed, maxAckPending)
	}
	return ""
}
```

`archive-worker/main.go` stub for now:

```go
package main

import (
	"log/slog"
	"os"

	"github.com/caarlos0/env/v11"
)

func main() {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	if err := cfg.validate(); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=archive-worker && make build SERVICE=archive-worker`
Expected: PASS and a binary at `bin/archive-worker`.

- [ ] **Step 5: Add deploy files and README**

`archive-worker/deploy/Dockerfile`:

```dockerfile
FROM golang:1.25.13-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ pkg/
COPY archive-worker/ archive-worker/
RUN CGO_ENABLED=0 go build -o /archive-worker ./archive-worker/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=builder /archive-worker /archive-worker
USER app
ENTRYPOINT ["/archive-worker"]
```

`archive-worker/deploy/docker-compose.yml`:

```yaml
name: archive-worker

services:
  archive-worker:
    build:
      context: ../..
      dockerfile: archive-worker/deploy/Dockerfile
    pull_policy: build
    environment:
      - OTEL_SERVICE_NAME=archive-worker
      - OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:-http://otel-collector:4318}
      - NATS_URL=${NATS_URL:-nats://nats:4222}
      - NATS_CREDS_FILE=${NATS_CREDS_FILE:-/etc/nats/backend.creds}
      - SITE_ID=${SITE_ID:-site-local}
      - ARCHIVE_SEARCH_URL=${ARCHIVE_SEARCH_URL:-http://elasticsearch:9200}
      - ARCHIVE_S3_ENDPOINT=${ARCHIVE_S3_ENDPOINT:-minio:9000}
      - ARCHIVE_S3_ACCESS_KEY=${MINIO_ROOT_USER:-minioadmin}
      - ARCHIVE_S3_SECRET_KEY=${MINIO_ROOT_PASSWORD:-minioadmin}
      - ARCHIVE_BUCKET=${ARCHIVE_BUCKET:-archive-${SITE_ID:-site-local}}
      - VAULT_ADDR=${VAULT_ADDR:-http://vault:8200}
      - VAULT_TOKEN=${VAULT_TOKEN:-dev-only-token}
      - ATREST_VAULT_TRANSIT_KEY=chat-audit-kek
      - DRIVE_URL=${DRIVE_URL:-http://drive-stub:8080}
      - DRIVE_API_TOKEN=${DRIVE_API_TOKEN:-dev-only}
      - BOOTSTRAP_STREAMS=${BOOTSTRAP_STREAMS:-true}
      - DEV_MODE=true
      - CONSUMER_ACK_WAIT=${CONSUMER_ACK_WAIT:-60s}
      - CONSUMER_MAX_ACK_PENDING=${CONSUMER_MAX_ACK_PENDING:-4000}
      - PPROF_ENABLED=${PPROF_ENABLED:-false}
    volumes:
      - ../../docker-local/backend.creds:${NATS_CREDS_FILE:-/etc/nats/backend.creds}:ro
    networks:
      - chat-local

networks:
  chat-local:
    external: true
```

`archive-worker/deploy/azure-pipelines.yml`: copy `search-sync-worker/deploy/azure-pipelines.yml` and replace every `search-sync-worker` with `archive-worker`.

`archive-worker/README.md`: one page: what it archives, the three lanes, the batching knobs table from spec §4, the plain-English outage behaviour from spec §4, the Object Lock precondition, and the Vault key name.

- [ ] **Step 6: Lint and commit**

Run: `make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): scaffold config, validation and deploy files"
```

---

### Task 8: `archive-worker` DEK bootstrap through the keys index

**Files:**
- Create: `archive-worker/store.go` (interfaces + `//go:generate mockgen`), `archive-worker/keys.go`
- Test: `archive-worker/keys_test.go`, `archive-worker/mock_store_test.go` (generated)

**Interfaces:**
- Consumes: `auditarchive.KeysIndex`, `auditarchive.KeyDocID`, `auditarchive.KeyDoc`, `auditarchive.NewCipher`, `atrest.KeyWrapper`, `searchengine.ActionCreate`.
- Produces:

```go
// store.go
type indexStore interface {
	Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error)
	GetDoc(ctx context.Context, index, docID string) (json.RawMessage, bool, error)
	UpsertTemplate(ctx context.Context, name string, body json.RawMessage) error
	EnsureLifecyclePolicy(ctx context.Context, name string, body json.RawMessage) (bool, error)
}
type objectStore interface {
	Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error
}
//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// keys.go
// loadOrCreateDEK returns the site's plaintext DEK. On an empty keys index it
// generates one, publishes the wrapped form with op_type create, and on a
// create conflict (another replica won) re-reads and unwraps that one.
func loadOrCreateDEK(ctx context.Context, idx indexStore, wrapper atrest.KeyWrapper, site string, now func() time.Time) ([]byte, error)
```

- [ ] **Step 1: Write the failing test**

`archive-worker/keys_test.go` (uses the generated mock plus a static wrapper copied from `pkg/atrest/cipher_test.go`'s pattern):

```go
package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// staticWrapper wraps with a fixed key so tests can round-trip without Vault.
type staticWrapper struct{ aead cipher.AEAD }

func newStaticWrapper(t *testing.T) *staticWrapper {
	t.Helper()
	block, err := aes.NewCipher(bytes.Repeat([]byte{'k'}, 32))
	require.NoError(t, err)
	a, err := cipher.NewGCM(block)
	require.NoError(t, err)
	return &staticWrapper{aead: a}
}
func (w *staticWrapper) GenerateDataKey(context.Context) ([]byte, []byte, error) {
	dek := bytes.Repeat([]byte{'d'}, 32)
	wrapped, err := w.Wrap(context.Background(), dek)
	return dek, wrapped, err
}
func (w *staticWrapper) Wrap(_ context.Context, dek []byte) ([]byte, error) {
	nonce := bytes.Repeat([]byte{'n'}, w.aead.NonceSize())
	return append(append([]byte{}, nonce...), w.aead.Seal(nil, nonce, dek, nil)...), nil
}
func (w *staticWrapper) Unwrap(_ context.Context, ct []byte) ([]byte, error) {
	ns := w.aead.NonceSize()
	return w.aead.Open(nil, ct[:ns], ct[ns:], nil)
}

func TestLoadOrCreateDEK(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }

	t.Run("existing key is unwrapped, nothing written", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		w := newStaticWrapper(t)
		wrapped, _ := w.Wrap(ctx, bytes.Repeat([]byte{'x'}, 32))
		doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: wrapped, CreatedAt: now()})
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil)

		dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'x'}, 32), dek)
	})

	t.Run("missing key is generated and created", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		w := newStaticWrapper(t)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		idx.EXPECT().Bulk(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
			require.Len(t, a, 1)
			assert.Equal(t, searchengine.ActionCreate, a[0].Action)
			assert.Equal(t, "audit-keys-site-a", a[0].Index)
			assert.Equal(t, "current", a[0].DocID)
			var d auditarchive.KeyDoc
			require.NoError(t, json.Unmarshal(a[0].Doc, &d))
			assert.Equal(t, "site-a", d.SiteID)
			assert.NotEmpty(t, d.WrappedDek)
			return []searchengine.BulkResult{{Status: 201}}, nil
		})
		dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
		require.NoError(t, err)
		assert.Len(t, dek, auditarchive.DEKSize)
	})

	t.Run("create conflict re-reads the winner", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		w := newStaticWrapper(t)
		winner, _ := w.Wrap(ctx, bytes.Repeat([]byte{'w'}, 32))
		doc := hitJSON(t, auditarchive.KeyDoc{SiteID: "site-a", WrappedDek: winner})
		gomock.InOrder(
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil),
			idx.EXPECT().Bulk(ctx, gomock.Any()).Return([]searchengine.BulkResult{{Status: 409, ErrorType: "version_conflict_engine_exception"}}, nil),
			idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(doc, true, nil),
		)
		dek, err := loadOrCreateDEK(ctx, idx, w, "site-a", now)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{'w'}, 32), dek)
	})

	t.Run("index error fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, errors.New("boom"))
		_, err := loadOrCreateDEK(ctx, idx, newStaticWrapper(t), "site-a", now)
		assert.Error(t, err)
	})

	t.Run("wrapper failure fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		idx := NewMockindexStore(ctrl)
		idx.EXPECT().GetDoc(ctx, "audit-keys-site-a", "current").Return(nil, false, nil)
		_, err := loadOrCreateDEK(ctx, idx, failingWrapper{}, "site-a", now)
		assert.Error(t, err)
	})
}

// hitJSON wraps a document the way GET /{index}/_doc/{id} returns it.
func hitJSON(t *testing.T, doc any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]any{"_index": "x", "_id": "current", "found": true, "_source": doc})
	require.NoError(t, err)
	return b
}

type failingWrapper struct{}

func (failingWrapper) GenerateDataKey(context.Context) ([]byte, []byte, error) {
	return nil, nil, errors.New("vault down")
}
func (failingWrapper) Wrap(context.Context, []byte) ([]byte, error)   { return nil, errors.New("vault down") }
func (failingWrapper) Unwrap(context.Context, []byte) ([]byte, error) { return nil, errors.New("vault down") }
```

- [ ] **Step 2: Create store.go, generate the mock, run the test to see it fail**

Write `archive-worker/store.go` with the two interfaces and the mockgen directive from the Interfaces block, then:

Run: `make generate SERVICE=archive-worker && make test SERVICE=archive-worker`
Expected: FAIL, `undefined: loadOrCreateDEK`.

- [ ] **Step 3: Implement keys.go**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func loadOrCreateDEK(ctx context.Context, idx indexStore, wrapper atrest.KeyWrapper, site string, now func() time.Time) ([]byte, error) {
	index := auditarchive.KeysIndex(site)
	if dek, found, err := readDEK(ctx, idx, wrapper, index); err != nil || found {
		return dek, err
	}
	plain, wrapped, err := wrapper.GenerateDataKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("generate archive DEK: %w", err)
	}
	doc, err := json.Marshal(auditarchive.KeyDoc{SiteID: site, WrappedDek: wrapped, CreatedAt: now()})
	if err != nil {
		return nil, fmt.Errorf("marshal key doc: %w", err)
	}
	results, err := idx.Bulk(ctx, []searchengine.BulkAction{{Action: searchengine.ActionCreate, Index: index, DocID: auditarchive.KeyDocID, Doc: doc}})
	if err != nil {
		return nil, fmt.Errorf("create key doc: %w", err)
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("create key doc: %d results for 1 action", len(results))
	}
	switch {
	case results[0].Status >= 200 && results[0].Status < 300:
		slog.Info("archive DEK created", "site", site, "index", index)
		return plain, nil
	case results[0].Status == 409:
		slog.Info("archive DEK already created by a peer, re-reading", "site", site)
		dek, found, err := readDEK(ctx, idx, wrapper, index)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("create key doc: conflict but no document found")
		}
		return dek, nil
	default:
		return nil, fmt.Errorf("create key doc: status %d %s", results[0].Status, results[0].ErrorType)
	}
}

func readDEK(ctx context.Context, idx indexStore, wrapper atrest.KeyWrapper, index string) ([]byte, bool, error) {
	raw, found, err := idx.GetDoc(ctx, index, auditarchive.KeyDocID)
	if err != nil {
		return nil, false, fmt.Errorf("read key doc: %w", err)
	}
	if !found {
		return nil, false, nil
	}
	// GetDoc returns the whole hit; the document is under _source.
	var hit struct {
		Source auditarchive.KeyDoc `json:"_source"`
	}
	if err := json.Unmarshal(raw, &hit); err != nil {
		return nil, false, fmt.Errorf("decode key doc: %w", err)
	}
	dek, err := wrapper.Unwrap(ctx, hit.Source.WrappedDek)
	if err != nil {
		return nil, false, fmt.Errorf("unwrap archive DEK: %w", err)
	}
	if len(dek) != auditarchive.DEKSize {
		return nil, false, fmt.Errorf("unwrapped DEK is %d bytes, want %d", len(dek), auditarchive.DEKSize)
	}
	return dek, true, nil
}
```

`GetDoc` (`pkg/searchengine/adapter.go:393`) returns the raw `GET /{index}/_doc/{id}`
body, so the document sits under `_source`; the test fixtures below wrap their
documents the same way.

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): bootstrap the site DEK through the audit-keys index"
```

---

### Task 9: `archive-worker` bootstrap: streams, templates, lifecycle policy

**Files:**
- Create: `archive-worker/bootstrap.go`
- Test: `archive-worker/bootstrap_test.go`

**Interfaces:**
- Consumes: `indexStore` (Task 8), `auditarchive.Templates`, `auditarchive.LifecyclePolicyBody`, `auditarchive.LifecyclePolicyName`, `stream.MessagesCanonical`.
- Produces:

```go
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}
// bootstrapStreams creates MESSAGES-CANONICAL in dev or verifies it exists in
// production; it never creates INBOX (owned by inbox-worker) and only verifies it.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error
// bootstrapIndex ensures the lifecycle policy (create-only) and upserts the
// four templates. Idempotent; runs on every start.
func bootstrapIndex(ctx context.Context, idx indexStore, siteID, retention string, devMode bool) error
```

- [ ] **Step 1: Write the failing test**

`archive-worker/bootstrap_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	o11ynats "github.com/flywindy/o11y/nats"
)

type fakeStreams struct {
	created  []jetstream.StreamConfig
	existing map[string]bool
}

func (f *fakeStreams) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error) {
	f.created = append(f.created, cfg)
	return nil, nil
}
func (f *fakeStreams) Stream(_ context.Context, name string) (o11ynats.Stream, error) {
	if f.existing[name] {
		return nil, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

func TestBootstrapStreams(t *testing.T) {
	t.Run("enabled creates canonical only and verifies inbox", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true}}
		require.NoError(t, bootstrapStreams(context.Background(), f, "site-a", true))
		require.Len(t, f.created, 1)
		assert.Equal(t, "MESSAGES-CANONICAL-site-a", f.created[0].Name)
		assert.Equal(t, []string{"chat.msg.canonical.site-a.>"}, f.created[0].Subjects)
	})
	t.Run("enabled still fails when inbox is missing", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{}}
		err := bootstrapStreams(context.Background(), f, "site-a", true)
		assert.True(t, errors.Is(err, jetstream.ErrStreamNotFound))
	})
	t.Run("disabled verifies both, creates nothing", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true, "MESSAGES-CANONICAL-site-a": true}}
		require.NoError(t, bootstrapStreams(context.Background(), f, "site-a", false))
		assert.Empty(t, f.created)
	})
	t.Run("disabled fails when canonical is missing", func(t *testing.T) {
		f := &fakeStreams{existing: map[string]bool{"INBOX-site-a": true}}
		assert.Error(t, bootstrapStreams(context.Background(), f, "site-a", false))
	})
}

func TestBootstrapIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	idx := NewMockindexStore(ctrl)
	ctx := context.Background()
	idx.EXPECT().EnsureLifecyclePolicy(ctx, "audit-archive", gomock.Any()).Return(true, nil)
	for _, name := range []string{"audit-events-site-a", "audit-members-site-a", "audit-blobs-site-a", "audit-keys-site-a"} {
		idx.EXPECT().UpsertTemplate(ctx, name, gomock.Any()).Return(nil)
	}
	require.NoError(t, bootstrapIndex(ctx, idx, "site-a", "2555d", true))

	t.Run("policy failure stops before templates", func(t *testing.T) {
		idx := NewMockindexStore(ctrl)
		idx.EXPECT().EnsureLifecyclePolicy(ctx, "audit-archive", gomock.Any()).Return(false, errors.New("ilm unavailable"))
		assert.Error(t, bootstrapIndex(ctx, idx, "site-a", "2555d", true))
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/bootstrap.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/stream"
)

// streamManager is the JetStream surface bootstrap needs; tests inject a fake.
type streamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (o11ynats.Stream, error)
	Stream(ctx context.Context, name string) (o11ynats.Stream, error)
}

// bootstrapStreams follows the repo convention: schema-only creation of the
// stream this worker reads when BOOTSTRAP_STREAMS=true (dev), verification
// otherwise. INBOX belongs to inbox-worker and is only ever verified.
func bootstrapStreams(ctx context.Context, js streamManager, siteID string, enabled bool) error {
	canonical := stream.MessagesCanonical(siteID)
	if enabled {
		if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: canonical.Name, Subjects: canonical.Subjects}); err != nil {
			return fmt.Errorf("create stream %s: %w", canonical.Name, err)
		}
	} else if _, err := js.Stream(ctx, canonical.Name); err != nil {
		return fmt.Errorf("verify stream %s: %w", canonical.Name, err)
	}
	inbox := stream.Inbox(siteID)
	if _, err := js.Stream(ctx, inbox.Name); err != nil {
		return fmt.Errorf("verify stream %s (owned by inbox-worker): %w", inbox.Name, err)
	}
	return nil
}

// bootstrapIndex is idempotent and runs on every start: the policy is created
// only if absent so operator edits survive, templates are upserted.
func bootstrapIndex(ctx context.Context, idx indexStore, siteID, retention string, devMode bool) error {
	created, err := idx.EnsureLifecyclePolicy(ctx, auditarchive.LifecyclePolicyName, auditarchive.LifecyclePolicyBody(retention))
	if err != nil {
		return fmt.Errorf("ensure lifecycle policy: %w", err)
	}
	slog.Info("lifecycle policy ensured", "name", auditarchive.LifecyclePolicyName, "created", created)
	for _, tpl := range auditarchive.Templates(siteID, devMode) {
		if err := idx.UpsertTemplate(ctx, tpl.Name, tpl.Body); err != nil {
			return fmt.Errorf("upsert template %s: %w", tpl.Name, err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): bootstrap streams, templates and lifecycle policy"
```

---

### Task 10: `archive-worker` bucket sink and Object Lock precondition

**Files:**
- Create: `archive-worker/sink_bucket.go`
- Test: `archive-worker/sink_bucket_test.go` (unit), `archive-worker/sink_bucket_integration_test.go` (MinIO)

**Interfaces:**
- Consumes: `objectStore` (Task 8), `minioutil.ObjectStore`.
- Produces:

```go
var errObjectLockRequired = errors.New("archive bucket is not in Object Lock compliance mode")
type lockConfigReader interface {
	GetObjectLockConfig(ctx context.Context, bucket string) (objectLock string, mode *minio.RetentionMode, validity *uint, unit *minio.ValidityUnit, err error)
}
// checkObjectLock fails unless lock is enabled and mode is COMPLIANCE; when
// require is false it only warns (local dev without a locked bucket).
func checkObjectLock(ctx context.Context, r lockConfigReader, bucket string, require bool) error
type bucketSink struct { client minioutil.ObjectStore; bucket string }
func newBucketSink(client minioutil.ObjectStore, bucket string) *bucketSink
func (s *bucketSink) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error
```

- [ ] **Step 1: Write the failing unit test**

`archive-worker/sink_bucket_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
)

type fakeLock struct {
	lock string
	mode *minio.RetentionMode
	err  error
}

func (f fakeLock) GetObjectLockConfig(context.Context, string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error) {
	return f.lock, f.mode, nil, nil, f.err
}

func TestCheckObjectLock(t *testing.T) {
	compliance, governance := minio.Compliance, minio.Governance
	cases := []struct {
		name    string
		lock    fakeLock
		require bool
		wantErr error
	}{
		{"compliance passes", fakeLock{lock: "Enabled", mode: &compliance}, true, nil},
		{"governance fails", fakeLock{lock: "Enabled", mode: &governance}, true, errObjectLockRequired},
		{"no lock fails", fakeLock{lock: "", mode: nil}, true, errObjectLockRequired},
		{"no lock allowed when not required", fakeLock{lock: ""}, false, nil},
		{"lookup error fails even when not required", fakeLock{err: errors.New("403")}, false, errors.New("403")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkObjectLock(context.Background(), tc.lock, "b", tc.require)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
			if errors.Is(tc.wantErr, errObjectLockRequired) {
				assert.True(t, errors.Is(err, errObjectLockRequired))
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/sink_bucket.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/minio/minio-go/v7"

	"github.com/hmchangw/chat/pkg/minioutil"
)

var errObjectLockRequired = errors.New("archive bucket is not in Object Lock compliance mode")

// lockConfigReader is the one minio-go call the startup check needs; the
// production value is a *minio.Client built for this check alone.
type lockConfigReader interface {
	GetObjectLockConfig(ctx context.Context, bucket string) (string, *minio.RetentionMode, *uint, *minio.ValidityUnit, error)
}

func checkObjectLock(ctx context.Context, r lockConfigReader, bucket string, require bool) error {
	lock, mode, _, _, err := r.GetObjectLockConfig(ctx, bucket)
	if err != nil {
		return fmt.Errorf("read object lock config for %q: %w", bucket, err)
	}
	ok := lock == "Enabled" && mode != nil && *mode == minio.Compliance
	if ok {
		return nil
	}
	if !require {
		slog.Warn("archive bucket is not Object Lock compliance mode; continuing because ARCHIVE_REQUIRE_OBJECT_LOCK=false", "bucket", bucket, "lock", lock)
		return nil
	}
	return fmt.Errorf("%w: bucket %q lock=%q", errObjectLockRequired, bucket, lock)
}

type bucketSink struct {
	client minioutil.ObjectStore
	bucket string
}

func newBucketSink(client minioutil.ObjectStore, bucket string) *bucketSink {
	return &bucketSink{client: client, bucket: bucket}
}

// Put writes one object. Retention comes from the bucket's default rule, so
// no per-object lock options are set.
func (s *bucketSink) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	if _, err := s.client.PutObject(ctx, s.bucket, key, body, size, minio.PutObjectOptions{ContentType: contentType}); err != nil {
		return fmt.Errorf("put %s/%s: %w", s.bucket, key, err)
	}
	return nil
}
```

- [ ] **Step 4: Write the integration test against a locked MinIO bucket**

`archive-worker/sink_bucket_integration_test.go`:

```go
//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

// lockedBucket makes a per-test bucket with Object Lock and a 1-day
// compliance default. testutil.MinIO creates unlocked buckets, so this test
// owns its own; the bucket cannot be deleted while objects are retained,
// so cleanup is intentionally skipped (the container is discarded).
func lockedBucket(t *testing.T) (*minio.Client, string) {
	t.Helper()
	endpoint, ak, sk := testutil.MinIOEndpoint(t)
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, ""), Secure: false})
	require.NoError(t, err)
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Name()))
	bucket := fmt.Sprintf("archive-%x", h.Sum64())
	ctx := context.Background()
	require.NoError(t, c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{ObjectLocking: true}))
	mode, validity, unit := minio.Compliance, uint(1), minio.Days
	require.NoError(t, c.SetObjectLockConfig(ctx, bucket, &mode, &validity, &unit))
	return c, bucket
}

func TestBucketSink_LockedBucket(t *testing.T) {
	ctx := context.Background()
	c, bucket := lockedBucket(t)
	require.NoError(t, checkObjectLock(ctx, c, bucket, true))

	sink := newBucketSink(c, bucket)
	body := []byte("sealed segment bytes")
	require.NoError(t, sink.Put(ctx, "site-a/2026/10/05/14/events-1-3.seg", bytes.NewReader(body), int64(len(body)), "application/octet-stream"))

	obj, err := c.GetObject(ctx, bucket, "site-a/2026/10/05/14/events-1-3.seg", minio.GetObjectOptions{})
	require.NoError(t, err)
	got, err := io.ReadAll(obj)
	require.NoError(t, err)
	assert.Equal(t, body, got)

	t.Run("retained object cannot be deleted", func(t *testing.T) {
		err := c.RemoveObject(ctx, bucket, "site-a/2026/10/05/14/events-1-3.seg", minio.RemoveObjectOptions{})
		assert.Error(t, err, "compliance retention must refuse the delete")
	})
	t.Run("unlocked bucket is refused", func(t *testing.T) {
		plain, unlocked := testutil.MinIO(t, "archive-unlocked")
		err := checkObjectLock(ctx, plain, unlocked, true)
		assert.ErrorIs(t, err, errObjectLockRequired)
	})
}
```

Add `archive-worker/main_test.go`:

```go
//go:build integration

package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithPrewarm(m, testutil.EnsureMinIO, testutil.EnsureNATS, testutil.EnsureElasticsearch)
}
```

- [ ] **Step 5: Run both, then commit**

Run: `make test SERVICE=archive-worker && make test-integration SERVICE=archive-worker`
Expected: PASS. If `RemoveObject` on a retained object succeeds, the MinIO image does not enforce the default rule on a plain delete; change the assertion to `RemoveObject` with `ForceDelete: false` and `GovernanceBypass: false` and verify instead that `GetObjectRetention` reports `COMPLIANCE`, which is what the worker depends on.

```bash
git add archive-worker
git commit -m "feat(archive-worker): bucket sink with Object Lock compliance precondition"
```

---

### Task 11: `archive-worker` batcher and sealer

**Files:**
- Create: `archive-worker/batch.go`
- Test: `archive-worker/batch_test.go`

**Interfaces:**
- Consumes: `auditarchive.WriteSegment`, `auditarchive.SegmentKey`, `auditarchive.Header`.
- Produces:

```go
// locatable is implemented by *auditarchive.EventDoc and *auditarchive.MemberDoc.
type locatable interface{ SetLocation(segmentKey string, frameOffset int64) }
type docSpec struct { Index, ID string; Doc locatable }
// item is one NATS message's contribution: one sealed frame, N documents.
type item struct {
	ctx   context.Context
	msg   jetstream.Msg
	seq   uint64
	frame []byte
	docs  []docSpec
}
type batcher struct { /* mu, items, bytes, firstAt, limits */ }
func newBatcher(maxEvents, maxBytes int, fill time.Duration) *batcher
func (b *batcher) add(it item, now time.Time) (full bool)
func (b *batcher) due(now time.Time) bool
func (b *batcher) take() []item // returns and clears; nil when empty
func (b *batcher) len() int
type sealed struct { key string; body []byte; actions []searchengine.BulkAction; items []item }
// seal builds the segment, fills every doc's location, and marshals the create actions.
func seal(site, lane string, items []item, now time.Time) (*sealed, error)
```

- [ ] **Step 1: Write the failing test**

`archive-worker/batch_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

func mkItem(seq uint64, size int) item {
	return item{ctx: context.Background(), seq: seq, frame: bytes.Repeat([]byte{1}, size),
		docs: []docSpec{{Index: "audit-events-site-a-2026.10.05", ID: auditarchive.EventDocID("site-a", seq), Doc: &auditarchive.EventDoc{Seq: seq}}}}
}

func TestBatcher_Bounds(t *testing.T) {
	t0 := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	t.Run("count bound", func(t *testing.T) {
		b := newBatcher(2, 1<<20, time.Minute)
		assert.False(t, b.add(mkItem(1, 10), t0))
		assert.True(t, b.add(mkItem(2, 10), t0))
		assert.Len(t, b.take(), 2)
		assert.Nil(t, b.take())
	})
	t.Run("byte bound", func(t *testing.T) {
		b := newBatcher(100, 25, time.Minute)
		assert.False(t, b.add(mkItem(1, 10), t0))
		assert.True(t, b.add(mkItem(2, 20), t0))
	})
	t.Run("time bound", func(t *testing.T) {
		b := newBatcher(100, 1<<20, 10*time.Second)
		assert.False(t, b.due(t0))
		b.add(mkItem(1, 1), t0)
		assert.False(t, b.due(t0.Add(9*time.Second)))
		assert.True(t, b.due(t0.Add(10*time.Second)))
	})
}

func TestSeal(t *testing.T) {
	now := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	items := []item{mkItem(7, 3), mkItem(5, 4), mkItem(9, 5)}
	s, err := seal("site-a", "events", items, now)
	require.NoError(t, err)
	assert.Equal(t, "site-a/2026/10/05/14/events-5-9.seg", s.key, "key uses min and max seq")

	h, frames, err := auditarchive.ReadSegment(bytes.NewReader(s.body))
	require.NoError(t, err)
	assert.Equal(t, uint32(3), h.Count)
	assert.Equal(t, "events", h.Lane)
	assert.Equal(t, [][]byte{items[0].frame, items[1].frame, items[2].frame}, frames, "frames keep delivery order")

	require.Len(t, s.actions, 3)
	for i, a := range s.actions {
		assert.Equal(t, searchengine.ActionCreate, a.Action)
		assert.Equal(t, "audit-events-site-a-2026.10.05", a.Index)
		var d auditarchive.EventDoc
		require.NoError(t, json.Unmarshal(a.Doc, &d))
		assert.Equal(t, s.key, d.SegmentKey)
		f, err := auditarchive.ReadFrameAt(bytes.NewReader(s.body), d.FrameOffset)
		require.NoError(t, err)
		assert.Equal(t, items[i].frame, f, "offset %d points at its own frame", i)
	}
	t.Run("empty is an error", func(t *testing.T) {
		_, err := seal("site-a", "events", nil, now)
		assert.Error(t, err)
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/batch.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type locatable interface {
	SetLocation(segmentKey string, frameOffset int64)
}

type docSpec struct {
	Index string
	ID    string
	Doc   locatable
}

type item struct {
	ctx   context.Context
	msg   jetstream.Msg
	seq   uint64
	frame []byte
	docs  []docSpec
}

type batcher struct {
	mu        sync.Mutex
	items     []item
	bytes     int
	firstAt   time.Time
	maxEvents int
	maxBytes  int
	fill      time.Duration
}

func newBatcher(maxEvents, maxBytes int, fill time.Duration) *batcher {
	return &batcher{maxEvents: maxEvents, maxBytes: maxBytes, fill: fill}
}

// add appends and reports whether a count or byte bound tripped.
func (b *batcher) add(it item, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		b.firstAt = now
	}
	b.items = append(b.items, it)
	b.bytes += len(it.frame)
	return len(b.items) >= b.maxEvents || b.bytes >= b.maxBytes
}

func (b *batcher) due(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items) > 0 && !now.Before(b.firstAt.Add(b.fill))
}

func (b *batcher) take() []item {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		return nil
	}
	out := b.items
	b.items, b.bytes = nil, 0
	return out
}

func (b *batcher) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

type sealed struct {
	key     string
	body    []byte
	actions []searchengine.BulkAction
	items   []item
}

// seal writes the segment for items in delivery order, points every document
// at its frame, and returns the create actions in the same order as the
// documents so flush can map results back to items.
func seal(site, lane string, items []item, now time.Time) (*sealed, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("seal: no items")
	}
	first, last := items[0].seq, items[0].seq
	frames := make([][]byte, len(items))
	for i, it := range items {
		frames[i] = it.frame
		first, last = min(first, it.seq), max(last, it.seq)
	}
	key := auditarchive.SegmentKey(site, lane, now, first, last)
	var buf bytes.Buffer
	offsets, _, err := auditarchive.WriteSegment(&buf, auditarchive.Header{Site: site, Lane: lane, FirstSeq: first, LastSeq: last, Count: uint32(len(frames))}, frames)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	var actions []searchengine.BulkAction
	for i, it := range items {
		for _, d := range it.docs {
			d.Doc.SetLocation(key, offsets[i])
			body, err := json.Marshal(d.Doc)
			if err != nil {
				return nil, fmt.Errorf("seal: marshal doc %s: %w", d.ID, err)
			}
			actions = append(actions, searchengine.BulkAction{Action: searchengine.ActionCreate, Index: d.Index, DocID: d.ID, Doc: body})
		}
	}
	return &sealed{key: key, body: buf.Bytes(), actions: actions, items: items}, nil
}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): batcher with three bounds and segment sealer"
```

---

### Task 12: `archive-worker` message event builder

**Files:**
- Create: `archive-worker/handler_events.go`
- Test: `archive-worker/handler_events_test.go`, `archive-worker/testdata/events.json`

**Interfaces:**
- Consumes: `item`, `docSpec` (Task 11), `auditarchive.Record`, `auditarchive.Cipher`, `auditarchive.EventDoc`, `auditarchive.EventsIndex`, `auditarchive.EventDocID`, `auditarchive.FrameAAD`, `auditarchive.BodyAAD`, `model.MessageEvent`, `cassandra.DecodeAttachments`.
- Produces:

```go
var errPoison = errors.New("poison event")   // terminate, never retry
var errSkip = errors.New("event not archived") // ack, nothing to do
type msgMeta struct { Stream string; Seq uint64; Subject string }
// metaOf reads stream name and sequence from a JetStream message.
func metaOf(msg jetstream.Msg) (msgMeta, error)
// eventBody is the user-authored part encrypted into EncBody.
type eventBody struct {
	Content             string                         `json:"content,omitempty"`
	Attachments         [][]byte                       `json:"attachments,omitempty"`
	Card                *cassandra.Card                `json:"card,omitempty"`
	CardAction          *cassandra.CardAction          `json:"cardAction,omitempty"`
	QuotedParentMessage *cassandra.QuotedParentMessage `json:"quotedParentMessage,omitempty"`
}
// buildEventItem turns one canonical message event into one frame and one document.
func buildEventItem(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error)
```

Rules: `Event` empty is treated as `created` (legacy); `created` and `updated` carry EncBody; `deleted`, `pinned`, `unpinned`, `reacted` carry no body and set `ActorAccount` (from `ReactionDelta.Actor.Account` for reacted, else `Message.UserAccount`); any other event type, including `thread_reply_added`, is `errSkip`; JSON that does not unmarshal, a missing message id, or a missing room id is `errPoison`. `EventAt` is the event's `Timestamp` in millis (fallback `now` when zero). The daily index is chosen by `EventAt`, not by `CreatedAt`, so a late edit lands in today's index and `createdAt` still sorts it.

- [ ] **Step 1: Write the fixture and the failing test**

`archive-worker/testdata/events.json` holds four `model.MessageEvent` objects: a `created` with content and one attachment (base64 JSON of `{"id":"f1","title":"mock.png","fileType":"image/png","titleLink":"api/v1/file/rooms/r1/file/f1?drive_host=https://drive.example"}`), an `updated` with new content and `editedAt`, a `deleted`, and a `reacted` with `reactionDelta.actor.account = "h.brandt"`. All have `message.id = "m1"`, `message.roomId = "r1"`, `message.userAccount = "p.ortiz"`, `siteId = "site-a"`, `timestamp = 1759672800000`.

`archive-worker/handler_events_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
)

// fakeMsg is the minimal jetstream.Msg used by builder tests; only Metadata,
// Subject and Data matter here. Ack/Nak/Term record what the lane decided.
type fakeMsg struct {
	jetstream.Msg
	subject string
	data    []byte
	seq     uint64
	stream  string
	acked, termed bool
	nakDelay time.Duration
	naked    bool
}

func (m *fakeMsg) Subject() string { return m.subject }
func (m *fakeMsg) Data() []byte    { return m.data }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Stream: m.stream, Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}
func (m *fakeMsg) Ack() error                        { m.acked = true; return nil }
func (m *fakeMsg) Term() error                       { m.termed = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error { m.naked, m.nakDelay = true, d; return nil }
func (m *fakeMsg) InProgress() error                 { return nil }

func loadEvents(t *testing.T) map[string]model.MessageEvent {
	t.Helper()
	raw, err := os.ReadFile("testdata/events.json")
	require.NoError(t, err)
	var list []model.MessageEvent
	require.NoError(t, json.Unmarshal(raw, &list))
	out := map[string]model.MessageEvent{}
	for _, e := range list {
		out[string(e.Event)] = e
	}
	return out
}

func TestBuildEventItem(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	events := loadEvents(t)
	now := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	mk := func(ev model.MessageEvent, seq uint64) (*fakeMsg, []byte) {
		data, _ := json.Marshal(ev)
		return &fakeMsg{subject: "chat.msg.canonical.site-a." + string(ev.Event), data: data, seq: seq, stream: "MESSAGES-CANONICAL-site-a"}, data
	}

	t.Run("created carries body, frame opens to the record", func(t *testing.T) {
		msg, data := mk(events["created"], 41)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, uint64(41), it.seq)
		require.Len(t, it.docs, 1)
		d := it.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Equal(t, "audit-events-site-a-2026.10.05", it.docs[0].Index)
		assert.Equal(t, "site-a-41", it.docs[0].ID)
		assert.Equal(t, "created", d.EventType)
		assert.Equal(t, "m1", d.MessageID)
		assert.Equal(t, "p.ortiz", d.SenderAccount)
		assert.Equal(t, 1, d.AttachmentCount)
		assert.Equal(t, []string{"image/png"}, d.AttachmentTypes)
		assert.NotEmpty(t, d.EncBody)
		assert.Regexp(t, `^sha256:`, d.ContentHash)

		plain, err := c.Open(it.frame, auditarchive.FrameAAD("site-a", 41))
		require.NoError(t, err)
		var rec auditarchive.Record
		require.NoError(t, json.Unmarshal(plain, &rec))
		assert.Equal(t, uint64(41), rec.Seq)
		assert.JSONEq(t, string(data), string(rec.Payload))
		assert.Equal(t, auditarchive.HashBytes(plain), d.ContentHash)

		body, err := c.Open(d.EncBody, auditarchive.BodyAAD("site-a", 41))
		require.NoError(t, err)
		var eb eventBody
		require.NoError(t, json.Unmarshal(body, &eb))
		assert.Equal(t, events["created"].Message.Content, eb.Content)
	})
	t.Run("deleted has no body and names the actor", func(t *testing.T) {
		msg, data := mk(events["deleted"], 42)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		d := it.docs[0].Doc.(*auditarchive.EventDoc)
		assert.Empty(t, d.EncBody)
		assert.Equal(t, "p.ortiz", d.ActorAccount)
	})
	t.Run("reacted names the reactor", func(t *testing.T) {
		msg, data := mk(events["reacted"], 43)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, "h.brandt", it.docs[0].Doc.(*auditarchive.EventDoc).ActorAccount)
	})
	t.Run("thread_reply_added is skipped", func(t *testing.T) {
		ev := events["created"]
		ev.Event = model.EventThreadReplyAdded
		msg, data := mk(ev, 44)
		_, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errSkip))
	})
	t.Run("malformed json is poison", func(t *testing.T) {
		msg := &fakeMsg{subject: "x", data: []byte("{nope"), seq: 45, stream: "s"}
		_, err := buildEventItem(context.Background(), "site-a", msg, []byte("{nope"), c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
	t.Run("missing message id is poison", func(t *testing.T) {
		ev := events["created"]
		ev.Message.ID = ""
		msg, data := mk(ev, 46)
		_, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
	t.Run("empty event type means created", func(t *testing.T) {
		ev := events["created"]
		ev.Event = ""
		msg, data := mk(ev, 47)
		it, err := buildEventItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		assert.Equal(t, "created", it.docs[0].Doc.(*auditarchive.EventDoc).EventType)
	})
}
```

Add `func testDEK() []byte { return bytes.Repeat([]byte{0x4b}, auditarchive.DEKSize) }` to `archive-worker/helpers_test.go` (package main, with the `bytes` and `auditarchive` imports).

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/handler_events.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

var (
	errPoison = errors.New("poison event")
	errSkip   = errors.New("event not archived")
)

type msgMeta struct {
	Stream  string
	Seq     uint64
	Subject string
}

func metaOf(msg jetstream.Msg) (msgMeta, error) {
	md, err := msg.Metadata()
	if err != nil {
		return msgMeta{}, fmt.Errorf("%w: metadata: %w", errPoison, err)
	}
	if md.Sequence.Stream == 0 {
		return msgMeta{}, fmt.Errorf("%w: zero stream sequence", errPoison)
	}
	return msgMeta{Stream: md.Stream, Seq: md.Sequence.Stream, Subject: msg.Subject()}, nil
}

type eventBody struct {
	Content             string                         `json:"content,omitempty"`
	Attachments         [][]byte                       `json:"attachments,omitempty"`
	Card                *cassandra.Card                `json:"card,omitempty"`
	CardAction          *cassandra.CardAction          `json:"cardAction,omitempty"`
	QuotedParentMessage *cassandra.QuotedParentMessage `json:"quotedParentMessage,omitempty"`
}

func eventAt(ts int64, now time.Time) time.Time {
	if ts <= 0 {
		return now
	}
	return time.UnixMilli(ts).UTC()
}

// sealRecord encrypts the canonical record into a frame and returns the
// frame plus the record hash the document carries for later verification.
func sealRecord(site string, meta msgMeta, at time.Time, data []byte, c *auditarchive.Cipher) ([]byte, string, error) {
	rec := auditarchive.Record{Site: site, Stream: meta.Stream, Seq: meta.Seq, Subject: meta.Subject, EventAt: at.UnixMilli(), Payload: json.RawMessage(data)}
	plain, err := rec.Marshal()
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", errPoison, err)
	}
	frame, err := c.Seal(plain, auditarchive.FrameAAD(site, meta.Seq))
	if err != nil {
		return nil, "", fmt.Errorf("seal record: %w", err)
	}
	return frame, auditarchive.HashBytes(plain), nil
}

func buildEventItem(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error) {
	meta, err := metaOf(msg)
	if err != nil {
		return item{}, err
	}
	var ev model.MessageEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return item{}, fmt.Errorf("%w: unmarshal message event: %w", errPoison, err)
	}
	if ev.Event == "" {
		ev.Event = model.EventCreated
	}
	switch ev.Event {
	case model.EventCreated, model.EventUpdated, model.EventDeleted, model.EventPinned, model.EventUnpinned, model.EventReacted:
	default:
		return item{}, fmt.Errorf("%w: event type %q", errSkip, ev.Event)
	}
	if ev.Message.ID == "" || ev.Message.RoomID == "" {
		return item{}, fmt.Errorf("%w: message id or room id missing", errPoison)
	}
	at := eventAt(ev.Timestamp, now)
	frame, hash, err := sealRecord(site, meta, at, data, c)
	if err != nil {
		return item{}, err
	}
	doc := &auditarchive.EventDoc{
		Seq: meta.Seq, EventType: string(ev.Event), EventAt: at,
		MessageID: ev.Message.ID, RoomID: ev.Message.RoomID, SiteID: site,
		SenderAccount: ev.Message.UserAccount, SenderID: ev.Message.UserID,
		CreatedAt: ev.Message.CreatedAt.UTC(), ThreadParentID: ev.Message.ThreadParentMessageID,
		ContentHash: hash,
	}
	switch ev.Event {
	case model.EventCreated, model.EventUpdated:
		atts, _ := cassandra.DecodeAttachments(ev.Message.Attachments)
		doc.AttachmentCount = len(atts)
		for _, a := range atts {
			doc.AttachmentTypes = append(doc.AttachmentTypes, a.FileType)
		}
		body, err := json.Marshal(eventBody{Content: ev.Message.Content, Attachments: ev.Message.Attachments, Card: ev.Message.Card, CardAction: ev.Message.CardAction, QuotedParentMessage: ev.Message.QuotedParentMessage})
		if err != nil {
			return item{}, fmt.Errorf("%w: marshal body: %w", errPoison, err)
		}
		if doc.EncBody, err = c.Seal(body, auditarchive.BodyAAD(site, meta.Seq)); err != nil {
			return item{}, fmt.Errorf("seal body: %w", err)
		}
	case model.EventReacted:
		if ev.ReactionDelta != nil {
			doc.ActorAccount = ev.ReactionDelta.Actor.Account
		}
	default:
		doc.ActorAccount = ev.Message.UserAccount
	}
	return item{ctx: ctx, msg: msg, seq: meta.Seq, frame: frame, docs: []docSpec{{Index: auditarchive.EventsIndex(site, at), ID: auditarchive.EventDocID(site, meta.Seq), Doc: doc}}}, nil
}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): build sealed frames and event documents from canonical messages"
```

---

### Task 13: `archive-worker` membership event builder

**Files:**
- Create: `archive-worker/handler_members.go`
- Test: `archive-worker/handler_members_test.go`

**Interfaces:**
- Consumes: `metaOf`, `sealRecord`, `eventAt`, `errPoison`, `errSkip` (Task 12), `model.InboxEvent`, `model.InboxMemberEvent`, `auditarchive.MemberDoc`, `auditarchive.MembersIndex`, `auditarchive.MemberDocID`.
- Produces:

```go
// buildMemberItem turns one INBOX member event into one frame and one document
// per account (room_renamed yields one document with an empty account).
func buildMemberItem(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error)
```

Accepted types: `member_added`, `member_removed`, `room_renamed`. `member_joinedat_refreshed` is `errSkip` (it changes no interval). `RoomSiteID` is the inner event's `SiteID` (the room's site). Empty payload, empty room id, or `member_added`/`member_removed` with zero accounts is `errPoison`.

- [ ] **Step 1: Write the failing test**

`archive-worker/handler_members_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
)

func inboxMsg(t *testing.T, typ string, inner model.InboxMemberEvent, seq uint64) (*fakeMsg, []byte) {
	t.Helper()
	payload, err := json.Marshal(inner)
	require.NoError(t, err)
	outer := model.InboxEvent{Type: typ, SiteID: inner.SiteID, DestSiteID: "site-a", Payload: payload, Timestamp: 1759672800000}
	data, err := json.Marshal(outer)
	require.NoError(t, err)
	return &fakeMsg{subject: "chat.inbox.site-a.external." + typ, data: data, seq: seq, stream: "INBOX-site-a"}, data
}

func TestBuildMemberItem(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	now := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	inner := model.InboxMemberEvent{RoomID: "r9", RoomName: "apac-sales", RoomType: model.RoomTypeChannel, SiteID: "site-b", Accounts: []string{"d.kwan", "e.wong"}, JoinedAt: 1759672800000, Timestamp: 1759672800000}

	t.Run("member_added fans out per account", func(t *testing.T) {
		msg, data := inboxMsg(t, model.InboxMemberAdded, inner, 900)
		it, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		require.Len(t, it.docs, 2)
		ids := []string{it.docs[0].ID, it.docs[1].ID}
		assert.Equal(t, []string{"site-a-900-0", "site-a-900-1"}, ids)
		d := it.docs[1].Doc.(*auditarchive.MemberDoc)
		assert.Equal(t, "e.wong", d.Account)
		assert.Equal(t, "site-b", d.RoomSiteID, "room is archived at its own site")
		assert.Equal(t, "r9", d.RoomID)
		assert.Equal(t, "member_added", d.EventType)
		assert.Equal(t, "audit-members-site-a-2026.10.05", it.docs[1].Index)
		assert.NotEmpty(t, it.frame)
		assert.Equal(t, it.docs[0].Doc.(*auditarchive.MemberDoc).ContentHash, d.ContentHash, "one frame, one hash")
	})
	t.Run("room_renamed yields one doc without account", func(t *testing.T) {
		in := inner
		in.Accounts = nil
		msg, data := inboxMsg(t, model.InboxRoomRenamed, in, 901)
		it, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		require.Len(t, it.docs, 1)
		assert.Empty(t, it.docs[0].Doc.(*auditarchive.MemberDoc).Account)
		assert.Equal(t, "apac-sales", it.docs[0].Doc.(*auditarchive.MemberDoc).RoomName)
	})
	t.Run("joinedat refresh is skipped", func(t *testing.T) {
		msg, data := inboxMsg(t, model.InboxMemberJoinedAtRefreshed, inner, 902)
		_, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errSkip))
	})
	t.Run("member_added with no accounts is poison", func(t *testing.T) {
		in := inner
		in.Accounts = nil
		msg, data := inboxMsg(t, model.InboxMemberAdded, in, 903)
		_, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
	t.Run("empty payload is poison", func(t *testing.T) {
		outer, _ := json.Marshal(model.InboxEvent{Type: model.InboxMemberAdded, Timestamp: 1})
		msg := &fakeMsg{subject: "x", data: outer, seq: 904, stream: "INBOX-site-a"}
		_, err := buildMemberItem(context.Background(), "site-a", msg, outer, c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/handler_members.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
)

func buildMemberItem(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error) {
	meta, err := metaOf(msg)
	if err != nil {
		return item{}, err
	}
	var outer model.InboxEvent
	if err := json.Unmarshal(data, &outer); err != nil {
		return item{}, fmt.Errorf("%w: unmarshal inbox event: %w", errPoison, err)
	}
	switch outer.Type {
	case model.InboxMemberAdded, model.InboxMemberRemoved, model.InboxRoomRenamed:
	case model.InboxMemberJoinedAtRefreshed:
		return item{}, fmt.Errorf("%w: %s", errSkip, outer.Type)
	default:
		return item{}, fmt.Errorf("%w: inbox type %q", errSkip, outer.Type)
	}
	if len(outer.Payload) == 0 {
		return item{}, fmt.Errorf("%w: empty inbox payload", errPoison)
	}
	var inner model.InboxMemberEvent
	if err := json.Unmarshal(outer.Payload, &inner); err != nil {
		return item{}, fmt.Errorf("%w: unmarshal member event: %w", errPoison, err)
	}
	if inner.RoomID == "" {
		return item{}, fmt.Errorf("%w: member event without room id", errPoison)
	}
	if outer.Type != model.InboxRoomRenamed && len(inner.Accounts) == 0 {
		return item{}, fmt.Errorf("%w: %s without accounts", errPoison, outer.Type)
	}
	at := eventAt(outer.Timestamp, now)
	frame, hash, err := sealRecord(site, meta, at, data, c)
	if err != nil {
		return item{}, err
	}
	accounts := inner.Accounts
	if outer.Type == model.InboxRoomRenamed {
		accounts = []string{""}
	}
	index := auditarchive.MembersIndex(site, at)
	docs := make([]docSpec, 0, len(accounts))
	for i, acct := range accounts {
		docs = append(docs, docSpec{Index: index, ID: auditarchive.MemberDocID(site, meta.Seq, i), Doc: &auditarchive.MemberDoc{
			Seq: meta.Seq, EventType: outer.Type, EventAt: at,
			RoomID: inner.RoomID, RoomSiteID: inner.SiteID, Account: acct,
			RoomType: string(inner.RoomType), RoomName: inner.RoomName, ContentHash: hash,
		}})
	}
	return item{ctx: ctx, msg: msg, seq: meta.Seq, frame: frame, docs: docs}, nil
}
```

If `model.RoomTypeChannel` is named differently in `pkg/model`, use the actual constant; `RoomType` is a string type.

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): build member documents from INBOX membership events"
```

---

### Task 14: `archive-worker` flush: PUT, bulk create, settle per message

**Files:**
- Create: `archive-worker/flush.go`
- Test: `archive-worker/flush_test.go`

**Interfaces:**
- Consumes: `sealed`, `item` (Task 11), `objectStore`, `indexStore` (Task 8), `jsretry`, `searchengine.IsBulkItemSuccess`, `IsBulkItemBackpressure`, `IsBulkItemPermanent`, `errcode.Permanent`.
- Produces:

```go
type flushConfig struct { putTimeout, bulkTimeout time.Duration; attempts int; retryWait func(attempt int) time.Duration }
type flusher struct { objects objectStore; index indexStore; cfg flushConfig; metrics *metrics }
func newFlusher(objects objectStore, index indexStore, cfg flushConfig, m *metrics) *flusher
// flush writes the segment, then the documents, then settles every message:
// PUT failure after all attempts NAKs the whole batch; a bulk transport
// failure NAKs the whole batch (the segment stays, a redelivery conflicts);
// per item: all actions 2xx or 409 means Ack, a 429 means NAK with
// BackpressureBackoff, a 400 means Term via errcode.Permanent, anything else
// NAKs with DefaultBackoff.
func (f *flusher) flush(ctx context.Context, s *sealed)
```

`metrics` is defined in Task 17; until then declare `type metrics struct{}` with no-op methods `segments(n int, bytes int)`, `events(outcome string, n int)`, `writeFailure(store string)` in `archive-worker/metrics.go`, and Task 17 fills them in.

- [ ] **Step 1: Write the failing test**

`archive-worker/flush_test.go`:

```go
package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type fakeObjects struct {
	failFirst int
	puts      []string
}

func (f *fakeObjects) Put(_ context.Context, key string, body io.Reader, _ int64, _ string) error {
	if f.failFirst > 0 {
		f.failFirst--
		return errors.New("minio down")
	}
	_, _ = io.ReadAll(body)
	f.puts = append(f.puts, key)
	return nil
}

type fakeIndex struct {
	indexStore
	results []searchengine.BulkResult
	err     error
	calls   int
}

func (f *fakeIndex) Bulk(_ context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.results == nil {
		out := make([]searchengine.BulkResult, len(a))
		for i := range out {
			out[i] = searchengine.BulkResult{Status: 201}
		}
		return out, nil
	}
	return f.results, nil
}

func sealedWith(t *testing.T, seqs ...uint64) *sealed {
	t.Helper()
	items := make([]item, 0, len(seqs))
	for _, s := range seqs {
		it := mkItem(s, 8)
		it.msg = &fakeMsg{seq: s}
		items = append(items, it)
	}
	s, err := seal("site-a", "events", items, time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	return s
}

func cfgFast() flushConfig {
	return flushConfig{putTimeout: time.Second, bulkTimeout: time.Second, attempts: 2, retryWait: func(int) time.Duration { return 0 }}
}

func TestFlush(t *testing.T) {
	ctx := context.Background()
	t.Run("happy path acks everything", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &fakeIndex{}
		s := sealedWith(t, 1, 2)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Equal(t, []string{s.key}, obj.puts)
		for _, it := range s.items {
			assert.True(t, it.msg.(*fakeMsg).acked)
		}
	})
	t.Run("409 is treated as archived", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 409, ErrorType: "version_conflict_engine_exception"}}}
		s := sealedWith(t, 3)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.True(t, s.items[0].msg.(*fakeMsg).acked)
	})
	t.Run("put retried once then succeeds", func(t *testing.T) {
		obj, idx := &fakeObjects{failFirst: 1}, &fakeIndex{}
		s := sealedWith(t, 4)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Len(t, obj.puts, 1)
		assert.True(t, s.items[0].msg.(*fakeMsg).acked)
	})
	t.Run("put exhausted naks the batch and never indexes", func(t *testing.T) {
		obj, idx := &fakeObjects{failFirst: 5}, &fakeIndex{}
		s := sealedWith(t, 5, 6)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Equal(t, 0, idx.calls)
		for _, it := range s.items {
			m := it.msg.(*fakeMsg)
			assert.True(t, m.naked)
			assert.False(t, m.acked)
			assert.Greater(t, m.nakDelay, time.Duration(0), "never a bare nak")
		}
	})
	t.Run("bulk transport failure naks after the segment is written", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &fakeIndex{err: errors.New("es down")}
		s := sealedWith(t, 7)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		assert.Len(t, obj.puts, 1)
		assert.True(t, s.items[0].msg.(*fakeMsg).naked)
	})
	t.Run("429 naks with backpressure backoff", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 429, ErrorType: "es_rejected_execution_exception"}}}
		s := sealedWith(t, 8)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		m := s.items[0].msg.(*fakeMsg)
		assert.True(t, m.naked)
		assert.Equal(t, jsretry.BackpressureBackoff[0], m.nakDelay)
	})
	t.Run("400 terminates the message", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &fakeIndex{results: []searchengine.BulkResult{{Status: 400, ErrorType: "mapper_parsing_exception"}}}
		s := sealedWith(t, 9)
		newFlusher(obj, idx, cfgFast(), &metrics{}).flush(ctx, s)
		m := s.items[0].msg.(*fakeMsg)
		assert.True(t, m.acked || m.termed, "permanent failure must not be redelivered")
		assert.False(t, m.naked)
	})
}
```

`jsretry.SettleQuiet` on a permanent error acks (drops) rather than terms; the test accepts either, and the log line says `disposition=drop`.

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/flush.go`:

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type flushConfig struct {
	putTimeout  time.Duration
	bulkTimeout time.Duration
	attempts    int
	retryWait   func(attempt int) time.Duration
}

func defaultRetryWait(attempt int) time.Duration {
	// short jittered pause between in-process attempts: 500ms, 1s, ...
	return time.Duration(attempt+1) * 500 * time.Millisecond
}

type flusher struct {
	objects objectStore
	index   indexStore
	cfg     flushConfig
	metrics *metrics
}

func newFlusher(objects objectStore, index indexStore, cfg flushConfig, m *metrics) *flusher {
	if cfg.retryWait == nil {
		cfg.retryWait = defaultRetryWait
	}
	return &flusher{objects: objects, index: index, cfg: cfg, metrics: m}
}

func (f *flusher) withAttempts(ctx context.Context, op string, timeout time.Duration, fn func(context.Context) error) error {
	var last error
	for attempt := 0; attempt < f.cfg.attempts; attempt++ {
		actx, cancel := context.WithTimeout(ctx, timeout)
		last = fn(actx)
		cancel()
		if last == nil {
			return nil
		}
		slog.WarnContext(ctx, "archive write attempt failed", "op", op, "attempt", attempt+1, "of", f.cfg.attempts, "error", last)
		if attempt+1 < f.cfg.attempts {
			select {
			case <-time.After(f.cfg.retryWait(attempt)):
			case <-ctx.Done():
				return fmt.Errorf("%s: %w", op, ctx.Err())
			}
		}
	}
	return fmt.Errorf("%s after %d attempts: %w", op, f.cfg.attempts, last)
}

func (f *flusher) flush(ctx context.Context, s *sealed) {
	err := f.withAttempts(ctx, "put segment", f.cfg.putTimeout, func(c context.Context) error {
		return f.objects.Put(c, s.key, bytes.NewReader(s.body), int64(len(s.body)), "application/octet-stream")
	})
	if err != nil {
		f.metrics.writeFailure("bucket")
		f.nakAll(ctx, s.items, "segment put failed: "+err.Error())
		return
	}
	f.metrics.segments(1, len(s.body))

	var results []searchengine.BulkResult
	err = f.withAttempts(ctx, "bulk create", f.cfg.bulkTimeout, func(c context.Context) error {
		var berr error
		results, berr = f.index.Bulk(c, s.actions)
		if berr != nil {
			return berr
		}
		if len(results) != len(s.actions) {
			return fmt.Errorf("%d results for %d actions", len(results), len(s.actions))
		}
		return nil
	})
	if err != nil {
		f.metrics.writeFailure("index")
		f.nakAll(ctx, s.items, "bulk create failed: "+err.Error())
		return
	}

	pos := 0
	for _, it := range s.items {
		var itemErr error
		backoff := jsretry.DefaultBackoff
		for range it.docs {
			r := results[pos]
			pos++
			if itemErr != nil || searchengine.IsBulkItemSuccess(searchengine.ActionCreate, r) {
				continue
			}
			switch {
			case searchengine.IsBulkItemPermanent(r):
				itemErr = errcode.Permanent(errcode.BadRequest(fmt.Sprintf("archive index rejected the document: status %d %s", r.Status, r.ErrorType)))
			case searchengine.IsBulkItemBackpressure(r):
				itemErr, backoff = fmt.Errorf("archive index backpressure: status %d %s", r.Status, r.ErrorType), jsretry.BackpressureBackoff
			default:
				itemErr = fmt.Errorf("archive index item failed: status %d %s", r.Status, r.ErrorType)
			}
			slog.ErrorContext(it.ctx, "archive index item failed", "seq", it.seq, "status", r.Status, "errorType", r.ErrorType)
		}
		if itemErr == nil {
			f.metrics.events("archived", 1)
		} else {
			f.metrics.events("failed", 1)
		}
		jsretry.SettleQuiet(it.ctx, it.msg, backoff, itemErr)
	}
}

func (f *flusher) nakAll(ctx context.Context, items []item, reason string) {
	for _, it := range items {
		jsretry.Nak(it.ctx, it.msg, jsretry.DefaultBackoff, reason)
	}
	f.metrics.events("nak", len(items))
}
```

`archive-worker/metrics.go` placeholder for this task:

```go
package main

type metrics struct{}

func (m *metrics) segments(int, int)      {}
func (m *metrics) events(string, int)     {}
func (m *metrics) writeFailure(string)    {}
func (m *metrics) blobs(string, int64)    {}
func (m *metrics) redelivered(int)        {}
```

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): flush sealed batches with bounded retries and per-message settle"
```

---

### Task 15: `archive-worker` lane loop with fetch, bounds, pipeline and loopguard

**Files:**
- Create: `archive-worker/lane.go`
- Test: `archive-worker/lane_test.go`

**Interfaces:**
- Consumes: `batcher`, `seal` (Task 11), `flusher` (Task 14), `buildEventItem`, `buildMemberItem` (Tasks 12, 13), `loopguard.Guard`, `natsutil.DecodePayload`, `natsutil.Ack`, `jobguard.Guard`.
- Produces:

```go
// Copied from search-sync-worker/consumer_source.go so a raw jetstream.Consumer
// (tests) and the o11y consumer (production) both fit.
type msgFetcher interface { Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) }
type msgBatch interface { Messages() <-chan o11ynats.FetchedMessage }
type rawConsumerAdapter struct{ c jetstream.Consumer }
type o11yConsumerAdapter struct{ c o11ynats.Consumer }

type builder func(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error)

type laneConfig struct { site, name string; fetchBatch int; fillInterval time.Duration; now func() time.Time }
type lane struct { /* cfg, fetcher, build, cipher, batcher, flusher, guard */ }
func newLane(cfg laneConfig, fetcher msgFetcher, build builder, cipher *auditarchive.Cipher, b *batcher, f *flusher, guard *loopguard.Guard) *lane
// run is the consume loop: fetch up to fetchBatch, build each message
// (poison → Term, skip → Ack), add to the batcher, seal+flush when a bound
// trips or the fill interval is due. One flush runs in the background while
// the next batch fills. On stop it drains. A terminal fetch error
// (consumer deleted/not found) reports guard.Stopped(err) and returns.
func (l *lane) run(ctx context.Context, stopCh <-chan struct{}, doneCh chan<- struct{})
```

- [ ] **Step 1: Write the failing test**

`archive-worker/lane_test.go` uses a scripted fetcher that returns one batch of `fakeMsg`s then empty batches, and asserts: a poison message is termed, a skip message is acked, two good messages are sealed into one segment (the fake object store sees one key) and both acked, and that `stopCh` drains a partial batch. Also assert the count bound triggers a flush before the fill interval.

```go
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model"
)

type scriptedFetcher struct {
	batches [][]jetstream.Msg
	i       int
}

type sliceBatch struct{ msgs []jetstream.Msg }

func (b sliceBatch) Messages() <-chan o11ynats.FetchedMessage {
	ch := make(chan o11ynats.FetchedMessage, len(b.msgs))
	for _, m := range b.msgs {
		ch <- o11ynats.FetchedMessage{Ctx: context.Background(), Msg: m}
	}
	close(ch)
	return ch
}

func (f *scriptedFetcher) Fetch(ctx context.Context, _ int, _ ...jetstream.FetchOpt) (msgBatch, error) {
	if f.i < len(f.batches) {
		b := f.batches[f.i]
		f.i++
		return sliceBatch{msgs: b}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return sliceBatch{}, nil
	}
}

func eventMsg(t *testing.T, seq uint64, ev model.MessageEvent) *fakeMsg {
	t.Helper()
	data, err := json.Marshal(ev)
	require.NoError(t, err)
	return &fakeMsg{subject: "chat.msg.canonical.site-a." + string(ev.Event), data: data, seq: seq, stream: "MESSAGES-CANONICAL-site-a"}
}

func TestLane_Run(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	events := loadEvents(t)
	good1 := eventMsg(t, 1, events["created"])
	good2 := eventMsg(t, 2, events["deleted"])
	skip := eventMsg(t, 3, func() model.MessageEvent { e := events["created"]; e.Event = model.EventThreadReplyAdded; return e }())
	poison := &fakeMsg{subject: "x", data: []byte("{"), seq: 4, stream: "MESSAGES-CANONICAL-site-a"}

	obj, idx := &fakeObjects{}, &fakeIndex{}
	fl := newFlusher(obj, idx, cfgFast(), &metrics{})
	b := newBatcher(2, 1<<20, time.Hour) // count bound of 2 trips before the hour
	guard := loopguard.New("test-lane", func() {})
	l := newLane(laneConfig{site: "site-a", name: "events", fetchBatch: 10, fillInterval: time.Hour, now: time.Now},
		&scriptedFetcher{batches: [][]jetstream.Msg{{good1, skip, poison, good2}}}, buildEventItem, c, b, fl, guard)

	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return len(obj.puts) == 1 }, 2*time.Second, 10*time.Millisecond)
	close(stop)
	<-done

	assert.True(t, good1.acked)
	assert.True(t, good2.acked)
	assert.True(t, skip.acked)
	assert.True(t, poison.termed)
	assert.False(t, poison.naked)
	assert.NoError(t, guard.Check().Probe(context.Background()), "a clean stop is not a death")
}

func TestLane_DrainOnStop(t *testing.T) {
	c, _ := auditarchive.NewCipher(testDEK())
	events := loadEvents(t)
	only := eventMsg(t, 1, events["created"])
	obj, idx := &fakeObjects{}, &fakeIndex{}
	l := newLane(laneConfig{site: "site-a", name: "events", fetchBatch: 10, fillInterval: time.Hour, now: time.Now},
		&scriptedFetcher{batches: [][]jetstream.Msg{{only}}}, buildEventItem, c, newBatcher(100, 1<<20, time.Hour),
		newFlusher(obj, idx, cfgFast(), &metrics{}), loopguard.New("test-lane", func() {}))
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	time.Sleep(50 * time.Millisecond)
	close(stop)
	<-done
	assert.Len(t, obj.puts, 1, "partial batch is flushed on shutdown")
	assert.True(t, only.acked)
}

func TestLane_TerminalFetchErrorStopsGuard(t *testing.T) {
	c, _ := auditarchive.NewCipher(testDEK())
	guard := loopguard.New("test-lane", func() {})
	l := newLane(laneConfig{site: "site-a", name: "events", fetchBatch: 10, fillInterval: time.Hour, now: time.Now},
		errFetcher{err: jetstream.ErrConsumerNotFound}, buildEventItem, c, newBatcher(100, 1<<20, time.Hour),
		newFlusher(&fakeObjects{}, &fakeIndex{}, cfgFast(), &metrics{}), guard)
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	<-done
	assert.Error(t, guard.Check().Probe(context.Background()), "readiness fails after a terminal fetch error")
}

type errFetcher struct{ err error }

func (e errFetcher) Fetch(context.Context, int, ...jetstream.FetchOpt) (msgBatch, error) { return nil, e.err }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

`archive-worker/lane.go`:

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type msgFetcher interface {
	Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error)
}

type msgBatch interface {
	Messages() <-chan o11ynats.FetchedMessage
}

type rawConsumerAdapter struct{ c jetstream.Consumer }

func (a rawConsumerAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return rawBatch{b: b, ctx: ctx}, nil
}

type rawBatch struct {
	b   jetstream.MessageBatch
	ctx context.Context
}

func (r rawBatch) Messages() <-chan o11ynats.FetchedMessage {
	out := make(chan o11ynats.FetchedMessage)
	go func() {
		defer close(out)
		for m := range r.b.Messages() {
			out <- o11ynats.FetchedMessage{Ctx: r.ctx, Msg: m}
		}
	}()
	return out
}

type o11yConsumerAdapter struct{ c o11ynats.Consumer }

func (a o11yConsumerAdapter) Fetch(ctx context.Context, n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	return a.c.Fetch(ctx, n, opts...)
}

type builder func(ctx context.Context, site string, msg jetstream.Msg, data []byte, c *auditarchive.Cipher, now time.Time) (item, error)

type laneConfig struct {
	site         string
	name         string
	fetchBatch   int
	fillInterval time.Duration
	now          func() time.Time
}

type lane struct {
	cfg     laneConfig
	fetcher msgFetcher
	build   builder
	cipher  *auditarchive.Cipher
	batcher *batcher
	flusher *flusher
	guard   *loopguard.Guard
	inFlight sync.WaitGroup
	slot     chan struct{} // one background flush at a time
}

func newLane(cfg laneConfig, fetcher msgFetcher, build builder, cipher *auditarchive.Cipher, b *batcher, f *flusher, guard *loopguard.Guard) *lane {
	return &lane{cfg: cfg, fetcher: fetcher, build: build, cipher: cipher, batcher: b, flusher: f, guard: guard, slot: make(chan struct{}, 1)}
}

func terminalFetchErr(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrStreamNotFound)
}

func (l *lane) run(ctx context.Context, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	flushNow := func() {
		items := l.batcher.take()
		if items == nil {
			return
		}
		s, err := seal(l.cfg.site, l.cfg.name, items, l.cfg.now())
		if err != nil {
			slog.ErrorContext(ctx, "seal failed, releasing batch", "lane", l.cfg.name, "error", err)
			l.flusher.nakAll(ctx, items, "seal failed")
			return
		}
		l.slot <- struct{}{}
		l.inFlight.Add(1)
		go func() {
			defer l.inFlight.Done()
			defer func() { <-l.slot }()
			jobguard.Guard("archive flush "+l.cfg.name, func() { l.flusher.flush(ctx, s) })
		}()
	}
	drain := func() { flushNow(); l.inFlight.Wait() }

	for {
		select {
		case <-stopCh:
			drain()
			return
		default:
		}
		batch, err := l.fetcher.Fetch(ctx, l.cfg.fetchBatch, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			select {
			case <-stopCh:
				drain()
				return
			default:
			}
			if terminalFetchErr(err) {
				drain()
				l.guard.Stopped(err)
				return
			}
			if l.batcher.due(l.cfg.now()) {
				flushNow()
			}
			continue
		}
		for fm := range batch.Messages() {
			l.handle(fm.Ctx, fm.Msg)
		}
		if l.batcher.due(l.cfg.now()) {
			flushNow()
		}
	}
}

func (l *lane) handle(ctx context.Context, msg jetstream.Msg) {
	jobguard.Guard("archive build "+l.cfg.name, func() {
		data, err := natsutil.DecodePayload(msg)
		if err != nil {
			slog.ErrorContext(ctx, "undecodable payload, terminating", "lane", l.cfg.name, "subject", msg.Subject(), "error", err)
			if err := msg.Term(); err != nil {
				slog.ErrorContext(ctx, "term failed", "error", err)
			}
			return
		}
		it, err := l.build(ctx, l.cfg.site, msg, data, l.cipher, l.cfg.now())
		switch {
		case errors.Is(err, errSkip):
			natsutil.Ack(msg, "not archived: "+err.Error())
			return
		case errors.Is(err, errPoison):
			slog.ErrorContext(ctx, "poison event, terminating", "lane", l.cfg.name, "subject", msg.Subject(), "error", err)
			if err := msg.Term(); err != nil {
				slog.ErrorContext(ctx, "term failed", "error", err)
			}
			return
		case err != nil:
			slog.ErrorContext(ctx, "build failed, will redeliver", "lane", l.cfg.name, "error", err)
			l.flusher.nakAll(ctx, []item{{ctx: ctx, msg: msg}}, "build failed")
			return
		}
		if l.batcher.add(it, l.cfg.now()) {
			l.flushBound()
		}
	})
}

// flushBound is the count/byte-bound trigger from inside handle; it is the
// same as the loop's flushNow but reachable without a closure.
func (l *lane) flushBound() {
	items := l.batcher.take()
	if items == nil {
		return
	}
	s, err := seal(l.cfg.site, l.cfg.name, items, l.cfg.now())
	if err != nil {
		l.flusher.nakAll(context.Background(), items, "seal failed")
		return
	}
	l.slot <- struct{}{}
	l.inFlight.Add(1)
	go func() {
		defer l.inFlight.Done()
		defer func() { <-l.slot }()
		jobguard.Guard("archive flush "+l.cfg.name, func() { l.flusher.flush(context.Background(), s) })
	}()
}
```

Refactor during the green phase: fold `flushNow` and `flushBound` into one method `l.flushTaken(ctx, items)` so the sealing and the slot logic live once.

- [ ] **Step 4: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS (the race detector is on; the batcher is mutex-guarded and the slot channel bounds the flush goroutine).

```bash
git add archive-worker
git commit -m "feat(archive-worker): fetch loop with bounds, one in-flight flush and loopguard"
```

---

### Task 16: `archive-worker` attachment lane

**Files:**
- Create: `archive-worker/blobs.go`, `archive-worker/blob_source.go`
- Test: `archive-worker/blobs_test.go`

**Interfaces:**
- Consumes: `metaOf`, `errPoison`, `errSkip` (Task 12), `msgFetcher` (Task 15), `objectStore`, `indexStore` (Task 8), `auditarchive.EncryptBlob`, `auditarchive.BlobKey`, `auditarchive.BlobDoc`, `auditarchive.BlobsIndex`, `auditarchive.BlobDocID`, `jsretry.Heartbeat`, `drive.Client.GetGroupImage`.
- Produces:

```go
var errBlobMissing = errors.New("attachment no longer at source")
var errBlobLegacy  = errors.New("legacy MinIO attachment is not archived")
// blobSource opens an attachment's bytes. size is -1 when unknown.
type blobSource interface {
	Open(ctx context.Context, roomID string, att cassandra.Attachment) (body io.ReadCloser, size int64, contentType string, err error)
}
// driveSource resolves the Drive host from the attachment's TitleLink
// (`?drive_host=`) and fetches through pkg/drive exactly as upload-service does.
// A TitleLink of the form api/v1/file-upload/{fileId}/{name} is errBlobLegacy.
type driveSource struct{ client *drive.Client }
func driveHostOf(titleLink string) (host string, legacy bool, err error)

type blobLaneConfig struct { site string; maxBytes int64; chunkBytes int; workers int; ackWait, heartbeatMax time.Duration; now func() time.Time }
type blobLane struct { /* cfg, fetcher, source, objects, index, cipher, guard, metrics */ }
func newBlobLane(cfg blobLaneConfig, fetcher msgFetcher, src blobSource, objects objectStore, index indexStore, c *auditarchive.Cipher, guard *loopguard.Guard, m *metrics) *blobLane
func (l *blobLane) run(ctx context.Context, stopCh <-chan struct{}, doneCh chan<- struct{})
// archiveAttachment is the per-attachment unit of work; exported shape for tests.
func (l *blobLane) archiveAttachment(ctx context.Context, msgID, roomID string, att cassandra.Attachment) error
```

Per message: decode the `created` event; no attachments → Ack. For each attachment: start `jsretry.Heartbeat`, call `archiveAttachment`; a `nil` or `errBlobMissing`/`errBlobLegacy`/size-skip outcome writes a `BlobDoc` (with `Skipped` set to `missing`, `legacy`, or `size`) and counts as done; any other error NAKs the message with `jsretry.DefaultBackoff`. The message is acked only when every attachment is done. A `BlobDoc` create that conflicts is done (archived earlier).

- [ ] **Step 1: Write the failing test**

`archive-worker/blobs_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type fakeSource struct {
	data map[string][]byte
	err  error
}

func (s *fakeSource) Open(_ context.Context, _ string, att cassandra.Attachment) (io.ReadCloser, int64, string, error) {
	if s.err != nil {
		return nil, 0, "", s.err
	}
	b, ok := s.data[att.ID]
	if !ok {
		return nil, 0, "", errBlobMissing
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), att.FileType, nil
}

type recordingIndex struct {
	fakeIndex
	docs []searchengine.BulkAction
}

func (r *recordingIndex) Bulk(ctx context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	r.docs = append(r.docs, a...)
	return r.fakeIndex.Bulk(ctx, a)
}

func TestDriveHostOf(t *testing.T) {
	host, legacy, err := driveHostOf("api/v1/file/rooms/r1/file/f1?drive_host=https://drive.example")
	require.NoError(t, err)
	assert.Equal(t, "https://drive.example", host)
	assert.False(t, legacy)

	_, legacy, err = driveHostOf("api/v1/file-upload/f1/mock.png")
	require.NoError(t, err)
	assert.True(t, legacy)

	_, _, err = driveHostOf("api/v1/file/rooms/r1/file/f1")
	assert.Error(t, err, "missing drive_host")
}

func TestBlobLane_ArchiveAttachment(t *testing.T) {
	c, _ := auditarchive.NewCipher(testDEK())
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }
	plain := bytes.Repeat([]byte{7}, 10_000)
	att := cassandra.Attachment{ID: "f1", Title: "mock.png", FileType: "image/png", TitleLink: "api/v1/file/rooms/r1/file/f1?drive_host=https://drive.example"}

	t.Run("archived, encrypted, documented", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 1, now: now}, nil, &fakeSource{data: map[string][]byte{"f1": plain}}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		require.Equal(t, []string{"site-a/blobs/f1"}, obj.puts)
		require.Len(t, idx.docs, 1)
		assert.Equal(t, searchengine.ActionCreate, idx.docs[0].Action)
		assert.Equal(t, "audit-blobs-site-a", idx.docs[0].Index)
		assert.Equal(t, "site-a-f1", idx.docs[0].DocID)
		var d auditarchive.BlobDoc
		require.NoError(t, json.Unmarshal(idx.docs[0].Doc, &d))
		assert.Equal(t, "site-a/blobs/f1", d.BlobKey)
		assert.Equal(t, auditarchive.HashBytes(plain), d.PlainSHA256)
		assert.Equal(t, int64(len(plain)), d.SizeBytes)
		assert.Equal(t, "image/png", d.ContentType)
		assert.Empty(t, d.Skipped)
	})
	t.Run("over the cap is recorded as skipped size, not uploaded", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 100, chunkBytes: 4096, workers: 1, now: now}, nil, &fakeSource{data: map[string][]byte{"f1": plain}}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Empty(t, obj.puts)
		var d auditarchive.BlobDoc
		require.NoError(t, json.Unmarshal(idx.docs[0].Doc, &d))
		assert.Equal(t, "size", d.Skipped)
	})
	t.Run("missing at source is recorded as skipped missing", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 1, now: now}, nil, &fakeSource{data: map[string][]byte{}}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		var d auditarchive.BlobDoc
		require.NoError(t, json.Unmarshal(idx.docs[0].Doc, &d))
		assert.Equal(t, "missing", d.Skipped)
	})
	t.Run("legacy minio link is recorded as skipped legacy", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		legacy := att
		legacy.TitleLink = "api/v1/file-upload/f1/mock.png"
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 1, now: now}, nil, &fakeSource{err: errBlobLegacy}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", legacy))
		var d auditarchive.BlobDoc
		require.NoError(t, json.Unmarshal(idx.docs[0].Doc, &d))
		assert.Equal(t, "legacy", d.Skipped)
	})
	t.Run("transient source error is returned for a nak", func(t *testing.T) {
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 1, now: now}, nil, &fakeSource{err: errors.New("drive 503")}, &fakeObjects{}, &recordingIndex{}, c, loopguard.New("b", func() {}), &metrics{})
		assert.Error(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
	})
	t.Run("doc create conflict is done", func(t *testing.T) {
		obj := &fakeObjects{}
		idx := &recordingIndex{fakeIndex: fakeIndex{results: []searchengine.BulkResult{{Status: 409}}}}
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 1, now: now}, nil, &fakeSource{data: map[string][]byte{"f1": plain}}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
	})
}

func TestBlobLane_Run(t *testing.T) {
	c, _ := auditarchive.NewCipher(testDEK())
	events := loadEvents(t)
	withAtt := eventMsg(t, 1, events["created"])
	noAtt := eventMsg(t, 2, events["deleted"])
	obj, idx := &fakeObjects{}, &recordingIndex{}
	l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 2, ackWait: time.Minute, heartbeatMax: time.Minute, now: time.Now},
		&scriptedFetcher{batches: [][]jetstream.Msg{{withAtt, noAtt}}}, &fakeSource{data: map[string][]byte{"f1": []byte("png bytes")}}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	require.Eventually(t, func() bool { return withAtt.acked && noAtt.acked }, 2*time.Second, 10*time.Millisecond)
	close(stop)
	<-done
	assert.Equal(t, []string{"site-a/blobs/f1"}, obj.puts)
}
```

Add `"github.com/nats-io/nats.go/jetstream"` to the test imports.

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=archive-worker`
Expected: FAIL to build.

- [ ] **Step 3: Implement the source**

`archive-worker/blob_source.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

var (
	errBlobMissing = errors.New("attachment no longer at source")
	errBlobLegacy  = errors.New("legacy MinIO attachment is not archived")
)

type blobSource interface {
	Open(ctx context.Context, roomID string, att cassandra.Attachment) (io.ReadCloser, int64, string, error)
}

// driveHostOf parses the Drive host out of an attachment link written by
// upload-service: "api/v1/file/rooms/{room}/file/{file}?drive_host={host}".
// The legacy "api/v1/file-upload/{file}/{name}" form points at the upload
// MinIO bucket through a Mongo lookup this worker does not have; it is
// reported as legacy so the lane records it as skipped.
func driveHostOf(titleLink string) (string, bool, error) {
	if strings.HasPrefix(strings.TrimPrefix(titleLink, "/"), "api/v1/file-upload/") {
		return "", true, nil
	}
	u, err := url.Parse(titleLink)
	if err != nil {
		return "", false, fmt.Errorf("parse attachment link: %w", err)
	}
	host := u.Query().Get("drive_host")
	if host == "" {
		return "", false, fmt.Errorf("attachment link has no drive_host")
	}
	return host, false, nil
}

type driveSource struct{ client *drive.Client }

func (s *driveSource) Open(_ context.Context, roomID string, att cassandra.Attachment) (io.ReadCloser, int64, string, error) {
	host, legacy, err := driveHostOf(att.TitleLink)
	if err != nil {
		return nil, 0, "", fmt.Errorf("%w: %w", errBlobMissing, err)
	}
	if legacy {
		return nil, 0, "", errBlobLegacy
	}
	resp, err := s.client.GetGroupImage(host, roomID, att.ID)
	if err != nil {
		if errors.Is(err, drive.ErrHostNotAllowed) {
			return nil, 0, "", fmt.Errorf("%w: %w", errBlobMissing, err)
		}
		return nil, 0, "", fmt.Errorf("drive fetch %s: %w", att.ID, err)
	}
	return resp.Reader, resp.ContentLength, resp.ContentType, nil
}
```

Check `pkg/drive` for how a 404 from Drive surfaces (a sentinel or a status in the error); map that to `errBlobMissing` too, so a deleted file is skipped rather than retried for an hour.

- [ ] **Step 4: Implement the lane**

`archive-worker/blobs.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type blobLaneConfig struct {
	site         string
	maxBytes     int64
	chunkBytes   int
	workers      int
	ackWait      time.Duration
	heartbeatMax time.Duration
	now          func() time.Time
}

type blobLane struct {
	cfg     blobLaneConfig
	fetcher msgFetcher
	source  blobSource
	objects objectStore
	index   indexStore
	cipher  *auditarchive.Cipher
	guard   *loopguard.Guard
	metrics *metrics
}

func newBlobLane(cfg blobLaneConfig, fetcher msgFetcher, src blobSource, objects objectStore, index indexStore, c *auditarchive.Cipher, guard *loopguard.Guard, m *metrics) *blobLane {
	if cfg.chunkBytes <= 0 {
		cfg.chunkBytes = auditarchive.DefaultChunkBytes
	}
	return &blobLane{cfg: cfg, fetcher: fetcher, source: src, objects: objects, index: index, cipher: c, guard: guard, metrics: m}
}

func (l *blobLane) run(ctx context.Context, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	sem := make(chan struct{}, l.cfg.workers)
	var wg sync.WaitGroup
	for {
		select {
		case <-stopCh:
			wg.Wait()
			return
		default:
		}
		batch, err := l.fetcher.Fetch(ctx, l.cfg.workers, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if terminalFetchErr(err) {
				wg.Wait()
				l.guard.Stopped(err)
				return
			}
			continue
		}
		for fm := range batch.Messages() {
			sem <- struct{}{}
			wg.Add(1)
			go func(mctx context.Context, msg jetstream.Msg) {
				defer wg.Done()
				defer func() { <-sem }()
				jobguard.Guard("archive blob", func() { l.handle(mctx, msg) })
			}(fm.Ctx, fm.Msg)
		}
	}
}

func (l *blobLane) handle(ctx context.Context, msg jetstream.Msg) {
	data, err := natsutil.DecodePayload(msg)
	if err != nil {
		slog.ErrorContext(ctx, "undecodable payload, terminating", "lane", "blobs", "error", err)
		_ = msg.Term()
		return
	}
	var ev model.MessageEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.Message.ID == "" {
		slog.ErrorContext(ctx, "poison event, terminating", "lane", "blobs", "error", err)
		_ = msg.Term()
		return
	}
	atts, _ := cassandra.DecodeAttachments(ev.Message.Attachments)
	if len(atts) == 0 {
		natsutil.Ack(msg, "no attachments")
		return
	}
	stop := jsretry.Heartbeat(ctx, msg, jsretry.HeartbeatBudget{Every: jsretry.HeartbeatInterval(l.cfg.ackWait), Max: l.cfg.heartbeatMax})
	defer stop()
	for _, att := range atts {
		if err := l.archiveAttachment(ctx, ev.Message.ID, ev.Message.RoomID, att); err != nil {
			jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "archive attachment "+att.ID+": "+err.Error())
			return
		}
	}
	natsutil.Ack(msg, "attachments archived")
}

// archiveAttachment copies one attachment once. A skip (size, missing,
// legacy) still writes a document so the console can show why.
func (l *blobLane) archiveAttachment(ctx context.Context, msgID, roomID string, att cassandra.Attachment) error {
	doc := auditarchive.BlobDoc{FileID: att.ID, MessageID: msgID, RoomID: roomID, SiteID: l.cfg.site, FileName: att.Title, ContentType: att.FileType, ArchivedAt: l.cfg.now()}
	body, size, ctype, err := l.source.Open(ctx, roomID, att)
	switch {
	case errors.Is(err, errBlobLegacy):
		doc.Skipped = "legacy"
	case errors.Is(err, errBlobMissing):
		doc.Skipped = "missing"
	case err != nil:
		return err
	default:
		defer body.Close()
		if ctype != "" {
			doc.ContentType = ctype
		}
		doc.SizeBytes = size
		if size > l.cfg.maxBytes {
			doc.Skipped = "size"
			break
		}
		limited := &io.LimitedReader{R: body, N: l.cfg.maxBytes + 1}
		var enc bytes.Buffer
		sum, n, err := auditarchive.EncryptBlob(&enc, limited, l.cipher, att.ID, l.cfg.chunkBytes)
		if err != nil {
			return fmt.Errorf("encrypt attachment %s: %w", att.ID, err)
		}
		if n > l.cfg.maxBytes {
			doc.Skipped, doc.SizeBytes = "size", n
			break
		}
		key := auditarchive.BlobKey(l.cfg.site, att.ID)
		if err := l.objects.Put(ctx, key, bytes.NewReader(enc.Bytes()), int64(enc.Len()), "application/octet-stream"); err != nil {
			return fmt.Errorf("put attachment %s: %w", att.ID, err)
		}
		doc.BlobKey, doc.PlainSHA256, doc.SizeBytes, doc.ChunkBytes = key, sum, n, l.cfg.chunkBytes
		l.metrics.blobs("archived", n)
	}
	if doc.Skipped != "" {
		l.metrics.blobs("skipped_"+doc.Skipped, 0)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal blob doc: %w", err)
	}
	results, err := l.index.Bulk(ctx, []searchengine.BulkAction{{Action: searchengine.ActionCreate, Index: auditarchive.BlobsIndex(l.cfg.site), DocID: auditarchive.BlobDocID(l.cfg.site, att.ID), Doc: raw}})
	if err != nil {
		return fmt.Errorf("create blob doc: %w", err)
	}
	if len(results) != 1 || !searchengine.IsBulkItemSuccess(searchengine.ActionCreate, results[0]) {
		return fmt.Errorf("create blob doc: status %d %s", results[0].Status, results[0].ErrorType)
	}
	return nil
}
```

The whole encrypted blob is buffered in memory before the PUT because minio-go needs the size up front for a single-part PUT; with a 100 MiB cap and `ARCHIVE_BLOB_WORKERS` default 4 that is at most 400 MiB per pod, which the README states. Streaming multipart is a follow-up.

- [ ] **Step 5: Run to verify it passes, then commit**

Run: `make test SERVICE=archive-worker && make lint`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): archive Drive attachments on a heartbeat-protected lane"
```

---

### Task 17: `archive-worker` metrics and main wiring

**Files:**
- Modify: `archive-worker/metrics.go` (replace the placeholder), `archive-worker/main.go` (replace the stub)
- Test: `archive-worker/metrics_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces: a runnable binary. `metrics` keeps the method set from Task 14 and records through `otel.Meter("archive-worker")`: `archive_segments_total`, `archive_segment_bytes` (histogram), `archive_events_total{outcome}`, `archive_write_failures_total{store}`, `archive_blobs_total{outcome}`, `archive_blob_bytes_total`, `archive_redeliveries_total`.

- [ ] **Step 1: Write the failing metrics test**

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)
	// With the global no-op meter these must not panic.
	m.segments(1, 10)
	m.events("archived", 2)
	m.writeFailure("bucket")
	m.blobs("archived", 5)
	m.redelivered(1)
}
```

- [ ] **Step 2: Run it to verify it fails, then implement metrics.go**

```go
package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	segmentsTotal  metric.Int64Counter
	segmentBytes   metric.Int64Histogram
	eventsTotal    metric.Int64Counter
	writeFailures  metric.Int64Counter
	blobsTotal     metric.Int64Counter
	blobBytesTotal metric.Int64Counter
	redeliveries   metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	meter := otel.Meter("archive-worker")
	var m metrics
	var err error
	if m.segmentsTotal, err = meter.Int64Counter("archive_segments_total"); err != nil {
		return nil, fmt.Errorf("metric archive_segments_total: %w", err)
	}
	if m.segmentBytes, err = meter.Int64Histogram("archive_segment_bytes"); err != nil {
		return nil, fmt.Errorf("metric archive_segment_bytes: %w", err)
	}
	if m.eventsTotal, err = meter.Int64Counter("archive_events_total"); err != nil {
		return nil, fmt.Errorf("metric archive_events_total: %w", err)
	}
	if m.writeFailures, err = meter.Int64Counter("archive_write_failures_total"); err != nil {
		return nil, fmt.Errorf("metric archive_write_failures_total: %w", err)
	}
	if m.blobsTotal, err = meter.Int64Counter("archive_blobs_total"); err != nil {
		return nil, fmt.Errorf("metric archive_blobs_total: %w", err)
	}
	if m.blobBytesTotal, err = meter.Int64Counter("archive_blob_bytes_total"); err != nil {
		return nil, fmt.Errorf("metric archive_blob_bytes_total: %w", err)
	}
	if m.redeliveries, err = meter.Int64Counter("archive_redeliveries_total"); err != nil {
		return nil, fmt.Errorf("metric archive_redeliveries_total: %w", err)
	}
	return &m, nil
}

func (m *metrics) segments(n, bytes int) {
	if m == nil || m.segmentsTotal == nil {
		return
	}
	m.segmentsTotal.Add(context.Background(), int64(n))
	m.segmentBytes.Record(context.Background(), int64(bytes))
}

func (m *metrics) events(outcome string, n int) {
	if m == nil || m.eventsTotal == nil {
		return
	}
	m.eventsTotal.Add(context.Background(), int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
}

func (m *metrics) writeFailure(store string) {
	if m == nil || m.writeFailures == nil {
		return
	}
	m.writeFailures.Add(context.Background(), 1, metric.WithAttributes(attribute.String("store", store)))
}

func (m *metrics) blobs(outcome string, bytes int64) {
	if m == nil || m.blobsTotal == nil {
		return
	}
	m.blobsTotal.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	if bytes > 0 {
		m.blobBytesTotal.Add(context.Background(), bytes)
	}
}

func (m *metrics) redelivered(n int) {
	if m == nil || m.redeliveries == nil {
		return
	}
	m.redeliveries.Add(context.Background(), int64(n))
}
```

The nil-guards keep `&metrics{}` usable in tests. Add the redelivery count in `lane.handle` and `blobLane.handle`: after `metaOf`/`Metadata()`, `if md.NumDelivered > 1 { metrics.redelivered(1) }`.

- [ ] **Step 3: Write main.go**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/health"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/obs"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func main() {
	if err := run(); err != nil {
		slog.Error("archive-worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if w := checkBatchAckCoupling(cfg.BatchEvents, cfg.Consumer.MaxAckPending, cfg.Replicas); w != "" {
		slog.Warn("batch/ack-pending coupling", "detail", w)
	}

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}
	m, err := newMetrics()
	if err != nil {
		return err
	}

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: cfg.SearchBackend, URL: cfg.SearchURL, Username: cfg.SearchUsername, Password: cfg.SearchPassword, TLSSkipVerify: cfg.SearchTLSSkipVerify}, searchengine.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect archive search: %w", err)
	}
	if err := bootstrapIndex(ctx, engine, cfg.SiteID, cfg.IndexRetention, cfg.DevMode); err != nil {
		return err
	}

	lockClient, err := minio.New(cfg.S3Endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""), Secure: cfg.S3UseSSL})
	if err != nil {
		return fmt.Errorf("archive bucket client: %w", err)
	}
	if err := checkObjectLock(ctx, lockClient, cfg.Bucket, cfg.RequireObjectLock); err != nil {
		return err
	}
	objClient, err := minioutil.Connect(ctx, cfg.S3Endpoint, cfg.S3UseSSL, cfg.S3AccessKey, cfg.S3SecretKey, minioutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("connect archive bucket: %w", err)
	}
	objects := newBucketSink(objClient, cfg.Bucket)

	wrapper, err := atrest.NewVaultKeyWrapper(ctx, cfg.Vault)
	if err != nil {
		return fmt.Errorf("vault wrapper: %w", err)
	}
	dek, err := loadOrCreateDEK(ctx, engine, wrapper, cfg.SiteID, time.Now)
	if err != nil {
		return err
	}
	cipher, err := auditarchive.NewCipher(dek)
	if err != nil {
		return err
	}

	nc, err := natsutil.Connect(ctx, cfg.NatsURL, cfg.NatsCredsFile, sdk.TracerProvider(), sdk.Propagator, sdk.Toggles.Trace)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		return err
	}

	sig := shutdown.Signals() // armed before any loop starts

	flushCfg := flushConfig{putTimeout: cfg.PutTimeout, bulkTimeout: cfg.BulkTimeout, attempts: cfg.WriteAttempts}
	mkConsumer := func(streamName, durable string, filters []string, settings stream.ConsumerSettings) (msgFetcher, error) {
		cc := stream.DurableConsumerDefaults(stream.WithUnlimitedRedelivery(settings))
		cc.Durable = durable
		cc.FilterSubjects = filters
		cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cc)
		if err != nil {
			return nil, fmt.Errorf("consumer %s on %s: %w", durable, streamName, err)
		}
		return o11yConsumerAdapter{c: cons}, nil
	}

	eventsGuard := loopguard.New("events-lane", loopguard.SelfShutdown)
	membersGuard := loopguard.New("members-lane", loopguard.SelfShutdown)
	blobsGuard := loopguard.New("blobs-lane", loopguard.SelfShutdown)

	eventsFetcher, err := mkConsumer(stream.MessagesCanonical(cfg.SiteID).Name, "archive-worker-events", []string{subject.MsgCanonicalMessageWildcard(cfg.SiteID)}, cfg.Consumer)
	if err != nil {
		return err
	}
	membersFetcher, err := mkConsumer(stream.Inbox(cfg.SiteID).Name, "archive-worker-members", subject.InboxMemberEventSubjects(cfg.SiteID), cfg.Consumer)
	if err != nil {
		return err
	}

	laneCfg := func(name string) laneConfig {
		return laneConfig{site: cfg.SiteID, name: name, fetchBatch: cfg.FetchBatch, fillInterval: cfg.FillInterval, now: time.Now}
	}
	events := newLane(laneCfg("events"), eventsFetcher, buildEventItem, cipher, newBatcher(cfg.BatchEvents, cfg.BatchBytes, cfg.FillInterval), newFlusher(objects, engine, flushCfg, m), eventsGuard)
	members := newLane(laneCfg("members"), membersFetcher, buildMemberItem, cipher, newBatcher(cfg.BatchEvents, cfg.BatchBytes, cfg.FillInterval), newFlusher(objects, engine, flushCfg, m), membersGuard)

	stopCh := make(chan struct{})
	var doneChs []chan struct{}
	start := func(run func(context.Context, <-chan struct{}, chan<- struct{})) {
		done := make(chan struct{})
		doneChs = append(doneChs, done)
		go run(ctx, stopCh, done)
	}
	start(events.run)
	start(members.run)

	checks := []health.Check{natsutil.HealthCheck(nc), eventsGuard.Check(), membersGuard.Check()}
	if cfg.BlobsEnabled {
		blobSettings := cfg.Consumer
		blobSettings.AckWait = cfg.BlobAckWait
		blobSettings = stream.WithOutageRetryBudget(blobSettings, nil)
		blobsFetcher, err := mkConsumer(stream.MessagesCanonical(cfg.SiteID).Name, "archive-worker-blobs", []string{subject.MsgCanonicalCreated(cfg.SiteID)}, blobSettings)
		if err != nil {
			return err
		}
		cfg.Drive.LoadBaseURLs()
		blobs := newBlobLane(blobLaneConfig{site: cfg.SiteID, maxBytes: cfg.BlobMaxBytes, chunkBytes: auditarchive.DefaultChunkBytes, workers: cfg.BlobWorkers, ackWait: cfg.BlobAckWait, heartbeatMax: cfg.Consumer.HeartbeatMax, now: time.Now},
			blobsFetcher, &driveSource{client: drive.NewClient(&cfg.Drive)}, objects, engine, cipher, blobsGuard, m)
		start(blobs.run)
		checks = append(checks, blobsGuard.Check())
	}

	healthStop, err := health.ServeWithPprof(cfg.HealthAddr, 5*time.Second, cfg.PProfEnabled, checks...)
	if err != nil {
		return fmt.Errorf("health server: %w", err)
	}
	slog.Info("archive-worker started", "site", cfg.SiteID, "bucket", cfg.Bucket, "blobs", cfg.BlobsEnabled)

	shutdown.WaitOn(ctx, sig, 25*time.Second,
		func(context.Context) error {
			eventsGuard.BeginShutdown()
			membersGuard.BeginShutdown()
			blobsGuard.BeginShutdown()
			return nil
		},
		func(context.Context) error { close(stopCh); return nil },
		func(ctx context.Context) error {
			for _, ch := range doneChs {
				select {
				case <-ch:
				case <-ctx.Done():
					return fmt.Errorf("lane drain timed out: %w", ctx.Err())
				}
			}
			return nil
		},
		func(ctx context.Context) error { return natsutil.Drain(ctx, nc) },
		func(context.Context) error { return wrapper.Close() },
		func(ctx context.Context) error { return healthStop(ctx) },
		func(ctx context.Context) error { return obsShutdown(ctx) },
	)
	return nil
}
```

`stream.WithOutageRetryBudget(blobSettings, nil)` is the schedule-less form only if the function accepts nil; if it requires a schedule, pass `jsretry.DefaultBackoff`. Check `pkg/stream/consumer.go` and use whichever compiles.

- [ ] **Step 4: Build, lint, test, commit**

Run: `make build SERVICE=archive-worker && make lint && make test SERVICE=archive-worker`
Expected: PASS.

```bash
git add archive-worker
git commit -m "feat(archive-worker): wire lanes, metrics, health and shutdown"
```

---

### Task 18: End-to-end integration test with NATS, Elasticsearch, MinIO and Vault

**Files:**
- Create: `archive-worker/integration_test.go`

**Interfaces:**
- Consumes: everything in `archive-worker` through the same constructors `main.go` uses, with `rawConsumerAdapter` over a plain `jetstream.Consumer`, `testutil.NATS`, `testutil.Elasticsearch`, `testutil.MinIOEndpoint`, `testutil.Vault`, `lockedBucket` (Task 10).

Scenario: create the two streams, push templates and policy, bootstrap the DEK through a real Vault wrapper, start the events and members lanes with a 1-second fill, publish three canonical events (created with an attachment, updated, deleted) and one `member_added` for two accounts on the INBOX internal lane, and assert:

1. Exactly one `events-*` segment and one `members-*` segment appear in the locked bucket, parse with `auditarchive.ReadSegment`, and every frame opens with the DEK and hashes to the `contentHash` of its document.
2. The events index has three documents with ids `site-x-{seq}`, the members index has two, each `segmentKey`/`frameOffset` resolves with `auditarchive.ReadFrameAt` to the right frame.
3. The consumer's `NumAckPending` is 0 and `NumPending` is 0 for both durables.
4. Forcing a redelivery (publish the same three events again under a fresh consumer, or call `consumer.Info()` after a second run with `DeliverPolicy` reset) yields a second segment and no new documents: the document count stays three and the bulk results were 409s.
5. A write or delete with a `create_doc`-only role is refused: create a role `audit-writer` with `{"indices":[{"names":["audit-*"],"privileges":["create_doc","auto_configure"]}]}` through the security API when the test cluster has security enabled; if `testutil.Elasticsearch` runs without security (it does today, `xpack.security.enabled=false`), skip this subtest with `t.Skip("security disabled in the test cluster")` and leave the role as an ops-documented step in the README.

Key helpers to write in the test file: `startES(t) searchengine.SearchEngine`, `publishJSON(t, js, subject string, v any)`, `countDocs(t, engine, index string) int` (via `engine.Search` with `{"query":{"match_all":{}},"size":0,"track_total_hits":true}` and reading `hits.total.value`), `listKeys(t, client, bucket, prefix) []string`.

- [ ] **Step 1: Write the test skeleton with the assertions above as `require`s against not-yet-wired lanes, run `make test-integration SERVICE=archive-worker`, and watch it fail on the first assertion (no segment).**
- [ ] **Step 2: Wire the lanes exactly as `main.go` does, using `rawConsumerAdapter{c: cons}` from a `jetstream.CreateOrUpdateConsumer` on the raw client, and the `staticWrapper` from Task 8 is NOT used here: use `atrest.NewVaultKeyWrapper(ctx, atrest.VaultConfig{Address: v.Address, TransitMount: v.TransitMount, TransitKey: v.TransitKey, Token: v.Token})` with `v := testutil.Vault(t, ctx)`.**
- [ ] **Step 3: Run until green. Use `require.Eventually` with a 20-second budget for the segment to appear, since the fill interval is 1 second and Elasticsearch refresh is 5 seconds (pass `refresh=true` on `countDocs` by searching with `?refresh` or call `POST /{index}/_refresh` through a small raw request helper; `engine.Search` does not refresh).**
- [ ] **Step 4: Add the bucket-outage subtest: construct the events lane with a `fakeObjects{failFirst: 100}` wrapped around the real sink (a `failingThen` decorator that fails N calls then delegates), publish one event, assert after 3 seconds that `NumAckPending` is 0 and `NumRedelivered` is at least 1 on the durable and no document exists, then set the decorator to pass and assert the document appears.**
- [ ] **Step 5: Commit**

```bash
git add archive-worker/integration_test.go
git commit -m "test(archive-worker): end-to-end archive, verify and redelivery against real containers"
```

---

### Task 19: Local infrastructure: compose include, locked bucket, audit Vault key

**Files:**
- Modify: `docker-local/compose.services.yaml` (include + o11y anchor)
- Modify: `docker-local/compose.deps.yaml` (vault-init key, new `minio-init`)
- Modify: `docker-local/README.md` (one paragraph)

- [ ] **Step 1: Add the include and service anchor**

In `docker-local/compose.services.yaml`, add `- ../archive-worker/deploy/docker-compose.yml` under `include:` next to the search-sync-worker line, and `archive-worker: *local-o11y` under `services:`.

- [ ] **Step 2: Create the audit key and the locked bucket in the init profile**

In `docker-local/compose.deps.yaml`, in `vault-init`'s script add after the `chat-kek` line:

```sh
        vault write -f transit/keys/chat-audit-kek
```

Add a new service after `vault-init`:

```yaml
  minio-init:
    image: pgsty/minio:RELEASE.2026-08-04T00-00-00Z
    container_name: chat-local-minio-init
    profiles: ["init"]
    depends_on:
      minio:
        condition: service_healthy
    restart: "no"
    entrypoint:
      - /bin/sh
      - -c
      - |
        set -e
        mc alias set local http://minio:9000 minioadmin minioadmin
        mc mb --with-lock --ignore-existing local/archive-site-local
        mc retention set --default COMPLIANCE 1d local/archive-site-local
        mc mb --with-lock --ignore-existing local/archive-site-remote
        mc retention set --default COMPLIANCE 1d local/archive-site-remote
    networks:
      - chat-local
```

If the image has no `mc` binary, use `minio/mc:RELEASE.2025-04-08T15-39-49Z` for this one service and note it in the README beside the pgsty note.

- [ ] **Step 3: Document**

Add to `docker-local/README.md`: the init profile now also creates `chat-audit-kek` and the locked `archive-<site>` buckets with a 1-day compliance default; objects cannot be deleted for a day, so `docker compose down -v` is the way to reset the archive locally.

- [ ] **Step 4: Validate and commit**

Run: `docker compose -f docker-local/compose.deps.yaml config -q && docker compose -f docker-local/compose.services.yaml config -q`
Expected: both exit 0.

```bash
git add docker-local
git commit -m "chore(docker-local): archive worker service, audit Vault key and locked buckets"
```

---

### Task 20: `auth-service` dev-mode guard (spec §2)

**Files:**
- Modify: `auth-service/main.go:26-36` (config), `auth-service/main.go:85-95` (dev branch)
- Modify: `auth-service/deploy/docker-compose.yml:18`
- Test: `auth-service/config_test.go` (new)

**Interfaces:**
- Produces: `config.DevModeLocalOnlyAck bool \`env:"DEV_MODE_LOCAL_ONLY_ACK" envDefault:"false"\`` and `func (c config) validateDevMode() error`.

- [ ] **Step 1: Write the failing test**

`auth-service/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ValidateDevMode(t *testing.T) {
	assert.NoError(t, config{DevMode: false}.validateDevMode())
	assert.NoError(t, config{DevMode: true, DevModeLocalOnlyAck: true}.validateDevMode())
	err := config{DevMode: true, DevModeLocalOnlyAck: false}.validateDevMode()
	assert.ErrorContains(t, err, "DEV_MODE_LOCAL_ONLY_ACK")
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test SERVICE=auth-service`
Expected: FAIL to build.

- [ ] **Step 3: Implement**

Add the field to `config` and:

```go
// validateDevMode refuses the unauthenticated JWT path unless the deployment
// explicitly declares itself local. A production manifest that copies
// DEV_MODE=true by mistake exits instead of minting tokens for any account.
func (c config) validateDevMode() error {
	if c.DevMode && !c.DevModeLocalOnlyAck {
		return fmt.Errorf("DEV_MODE=true requires DEV_MODE_LOCAL_ONLY_ACK=true; dev mode mints unauthenticated JWTs and is only for local environments")
	}
	return nil
}
```

Call it in `run()` right after `env.ParseAs[config]()` succeeds: `if err := cfg.validateDevMode(); err != nil { return err }`. In `auth-service/deploy/docker-compose.yml` add `- DEV_MODE_LOCAL_ONLY_ACK=true` under the `DEV_MODE` line.

- [ ] **Step 4: Run, lint, commit**

Run: `make test SERVICE=auth-service && make lint`
Expected: PASS.

```bash
git add auth-service
git commit -m "feat(auth-service): refuse DEV_MODE without an explicit local-only acknowledgement"
```

---

### Task 21: Per-service NATS credentials for the worker (spec §2, local parity)

**Files:**
- Modify: `docker-local/setup.sh` (after the `backend` user block, around line 101)
- Modify: `archive-worker/deploy/docker-compose.yml` (creds path)
- Modify: `archive-worker/README.md` (the permission list, for ops)

The spec's full per-service split is an ops change; this task adds the one user this PR introduces so the worker never runs on the `>` credential, and records the exact permission set ops needs for production.

- [ ] **Step 1: Add the user in `setup.sh`**

After the `backend` user lines:

```sh
    # archive-worker: pull consumers on MESSAGES-CANONICAL and INBOX only.
    # No chat.user.> publish right: this worker never calls history.
    nsc add user --account chatapp --name archive-worker
    nsc edit user --account chatapp --name archive-worker \
      --allow-pub '$JS.API.INFO' \
      --allow-pub '$JS.API.STREAM.INFO.MESSAGES-CANONICAL-*' \
      --allow-pub '$JS.API.STREAM.INFO.INBOX-*' \
      --allow-pub '$JS.API.STREAM.CREATE.MESSAGES-CANONICAL-*' \
      --allow-pub '$JS.API.STREAM.UPDATE.MESSAGES-CANONICAL-*' \
      --allow-pub '$JS.API.CONSUMER.CREATE.MESSAGES-CANONICAL-*.>' \
      --allow-pub '$JS.API.CONSUMER.CREATE.INBOX-*.>' \
      --allow-pub '$JS.API.CONSUMER.DURABLE.CREATE.MESSAGES-CANONICAL-*.>' \
      --allow-pub '$JS.API.CONSUMER.DURABLE.CREATE.INBOX-*.>' \
      --allow-pub '$JS.API.CONSUMER.INFO.MESSAGES-CANONICAL-*.>' \
      --allow-pub '$JS.API.CONSUMER.INFO.INBOX-*.>' \
      --allow-pub '$JS.API.CONSUMER.MSG.NEXT.MESSAGES-CANONICAL-*.>' \
      --allow-pub '$JS.API.CONSUMER.MSG.NEXT.INBOX-*.>' \
      --allow-pub '$JS.ACK.MESSAGES-CANONICAL-*.>' \
      --allow-pub '$JS.ACK.INBOX-*.>' \
      --allow-sub '_INBOX.>'
    nsc generate creds --account chatapp --name archive-worker > /output/archive-worker.creds
```

The `STREAM.CREATE`/`UPDATE` rights exist only for the local `BOOTSTRAP_STREAMS=true` path; the README lists the production set without them. The setup script's comment block warns that the whole `sh -c` argument is single-quoted; these lines use single quotes inside a double-quoted outer argument in the existing style of the file, so check how the file quotes `$JS` elsewhere and match it.

- [ ] **Step 2: Point the compose file at the new creds**

In `archive-worker/deploy/docker-compose.yml` change the default creds to `/etc/nats/archive-worker.creds` and the volume to `../../docker-local/archive-worker.creds`.

- [ ] **Step 3: Verify the script still parses and the worker still consumes locally**

Run: `bash -n docker-local/setup.sh`, then if Docker is available locally, `make up SERVICE=archive-worker` after `docker-local` init and watch the worker log `archive-worker started`. If Docker is not available in the execution environment, say so in the task report rather than claiming it ran.

- [ ] **Step 4: Commit**

```bash
git add docker-local/setup.sh archive-worker
git commit -m "chore: dedicated NATS credentials for archive-worker"
```

---

### Task 22: Spec and documentation amendments

**Files:**
- Modify: `docs/superpowers/specs/2026-09-29-message-audit-access-design.md`
- Modify: `docs/architecture.md` (one bullet under the workers list)

Record the four decisions this plan made that differ from the spec text, so the audit-service PR builds against what was shipped:

- [ ] **Step 1: In §4 "Segment format", change the key to `{site}/{yyyy}/{mm}/{dd}/{hh}/{lane}-{firstSeq}-{lastSeq}.seg` and add one sentence: the lane (`events` or `members`) is in the key because the two lanes read different streams with independent sequence spaces.**
- [ ] **Step 2: In §4 "Index shape", remove `roomType` from the events document (the canonical message event does not carry it; the fold takes it from the members index), note that `audit-members` documents are per account with id `{site}-{seq}-{i}`, and replace "stripping it is done by the lifecycle policy reindexing" with "stripping is a scheduled reindex job owned by the audit-service PR; ILM has no reindex action. The policy sets each daily index read-only at `min_age: 1d` and deletes it at `ARCHIVE_INDEX_RETENTION`."**
- [ ] **Step 3: In §4 "Attachment lane", add: legacy MinIO-hosted attachments (`api/v1/file-upload/...`) are recorded as `skipped: legacy` because resolving them needs the upload MongoDB lookup this worker deliberately does not have; and the encrypted blob is buffered in memory before its single PUT, bounded by `ARCHIVE_BLOB_MAX_BYTES × ARCHIVE_BLOB_WORKERS` per pod.**
- [ ] **Step 4: In §9, replace `ARCHIVE_INDEX_BODY_RETENTION` in the worker's list with `ARCHIVE_INDEX_RETENTION`, and add `ARCHIVE_REQUIRE_OBJECT_LOCK`, `ARCHIVE_REPLICAS`, `ARCHIVE_FETCH_BATCH`, `ARCHIVE_BLOBS_ENABLED`, `ARCHIVE_BLOB_WORKERS`.**
- [ ] **Step 5: In `docs/architecture.md`, add `archive-worker` to the JetStream workers list with one line: "copies canonical message, membership and attachment events into the per-site audit archive (Object Lock bucket + append-only Elasticsearch index)".**
- [ ] **Step 6: Commit**

```bash
git add docs
git commit -m "docs: record the archive-worker decisions in the audit design spec"
```

---

## Execution order and checkpoints

Tasks 1 to 6 are `pkg/` work with no service and can be reviewed as one unit. Tasks 7 to 17 build the worker bottom-up; each leaves `make test SERVICE=archive-worker` green. Task 18 is the first time real containers are involved. Tasks 19 to 22 are infrastructure, hardening and documentation and do not depend on each other.

Not in this plan, by design: the CCS hub, remote-cluster registration and cross-cluster API keys (spec §4 "Cross-cluster search") are read-side and ops work that lands with the audit-service PR; the body-stripping reindex job (spec §4) likewise. This plan leaves every index and object exactly as that PR expects to read them.

Before the branch is handed over: `make lint`, `make test`, `make test-integration SERVICE=archive-worker`, `make test-integration SERVICE=pkg/searchengine`, and `make sast` must all pass, and `docs/reviews/` must be empty.
