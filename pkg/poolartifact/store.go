package poolartifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// StoreConfig locates the object store the pool artifact travels through:
// the producer writes it there, every consumer pod reads it back.
//
// Declared here rather than in each tool because both ends read it, and the
// two must agree on bucket and prefix or the consumer reads an object that
// does not exist. Nothing here has a safe default except the layout knobs —
// a half-configured store pointed at the wrong bucket would surface as an
// empty pool hours into a run, so Validate refuses one.
type StoreConfig struct {
	Endpoint  string `env:"POOL_S3_ENDPOINT"`
	AccessKey string `env:"POOL_S3_ACCESS_KEY"`
	SecretKey string `env:"POOL_S3_SECRET_KEY"`
	Bucket    string `env:"POOL_S3_BUCKET"`
	// Prefix keeps pool artifacts in their own keyspace when the bucket is
	// shared with something else. A dedicated bucket is still the better
	// answer: the artifact is a list of real accounts.
	Prefix string `env:"POOL_S3_PREFIX" envDefault:"clientsim"`
	UseSSL bool   `env:"POOL_S3_USE_SSL" envDefault:"true"`
}

// Configured reports whether the store is in use at all. Both tools accept a
// file path instead, so an unset store is a valid configuration — only a
// PARTLY set one is an error.
func (c *StoreConfig) Configured() bool {
	return c.Endpoint != "" || c.AccessKey != "" || c.SecretKey != "" || c.Bucket != ""
}

// Validate names the missing env var rather than the struct field, because
// the operator setting it is reading a chart, not this source.
func (c *StoreConfig) Validate() error {
	var missing []string
	for _, f := range []struct {
		env, val string
	}{
		{"POOL_S3_ENDPOINT", c.Endpoint},
		{"POOL_S3_ACCESS_KEY", c.AccessKey},
		{"POOL_S3_SECRET_KEY", c.SecretKey},
		{"POOL_S3_BUCKET", c.Bucket},
	} {
		if f.val == "" {
			missing = append(missing, f.env)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("pool object store is missing %s", strings.Join(missing, ", "))
	}
	return nil
}

// objects is the slice of an object store this package needs.
//
// Defined here rather than reusing minioutil.ObjectStore because that one
// returns *minio.Object — a concrete type a test cannot fabricate, which
// would force every store test through a container.
type objects interface {
	// put overwrites. ifAbsent makes it a conditional create — the write
	// fails with ErrObjectExists rather than replacing an existing object.
	put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, ifAbsent bool) error
	get(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

// ErrObjectNotFound distinguishes "nothing published yet" from a broken
// store. The exporter treats the first as a normal first run and anything
// else as a failure, and it cannot tell them apart from an opaque string.
var ErrObjectNotFound = errors.New("pool object not found")

// ErrObjectExists reports a lost create-if-absent race: another writer claimed
// the key first. The loser must reconcile against what is actually stored, not
// overwrite it.
var ErrObjectExists = errors.New("pool object already exists")

// contentTypeFor follows the key, exactly as compression does, so the two
// cannot disagree: a plain-JSON manifest labelled application/gzip is
// unparseable to anything reading object metadata.
func contentTypeFor(key string) string {
	if isGzipPath(key) {
		return "application/gzip"
	}
	return "application/json"
}

// Store reads and writes pool artifacts in an object store.
type Store struct {
	objects objects
	bucket  string
	prefix  string
}

type minioObjects struct{ c *minio.Client }

func (m minioObjects) put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, ifAbsent bool) error {
	opts := minio.PutObjectOptions{ContentType: contentType}
	if ifAbsent {
		// If-None-Match: * — the server rejects the write if the key exists,
		// so the claim IS the write. A Load-then-Put check cannot do this:
		// two exporters both read "absent" and both go on to write.
		opts.SetMatchETagExcept("*")
	}
	_, err := m.c.PutObject(ctx, bucket, key, r, size, opts)
	if err != nil {
		return fmt.Errorf("put %s/%s: %w", bucket, key, asAlreadyExists(err))
	}
	return nil
}

// asAlreadyExists maps the conditional write's rejection onto the package
// sentinel. Same errors.As discipline as asNotFound, and for the same reason.
func asAlreadyExists(err error) error {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) &&
		(resp.Code == "PreconditionFailed" || resp.StatusCode == http.StatusPreconditionFailed) {
		return fmt.Errorf("%w: %s", ErrObjectExists, err)
	}
	return err
}

func (m minioObjects) get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	obj, err := m.c.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", bucket, key, asNotFound(err))
	}
	return obj, nil
}

// asNotFound maps the object store's own missing-key code onto the package
// sentinel. GetObject is lazy — it does not reach the server until the first
// read — so this has to be applied on the read path too.
//
// errors.As, not minio.ToErrorResponse: the latter is a plain type switch
// (minio-go v7 api-error-response.go) and cannot see an ErrorResponse through
// the %w wrapping readCapped applies on the read path. Classifying with it
// silently failed to recognise a missing object, and the first pool-export of
// any run ID then died on its own overwrite guard.
func asNotFound(err error) error {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) && resp.Code == "NoSuchKey" {
		return fmt.Errorf("%w: %s", ErrObjectNotFound, err)
	}
	return err
}

// NewStore connects to the configured object store.
func NewStore(cfg *StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("connect pool object store: %w", err)
	}
	return newStore(minioObjects{c: c}, cfg.Bucket, cfg.Prefix), nil
}

func newStore(o objects, bucket, prefix string) *Store {
	return &Store{objects: o, bucket: bucket, prefix: prefix}
}

// Key is the run-scoped layout: <prefix>/<siteID>/<runID>/<name>. Scoping by
// run is what lets the store double as the record of which accounts a given
// run connected as, months after the pods are gone.
// keySegmentPattern is the shape a single object-key segment may take. It
// mirrors loadgen's run-ID rule so the two halves of one path cannot disagree
// about what is safe.
var keySegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// PlaintextEndpoint reports a store access that crosses a network
// unencrypted. Loopback is excluded: that is the documented local MinIO
// workflow, and a warning that fires on every local run is one operators learn
// to skip — which would cost the deployed case the visibility it exists for.
//
// It lives here rather than in either tool because the exposure is the same at
// both ends: pool-export uploads the account list, clientsim downloads it, and
// both carry the store's access key ID.
func (c *StoreConfig) PlaintextEndpoint() bool {
	if c.UseSSL {
		return false
	}
	host := c.Endpoint
	if h, _, err := net.SplitHostPort(c.Endpoint); err == nil {
		host = h
	}
	if host == "localhost" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return false
	}
	return true
}

// ValidateKeySegment rejects anything that would not survive path.Join as one
// literal segment. Key joins prefix/siteID/runID/name, and path.Join
// NORMALISES "..", so an unchecked segment can write the artifact into another
// site's scope or out of the prefix entirely — silently overwriting a pool a
// fleet may already be reading. Both segments come from deployment config, so
// this catches a typo or a copied env rather than an attacker; the damage is
// the same either way.
func ValidateKeySegment(s string) error {
	// No explicit "."/".." case: the pattern requires an alphanumeric first
	// rune, so neither can match.
	if !keySegmentPattern.MatchString(s) {
		return fmt.Errorf("%q is not a valid object-key segment: it must match %s ",
			s, keySegmentPattern.String())
	}
	return nil
}

func (s *Store) Key(siteID, runID, name string) string {
	return path.Join(s.prefix, siteID, runID, name)
}

// Put validates the artifact exactly as Write does — a producer must not be
// able to publish something the consumer will refuse — then stores it,
// gzipped when the key says .gz.
func (s *Store) Put(ctx context.Context, key string, a *Artifact) error {
	return s.putArtifact(ctx, key, a, false)
}

// PutIfAbsent claims the key with the write itself, returning ErrObjectExists
// if another writer got there first. A run ID names one immutable population,
// and a Load-then-Put check cannot enforce that: two exporters both read the
// run as unpublished and both write, so a fleet mid-run can be reading one
// population while pods that restart read the other — overlapping or disjoint
// shards, every pod still reporting ready.
func (s *Store) PutIfAbsent(ctx context.Context, key string, a *Artifact) error {
	return s.putArtifact(ctx, key, a, true)
}

func (s *Store) putArtifact(ctx context.Context, key string, a *Artifact, ifAbsent bool) error {
	// Marshal (and its validation) before the claim, so a rejected artifact
	// never burns a run ID.
	data, err := marshalArtifact(a)
	if err != nil {
		return err
	}
	if isGzipPath(key) {
		if data, err = gzipBytes(data); err != nil {
			return fmt.Errorf("compress pool artifact: %w", err)
		}
	}
	return s.objects.put(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), contentTypeFor(key), ifAbsent)
}

// PutJSON stores an arbitrary JSON document beside the artifact — the export
// manifest, which records HOW a pool was produced. It carries no artifact
// validation because it is not an artifact: nothing reads it back to connect
// with, it exists so a run can be explained months later.
func (s *Store) PutJSON(ctx context.Context, key string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	if isGzipPath(key) {
		if data, err = gzipBytes(data); err != nil {
			return fmt.Errorf("compress %s: %w", key, err)
		}
	}
	return s.objects.put(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), contentTypeFor(key), false)
}

// Load fetches and validates an artifact, applying the same caps as the file
// path — including the decompressed-byte cap, which is what keeps a hostile
// or corrupt object from expanding into the consumer's heap.
func (s *Store) Load(ctx context.Context, key, wantSiteID string) (*Artifact, error) {
	r, err := s.objects.get(ctx, s.bucket, key)
	if err != nil {
		return nil, err
	}
	defer r.Close() //nolint:errcheck // read-only handle
	data, err := readCapped(r, isGzipPath(key))
	if err != nil {
		return nil, asNotFound(err)
	}
	return decodeAndValidate(data, wantSiteID)
}

// ParsePoolURL splits an s3://bucket/key URL. The bucket travels with the URL
// so one env var names the whole location; StoreConfig.Bucket is the fallback
// for callers that address objects by key alone.
func ParsePoolURL(raw string) (bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("parse pool URL %q: %w", raw, err)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("pool URL %q must use the s3:// scheme, got %q", raw, u.Scheme)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("pool URL %q names no bucket", raw)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("pool URL %q names no object key", raw)
	}
	return u.Host, key, nil
}
