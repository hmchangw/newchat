// Package minioutil is a typed JSON-blob wrapper around minio-go/v7.
package minioutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	o11yminio "github.com/flywindy/o11y/minio"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
)

// ObjectStore is the subset of the MinIO client API that Bucket uses. Both the
// plain *minio.Client and the instrumented *o11yminio.Client satisfy it, so a
// Bucket transparently emits o11y spans when constructed over an instrumented
// client (whose data-plane methods are traced overrides).
type ObjectStore interface {
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
}

// Bucket binds an ObjectStore to a single bucket. Goroutine-safe.
type Bucket[T any] struct {
	client ObjectStore
	name   string
}

// NewBucket fails fast if the bucket does not exist. Does not create it.
func NewBucket[T any](ctx context.Context, client ObjectStore, name string) (*Bucket[T], error) {
	exists, err := client.BucketExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("minioutil bucket exists check %q: %w", name, err)
	}
	if !exists {
		return nil, fmt.Errorf("minioutil bucket %q does not exist", name)
	}
	return &Bucket[T]{client: client, name: name}, nil
}

// Raw is the escape hatch for features the wrapper does not surface.
func (b *Bucket[T]) Raw() ObjectStore { return b.client }

func (b *Bucket[T]) Name() string { return b.name }

// Put marshals v as JSON with Content-Type application/json; charset=utf-8.
func (b *Bucket[T]) Put(ctx context.Context, key string, v T) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("minioutil put %s/%s marshal: %w", b.name, key, err)
	}
	_, err = b.client.PutObject(ctx, b.name, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
		ContentType: "application/json; charset=utf-8",
	})
	if err != nil {
		return fmt.Errorf("minioutil put %s/%s: %w", b.name, key, err)
	}
	return nil
}

// Get returns (nil, nil) when the key does not exist (matches Collection.FindOne).
func (b *Bucket[T]) Get(ctx context.Context, key string) (*T, error) {
	obj, err := b.client.GetObject(ctx, b.name, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("minioutil get %s/%s: %w", b.name, key, err)
	}
	defer obj.Close()

	// Stat first: minio-go's GetObject is lazy, so NoSuchKey only surfaces here.
	if _, err := obj.Stat(); err != nil {
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && minioErr.Code == "NoSuchKey" {
			return nil, nil
		}
		return nil, fmt.Errorf("minioutil get %s/%s stat: %w", b.name, key, err)
	}

	var v T
	if err := json.NewDecoder(obj).Decode(&v); err != nil {
		return nil, fmt.Errorf("minioutil get %s/%s decode: %w", b.name, key, err)
	}
	return &v, nil
}

const defaultListCap = 1000

// List returns up to maxKeys keys with the prefix; maxKeys<=0 -> 1000. Use Raw() for unbounded scans.
func (b *Bucket[T]) List(ctx context.Context, prefix string, maxKeys int) ([]string, error) {
	if maxKeys <= 0 {
		maxKeys = defaultListCap
	}
	// defer cancel() is load-bearing: breaking out of the range without it leaks the channel-fill goroutine.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	keys := make([]string, 0)
	for obj := range b.client.ListObjects(ctx, b.name, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
		MaxKeys:   maxKeys,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("minioutil list %s prefix=%q: %w", b.name, prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
		if len(keys) >= maxKeys {
			break
		}
	}
	return keys, nil
}

func (b *Bucket[T]) Delete(ctx context.Context, key string) error {
	if err := b.client.RemoveObject(ctx, b.name, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("minioutil delete %s/%s: %w", b.name, key, err)
	}
	return nil
}

// Connect does not probe; NewBucket's BucketExists is the bucket-scoped fail-fast hook.
// When WithObservability is supplied the returned client is an instrumented
// *o11yminio.Client; otherwise it is a plain *minio.Client. Both satisfy
// ObjectStore, so NewBucket accepts either.
func Connect(_ context.Context, endpoint string, useSSL bool, accessKey, secretKey string, opts ...Option) (ObjectStore, error) {
	cc := newConnectConfig(opts...)
	minioOpts := &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	}

	if cc.obs != nil {
		// Propagator comes from the OTel global (obs.Init installs sdk.Propagator
		// there) rather than the Observability interface, matching o11y's object-
		// store examples. It is inert for an object-store client anyway — the peer
		// never extracts traceparent — so spans still nest via ctx.
		client, err := o11yminio.New(endpoint, minioOpts, cc.obs.TracerProvider(), cc.obs.MeterProvider(), otel.GetTextMapPropagator())
		if err != nil {
			return nil, fmt.Errorf("minioutil connect: %w", err)
		}
		slog.Info("connected to MinIO", "endpoint", endpoint, "useSSL", useSSL)
		return client, nil
	}

	client, err := minio.New(endpoint, minioOpts)
	if err != nil {
		return nil, fmt.Errorf("minioutil connect: %w", err)
	}
	slog.Info("connected to MinIO", "endpoint", endpoint, "useSSL", useSSL)
	return client, nil
}
