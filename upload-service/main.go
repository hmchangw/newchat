package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/gin-gonic/gin"

	o11ygin "github.com/flywindy/o11y/gin"

	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/minioutil"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/obs"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/shutdown"
)

type config struct {
	Port    string `env:"PORT"      envDefault:"8080"`
	DevMode bool   `env:"DEV_MODE"  envDefault:"false"`
	SiteID  string `env:"SITE_ID,required"`

	// CORSAllowedOrigins is the credentialed-CORS allowlist. Empty (default) emits no
	// CORS headers. Comma-separated exact origins, e.g. "https://app.example.com".
	CORSAllowedOrigins []string `env:"CORS_ALLOWED_ORIGINS" envSeparator:"," envDefault:""`

	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB"        envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"  envDefault:""`
	MongoPassword string `env:"MONGO_PASSWORD"  envDefault:""`

	// MaxImages caps the number of images per image-upload request.
	MaxImages int `env:"MAX_IMAGES" envDefault:"10"`
	// MaxAttachments caps the number of files the single-file upload endpoint accepts.
	MaxAttachments int `env:"MAX_ATTACHMENTS" envDefault:"1"`
	// MaxImageSizeBytes is the per-image upload ceiling (default 25 MiB).
	MaxImageSizeBytes int64 `env:"MAX_IMAGE_SIZE_BYTES" envDefault:"26214400"`

	// FileUploadMaxFileSize is the single-file upload ceiling (default 100 MiB; -1 = unlimited).
	FileUploadMaxFileSize int64 `env:"FILE_UPLOAD_MAX_FILE_SIZE" envDefault:"104857600"`
	// FileUploadMediaTypeWhitelist/Blacklist gate the file endpoint's MIME types.
	FileUploadMediaTypeWhitelist string `env:"FILE_UPLOAD_MEDIA_TYPE_WHITELIST" envDefault:""`
	FileUploadMediaTypeBlacklist string `env:"FILE_UPLOAD_MEDIA_TYPE_BLACKLIST" envDefault:"image/svg+xml"`
	// FileDownloadCacheMaxAgeSeconds is the Cache-Control max-age (seconds) on the
	// MinIO/S3 download response (default 1 year).
	FileDownloadCacheMaxAgeSeconds int `env:"FILE_DOWNLOAD_CACHE_MAX_AGE_SECONDS" envDefault:"31536000"`

	OIDCIssuerURL string   `env:"OIDC_ISSUER_URL"`
	OIDCAudiences []string `env:"OIDC_AUDIENCES" envSeparator:","`
	TLSSkipVerify bool     `env:"TLS_SKIP_VERIFY" envDefault:"false"`

	MinioEndpoint  string `env:"MINIO_ENDPOINT,required"`
	MinioAccessKey string `env:"MINIO_ACCESS_KEY,required"`
	MinioSecretKey string `env:"MINIO_SECRET_KEY,required"`
	MinioUseSSL    bool   `env:"MINIO_USE_SSL" envDefault:"false"`
	MinioBucket    string `env:"MINIO_BUCKET"`
	// MinioDownloadTimeout bounds a single MinIO/S3 download (Stat probe + streamed body).
	MinioDownloadTimeout time.Duration `env:"MINIO_DOWNLOAD_TIMEOUT" envDefault:"5m"`

	Drive drive.Config `envPrefix:"DRIVE_"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	ctx := context.Background()
	cfg.Drive.LoadBaseURLs()

	sdk, obsShutdown, err := obs.Init(ctx)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	mongoClient, err := mongoutil.Connect(ctx, cfg.MongoURI, cfg.MongoUsername, cfg.MongoPassword, mongoutil.WithObservability(sdk))
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}
	store := NewMongoStore(mongoClient.Database(cfg.MongoDB))
	driveClient := drive.NewClient(&cfg.Drive)

	minioClient, err := minioutil.Connect(ctx, cfg.MinioEndpoint, cfg.MinioUseSSL, cfg.MinioAccessKey, cfg.MinioSecretKey)
	if err != nil {
		return fmt.Errorf("minio connect: %w", err)
	}
	bucket := cfg.MinioBucket
	if bucket == "" {
		bucket = "chat-" + cfg.SiteID
	}
	s3Store := newMinioObjectStore(minioClient, bucket, cfg.MinioDownloadTimeout)

	var validator TokenValidator
	if !cfg.DevMode {
		if cfg.OIDCIssuerURL == "" || len(cfg.OIDCAudiences) == 0 {
			return fmt.Errorf("OIDC_ISSUER_URL and OIDC_AUDIENCES are required when DEV_MODE is false")
		}
		v, err := pkgoidc.NewValidator(ctx, pkgoidc.Config{
			IssuerURL:     cfg.OIDCIssuerURL,
			Audiences:     cfg.OIDCAudiences,
			TLSSkipVerify: cfg.TLSSkipVerify,
		})
		if err != nil {
			return fmt.Errorf("create oidc validator: %w", err)
		}
		validator = v
	}

	mimeFilter := newMediaTypeFilter(cfg.FileUploadMediaTypeWhitelist, cfg.FileUploadMediaTypeBlacklist)
	handler := NewHandler(store, driveClient, s3Store, cfg.MaxImages, cfg.MaxAttachments, cfg.MaxImageSizeBytes,
		cfg.FileUploadMaxFileSize, mimeFilter, imagePreview, cfg.FileDownloadCacheMaxAgeSeconds)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// CORS handles preflight before tracing so OPTIONS noise does not pollute Tempo.
	r.Use(corsMiddleware(cfg.CORSAllowedOrigins))
	// o11y server-span middleware wraps real requests so downstream slog/handlers
	// are trace-correlated.
	r.Use(o11ygin.Middleware("upload-service", sdk.TracerProvider(), sdk.MeterProvider(), sdk.Propagator, o11ygin.WithSkipPaths())...)
	r.Use(gin.Recovery())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	registerRoutes(r, handler, validator, cfg.DevMode)

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute, // downloads stream potentially-large bodies
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("upload service starting", "addr", addr, "site", cfg.SiteID)
		srvErr <- srv.ListenAndServe()
	}()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shutdown.Wait(ctx, 25*time.Second,
			func(ctx context.Context) error { return srv.Shutdown(ctx) },
			func(ctx context.Context) error { mongoutil.Disconnect(ctx, mongoClient); return nil },
			// obsShutdown LAST so all prior teardown telemetry is exported.
			func(ctx context.Context) error { return obsShutdown(ctx) },
		)
	}()

	err = <-srvErr
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen upload server: %w", err)
	}
	<-shutdownDone
	return nil
}
