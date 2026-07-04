package atrest

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	vault "github.com/hashicorp/vault/api"
	authapprole "github.com/hashicorp/vault/api/auth/approle"
	authk8s "github.com/hashicorp/vault/api/auth/kubernetes"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tokenLease is one authenticated token plus the means to observe and
// release it: done fires when the token's lifetime watcher ends (renewal
// hit max_ttl, renewal failed across the grace window, or the token is
// non-renewable and near expiry), and stop tears the watcher down.
type tokenLease struct {
	done <-chan error
	stop func()
}

// leaseFunc authenticates to Vault and returns a fresh tokenLease. It is
// the re-authentication primitive the maintain loop calls whenever the
// current token dies. Implemented by vaultLease for real auth; faked in
// tests.
type leaseFunc func(ctx context.Context) (tokenLease, error)

// backoffFunc maps a 1-based failed-attempt count to a wait before the
// next re-login try.
type backoffFunc func(attempt int) time.Duration

// vaultLease builds a leaseFunc that logs in via the given auth method and
// starts a lifetime watcher for the issued token. Re-reading the credential
// happens inside method.Login (the AppRole helper re-reads its secret-ID
// file on each call), so a rotated secret ID is picked up automatically on
// every re-auth.
func vaultLease(client *vault.Client, method vault.AuthMethod) leaseFunc {
	return func(ctx context.Context) (tokenLease, error) {
		secret, err := client.Auth().Login(ctx, method)
		if err != nil {
			return tokenLease{}, fmt.Errorf("login: %w", err)
		}
		if secret == nil || secret.Auth == nil {
			return tokenLease{}, errors.New("login returned empty auth")
		}
		watcher, err := client.NewLifetimeWatcher(&vault.LifetimeWatcherInput{Secret: secret})
		if err != nil {
			return tokenLease{}, fmt.Errorf("lifetime watcher: %w", err)
		}
		go watcher.Start()
		return tokenLease{done: watcher.DoneCh(), stop: watcher.Stop}, nil
	}
}

// maintainToken keeps a long-lived process authenticated to Vault. It
// watches the current token's lifetime; when that ends — for any reason —
// it discards the dead token and authenticates again for a fresh one.
// Renewal (handled by the lifetime watcher inside each lease) keeps a token
// alive cheaply up to its max_ttl; this loop covers everything renewal
// cannot — hitting max_ttl, a renewal window missed during a Vault outage,
// or a non-renewable token — by re-logging-in. AppRole/Kubernetes
// credentials are reusable, so recovery needs no operator action. The loop
// runs until ctx is cancelled (via the wrapper's Close).
func maintainToken(ctx context.Context, initial tokenLease, lease leaseFunc, backoff backoffFunc) {
	current := initial
	for {
		select {
		case <-ctx.Done():
			current.stop()
			return
		case <-current.done:
			current.stop()
		}

		next, ok := reauth(ctx, lease, backoff)
		if !ok {
			return // context cancelled while re-authenticating
		}
		current = next
	}
}

// reauth retries lease until it succeeds, backing off between failures, and
// returns the new lease. The bool is false when ctx is cancelled first (the
// caller should then exit). Each failure increments kekRenewalFailures and
// logs, so a sustained inability to obtain a token is visible to operators
// while the old, now-expired token makes every Wrap/Unwrap fail.
func reauth(ctx context.Context, lease leaseFunc, backoff backoffFunc) (tokenLease, bool) {
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return tokenLease{}, false
		}
		next, err := lease(ctx)
		if err == nil {
			// Debug, not Info: a successful re-auth is a routine event that
			// recurs roughly every token max_ttl (e.g. every ~20 min), so it
			// would otherwise be steady log noise. The failure path below
			// stays at Error — that's the signal operators act on.
			slog.Debug("atrest: vault re-authenticated for a fresh token")
			return next, true
		}
		if ctx.Err() != nil {
			return tokenLease{}, false // cancelled mid-login; not a real failure
		}
		kekRenewalFailures.Inc()
		slog.Error("atrest: vault re-authentication failed; retrying", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return tokenLease{}, false
		case <-time.After(backoff(attempt)):
		}
	}
}

// defaultBackoff is exponential (1s, 2s, 4s, …) capped at 30s, with a guard
// against a non-positive attempt and against shift overflow.
func defaultBackoff(attempt int) time.Duration {
	const base = time.Second
	const maxDelay = 30 * time.Second
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 30 {
		return maxDelay
	}
	d := base << (attempt - 1)
	if d > maxDelay {
		return maxDelay
	}
	return d
}

// VaultConfig configures the Vault transit-engine KeyWrapper. It is
// parsed via caarlos0/env in each consuming service.
type VaultConfig struct {
	// Address is the Vault server URL (e.g. https://vault.svc:8200).
	Address string `env:"VAULT_ADDR" envDefault:""`

	// TransitMount is the path the transit secrets engine is mounted at.
	TransitMount string `env:"ATREST_VAULT_TRANSIT_MOUNT" envDefault:"transit"`

	// TransitKey is the named key under that mount used to wrap DEKs
	// (e.g. "chat-kek").
	TransitKey string `env:"ATREST_VAULT_TRANSIT_KEY" envDefault:"chat-kek"`

	// K8sRole is the Vault role to log in as via Kubernetes auth. When
	// set, the service uses its mounted ServiceAccount token to obtain a
	// Vault token. Leave empty for non-k8s deployments and pair with
	// Token.
	K8sRole string `env:"VAULT_K8S_ROLE" envDefault:""`

	// K8sAuthPath is the auth method's mount path (default "kubernetes").
	K8sAuthPath string `env:"VAULT_K8S_AUTH_PATH" envDefault:"kubernetes"`

	// AppRoleID is the RoleID for AppRole auth. When set (and K8sRole is
	// empty), the service logs in via AppRole using the secret ID read from
	// AppRoleSecretIDFile. Suited to non-Kubernetes production deployments.
	AppRoleID string `env:"VAULT_APPROLE_ROLE_ID" envDefault:""`

	// AppRoleSecretIDFile is the path to a file holding the AppRole secret
	// ID. File-based delivery keeps the secret out of the process
	// environment (and out of child processes, crash dumps, and `kubectl
	// describe`), and lets an orchestrator rotate it without a restart — the
	// helper re-reads the file on each login. Required when AppRoleID is set.
	AppRoleSecretIDFile string `env:"VAULT_APPROLE_SECRET_ID_FILE" envDefault:""`

	// AppRoleAuthPath is the AppRole auth method's mount path (default
	// "approle").
	AppRoleAuthPath string `env:"VAULT_APPROLE_AUTH_PATH" envDefault:"approle"`

	// Token is the static Vault token used when no other auth method is
	// selected. Suitable for local docker-compose only; production should
	// use Kubernetes or AppRole auth.
	Token string `env:"VAULT_TOKEN" envDefault:""`
}

// vaultKeyWrapper wraps DEKs using Vault's transit secrets engine.
type vaultKeyWrapper struct {
	client       *vault.Client
	transitMount string
	transitKey   string

	// cancel stops the background maintainToken loop; nil when using a
	// static token (no loop). loopDone is closed when that loop exits, so
	// Close can wait for the lifetime watcher to be torn down.
	cancel   context.CancelFunc
	loopDone chan struct{}
}

// Close stops the background token-maintenance loop if Kubernetes or
// AppRole auth is in use and waits for it to release its lifetime watcher.
// Safe to call repeatedly; safe to call when the wrapper was constructed
// with a static token.
func (w *vaultKeyWrapper) Close() error {
	if w.cancel != nil {
		w.cancel()
		<-w.loopDone
	}
	return nil
}

// startTokenMaintenance performs the initial synchronous login (so a bad
// credential fails the constructor rather than surfacing later) and then
// launches the background maintainToken loop that re-authenticates on
// expiry. It returns the cancel func and done channel for Close to drive.
func startTokenMaintenance(ctx context.Context, lease leaseFunc) (context.CancelFunc, chan struct{}, error) {
	initial, err := lease(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("initial token lease: %w", err)
	}
	// The loop outlives the constructor's ctx, so give it its own.
	loopCtx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		maintainToken(loopCtx, initial, lease, defaultBackoff)
	}()
	return cancel, loopDone, nil
}

// NewVaultKeyWrapper constructs a KeyWrapper backed by Vault's transit
// engine. It selects an auth method by precedence: Kubernetes auth when
// cfg.K8sRole is set, then AppRole when cfg.AppRoleID is set, then a static
// cfg.Token. Returns an error if the resulting client cannot reach Vault or
// has no usable credentials.
func NewVaultKeyWrapper(ctx context.Context, cfg VaultConfig) (*vaultKeyWrapper, error) { //nolint:revive,gocritic // unexported return is intentional (callers consume via KeyWrapper); hugeParam: cfg passed by value matches the project's caarlos0/env config-struct convention
	if cfg.Address == "" {
		return nil, errors.New("vault: VAULT_ADDR is required")
	}
	if cfg.TransitKey == "" {
		return nil, errors.New("vault: ATREST_VAULT_TRANSIT_KEY is required")
	}

	vcfg := vault.DefaultConfig()
	vcfg.Address = cfg.Address
	client, err := vault.NewClient(vcfg)
	if err != nil {
		return nil, fmt.Errorf("vault: new client: %w", err)
	}

	// Each auth-method branch builds a leaseFunc; the shared
	// startTokenMaintenance call below performs the initial login and starts
	// the re-authentication loop. The static-token branch leaves lease nil —
	// a static token has nothing to renew or re-issue.
	var lease leaseFunc
	switch {
	case cfg.K8sRole != "":
		k8sAuth, err := authk8s.NewKubernetesAuth(cfg.K8sRole, authk8s.WithMountPath(cfg.K8sAuthPath))
		if err != nil {
			return nil, fmt.Errorf("vault: configure kubernetes auth: %w", err)
		}
		lease = vaultLease(client, k8sAuth)
	case cfg.AppRoleID != "":
		// The secret ID is the sensitive half of the AppRole credential and
		// is only ever sourced from a file — never an env var — so it stays
		// out of the process environment. The helper reads the file lazily
		// at each login, which also makes secret-ID rotation a file rewrite
		// rather than a restart.
		if cfg.AppRoleSecretIDFile == "" {
			return nil, errors.New("vault: VAULT_APPROLE_SECRET_ID_FILE is required when VAULT_APPROLE_ROLE_ID is set")
		}
		// Only override the mount path when explicitly set — an empty value
		// would build a broken "auth//login" path. Leaving the option off
		// lets the helper apply its own "approle" default, matching our
		// envDefault.
		var opts []authapprole.LoginOption
		if cfg.AppRoleAuthPath != "" {
			opts = append(opts, authapprole.WithMountPath(cfg.AppRoleAuthPath))
		}
		appRoleAuth, err := authapprole.NewAppRoleAuth(
			cfg.AppRoleID,
			&authapprole.SecretID{FromFile: cfg.AppRoleSecretIDFile},
			opts...,
		)
		if err != nil {
			return nil, fmt.Errorf("vault: configure approle auth: %w", err)
		}
		lease = vaultLease(client, appRoleAuth)
	case cfg.Token != "":
		client.SetToken(cfg.Token)
		// Validate the token + connectivity at construction time so a
		// misconfigured deploy fails closed at startup rather than at the
		// first Wrap/Unwrap call. Matches the login branches above which
		// fail immediately on a bad credential.
		if _, err := client.Auth().Token().LookupSelfWithContext(ctx); err != nil {
			return nil, fmt.Errorf("vault: validate static token: %w", err)
		}
	default:
		return nil, errors.New("vault: one of VAULT_K8S_ROLE, VAULT_APPROLE_ROLE_ID, or VAULT_TOKEN must be set")
	}

	w := &vaultKeyWrapper{
		client:       client,
		transitMount: cfg.TransitMount,
		transitKey:   cfg.TransitKey,
	}
	if lease != nil {
		cancel, loopDone, err := startTokenMaintenance(ctx, lease)
		if err != nil {
			return nil, fmt.Errorf("vault: %w", err)
		}
		w.cancel = cancel
		w.loopDone = loopDone
	}
	return w, nil
}

// GenerateDataKey asks Vault to mint a fresh 256-bit DEK via the transit
// engine's wrapped datakey endpoint and returns both the plaintext DEK and
// its KEK-wrapped form. The DEK is generated inside Vault (the HSM, in
// compliant deployments), so the plaintext material originates there rather
// than in this process — the property our security review asked for.
//
// The wrapped endpoint returns only the KEK-wrapped DEK (no plaintext), so
// the plaintext is recovered with a follow-up decrypt. This deliberately
// avoids the datakey/plaintext capability: callers need only
// datakey/wrapped + decrypt, the same decrypt that Unwrap already requires.
func (w *vaultKeyWrapper) GenerateDataKey(ctx context.Context) (plaintext, wrapped []byte, err error) {
	defer func() { kekWrapCounter.WithLabelValues(resultLabel(err)).Inc() }()
	ctx, span := w.startTransitSpan(ctx, "vault.transit.datakey")
	defer func() { finishVaultSpan(span, err) }()

	resp, err := w.client.Logical().WriteWithContext(ctx,
		fmt.Sprintf("%s/datakey/wrapped/%s", w.transitMount, w.transitKey),
		map[string]any{"bits": 256},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("vault transit datakey: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return nil, nil, errors.New("vault transit datakey: empty response")
	}
	ct, ok := resp.Data["ciphertext"].(string)
	if !ok || ct == "" {
		return nil, nil, errors.New("vault transit datakey: missing ciphertext")
	}
	wrapped = []byte(ct)
	dek, err := w.decryptDEK(ctx, wrapped)
	if err != nil {
		return nil, nil, fmt.Errorf("vault transit datakey decrypt: %w", err)
	}
	return dek, wrapped, nil
}

// Wrap encrypts the DEK via Vault's transit engine and returns the
// "vault:vN:..." ciphertext bytes.
func (w *vaultKeyWrapper) Wrap(ctx context.Context, dek []byte) (out []byte, err error) {
	defer func() { kekWrapCounter.WithLabelValues(resultLabel(err)).Inc() }()
	ctx, span := w.startTransitSpan(ctx, "vault.transit.encrypt")
	defer func() { finishVaultSpan(span, err) }()

	resp, err := w.client.Logical().WriteWithContext(ctx,
		fmt.Sprintf("%s/encrypt/%s", w.transitMount, w.transitKey),
		map[string]any{
			"plaintext": base64.StdEncoding.EncodeToString(dek),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("vault transit encrypt: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return nil, errors.New("vault transit encrypt: empty response")
	}
	ct, ok := resp.Data["ciphertext"].(string)
	if !ok || ct == "" {
		return nil, errors.New("vault transit encrypt: missing ciphertext")
	}
	return []byte(ct), nil
}

// Unwrap decrypts a "vault:vN:..." ciphertext via Vault's transit engine
// and returns the plaintext DEK.
func (w *vaultKeyWrapper) Unwrap(ctx context.Context, ciphertext []byte) (out []byte, err error) {
	defer func() { kekUnwrapCounter.WithLabelValues(resultLabel(err)).Inc() }()
	return w.decryptDEK(ctx, ciphertext)
}

// decryptDEK decrypts a "vault:vN:..." transit ciphertext back to the
// plaintext DEK. It carries no metric of its own; the public callers (Unwrap,
// and the wrapped-datakey path in GenerateDataKey) each record exactly one
// operation around it. It does create its own child span so Vault failures show
// the exact transit operation that failed.
func (w *vaultKeyWrapper) decryptDEK(ctx context.Context, ciphertext []byte) (out []byte, err error) {
	ctx, span := w.startTransitSpan(ctx, "vault.transit.decrypt")
	defer func() { finishVaultSpan(span, err) }()

	resp, err := w.client.Logical().WriteWithContext(ctx,
		fmt.Sprintf("%s/decrypt/%s", w.transitMount, w.transitKey),
		map[string]any{
			"ciphertext": string(ciphertext),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("vault transit decrypt: %w", err)
	}
	if resp == nil || resp.Data == nil {
		return nil, errors.New("vault transit decrypt: empty response")
	}
	b64, ok := resp.Data["plaintext"].(string)
	if !ok || b64 == "" {
		return nil, errors.New("vault transit decrypt: missing plaintext")
	}
	dek, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("vault transit decrypt: base64 decode: %w", err)
	}
	return dek, nil
}

func (w *vaultKeyWrapper) startTransitSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("vault.transit.mount", w.transitMount),
			attribute.String("vault.transit.key", w.transitKey),
		),
	)
}

func finishVaultSpan(span trace.Span, err error) {
	defer span.End()
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
