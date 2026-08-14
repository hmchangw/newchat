package main

import (
	"context"
	"fmt"
	"log/slog"

	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/service"
)

// oidcValidator builds the SSO token validator/refresher, or returns (nil, nil, nil)
// when OIDC is unconfigured so sso.set/sso.refresh reply unavailable.
func oidcValidator(ctx context.Context, cfg *config.Config) (service.TokenValidator, service.TokenRefresher, error) {
	if cfg.OIDCIssuerURL == "" {
		slog.Warn("OIDC_ISSUER_URL not set — sso.set/sso.refresh will reply unavailable")
		return nil, nil, nil
	}
	if cfg.TLSSkipVerify {
		slog.Warn("OIDC issuer TLS verification is OFF (dev only)")
	}
	if cfg.OIDCSkipIssuerCheck {
		slog.Warn("OIDC issuer check is OFF — tokens are accepted from any hostname serving this realm's keys",
			"issuer", cfg.OIDCIssuerURL)
	}
	v, err := pkgoidc.NewValidator(ctx, pkgoidc.Config{
		IssuerURL:       cfg.OIDCIssuerURL,
		Audiences:       cfg.OIDCAudiences,
		TLSSkipVerify:   cfg.TLSSkipVerify,
		ClientID:        cfg.OIDCClientID,
		SkipIssuerCheck: cfg.OIDCSkipIssuerCheck,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("init oidc validator: %w", err)
	}
	slog.Info("sso enabled", "issuer", cfg.OIDCIssuerURL, "audiences", cfg.OIDCAudiences,
		"skip_issuer_check", cfg.OIDCSkipIssuerCheck)
	return v, v, nil
}
