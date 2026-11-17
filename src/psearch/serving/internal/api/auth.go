package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/api/idtoken"
	"psearch/serving-go/internal/config"
)

// TokenClaims holds the subset of Google OIDC ID token claims evaluated by AuthorizeAPI.
type TokenClaims struct {
	Issuer        string
	Email         string
	EmailVerified bool
	HostedDomain  string
}

// TokenVerifier validates a Bearer ID token against the expected audience and extracts claims.
type TokenVerifier func(ctx context.Context, token, audience string) (*TokenClaims, error)

// DefaultTokenVerifier validates Google OIDC ID tokens using google.golang.org/api/idtoken.
func DefaultTokenVerifier(ctx context.Context, token, audience string) (*TokenClaims, error) {
	payload, err := idtoken.Validate(ctx, token, audience)
	if err != nil {
		return nil, err
	}
	return claimsFromPayload(payload), nil
}

func claimsFromPayload(payload *idtoken.Payload) *TokenClaims {
	if payload == nil {
		return nil
	}
	claims := &TokenClaims{
		Issuer: payload.Issuer,
	}
	if payload.Claims != nil {
		if email, ok := payload.Claims["email"].(string); ok {
			claims.Email = email
		}
		if verified, ok := payload.Claims["email_verified"].(bool); ok {
			claims.EmailVerified = verified
		}
		if hd, ok := payload.Claims["hd"].(string); ok {
			claims.HostedDomain = hd
		}
	}
	return claims
}

// AuthorizeAPI enforces fail-closed API authentication when RequireAPIAuth, APIToken,
// or AuthAudience is configured. Admin-write endpoints continue to enforce X-Admin-Token
// in their handlers in addition to this middleware.
func AuthorizeAPI(cfg *config.Config, verifier TokenVerifier) gin.HandlerFunc {
	if verifier == nil {
		verifier = DefaultTokenVerifier
	}
	return func(ctx *gin.Context) {
		if cfg == nil {
			ctx.Next()
			return
		}

		apiToken := strings.TrimSpace(cfg.APIToken)
		adminToken := strings.TrimSpace(cfg.AdminToken)
		audience := strings.TrimSpace(cfg.AuthAudience)

		if !cfg.RequireAPIAuth && apiToken == "" && audience == "" {
			ctx.Next()
			return
		}

		allowedPrincipals := normalizePrincipals(cfg.AllowedPrincipals)
		allowedDomains := normalizeDomains(cfg.AllowedDomains)
		oidcConfigured := audience != "" && (len(allowedPrincipals) > 0 || len(allowedDomains) > 0)

		if apiToken == "" && adminToken == "" && !oidcConfigured {
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "API authentication is not configured"})
			return
		}

		if providedAPIKey := ctx.GetHeader("X-API-Key"); providedAPIKey != "" {
			if apiToken != "" && subtle.ConstantTimeCompare([]byte(providedAPIKey), []byte(apiToken)) == 1 {
				ctx.Next()
				return
			}
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}

		if providedAdmin := ctx.GetHeader("X-Admin-Token"); providedAdmin != "" {
			if adminToken != "" && subtle.ConstantTimeCompare([]byte(providedAdmin), []byte(adminToken)) == 1 {
				ctx.Next()
				return
			}
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}

		rawAuth := strings.TrimSpace(ctx.GetHeader("Authorization"))
		if rawAuth == "" {
			ctx.Header("WWW-Authenticate", "Bearer")
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authentication credentials"})
			return
		}

		parts := strings.Fields(rawAuth)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			ctx.Header("WWW-Authenticate", "Bearer")
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid bearer token"})
			return
		}
		token := parts[1]

		if !oidcConfigured {
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "OIDC authentication is not configured"})
			return
		}

		claims, err := verifier(ctx.Request.Context(), token, audience)
		if err != nil || claims == nil || (claims.Issuer != "https://accounts.google.com" && claims.Issuer != "accounts.google.com") {
			ctx.Header("WWW-Authenticate", "Bearer")
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid bearer token"})
			return
		}

		emailLower := strings.ToLower(strings.TrimSpace(claims.Email))
		if !claims.EmailVerified || emailLower == "" {
			ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden"})
			return
		}

		if slices.Contains(allowedPrincipals, emailLower) {
			ctx.Next()
			return
		}

		emailDomain := extractEmailDomain(emailLower)
		hd := strings.ToLower(strings.TrimSpace(claims.HostedDomain))
		if emailDomain != "" && slices.Contains(allowedDomains, emailDomain) && hd == emailDomain {
			ctx.Next()
			return
		}

		ctx.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "forbidden"})
	}
}

func normalizePrincipals(raw []string) []string {
	var out []string
	for _, p := range raw {
		if v := strings.ToLower(strings.TrimSpace(p)); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func normalizeDomains(raw []string) []string {
	var out []string
	for _, d := range raw {
		v := strings.TrimSpace(d)
		v = strings.TrimPrefix(v, "@")
		v = strings.ToLower(strings.TrimSpace(v))
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func extractEmailDomain(email string) string {
	idx := strings.IndexByte(email, '@')
	if idx <= 0 || idx >= len(email)-1 {
		return ""
	}
	if strings.LastIndexByte(email, '@') != idx {
		return ""
	}
	return email[idx+1:]
}
