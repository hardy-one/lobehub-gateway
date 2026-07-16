package gateway

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var errTokenExpired = errors.New("token is expired")

type authResolver struct {
	cfg Config
}

func newAuthResolver(cfg Config) *authResolver {
	return &authResolver{cfg: cfg}
}

func (a *authResolver) resolve(ctx context.Context, storedUserID string, msg authMessage) (string, error) {
	if msg.Token == "" {
		return "", errors.New("Missing token")
	}

	if msg.TokenType == "apiKey" {
		if msg.ServerURL == "" {
			return "", errors.New("Missing serverUrl")
		}
		return verifyAPIKey(ctx, msg.ServerURL, msg.Token)
	}

	if msg.Token == a.cfg.ServiceToken {
		if storedUserID == "" {
			return "", errors.New("Missing userId")
		}
		return storedUserID, nil
	}

	claims, err := a.verifyJWT(msg.Token)
	return claims.Subject, err
}

func (a *authResolver) resolveWorkspace(msg authMessage, workspaceID string) error {
	if msg.TokenType == "apiKey" || msg.Token == a.cfg.ServiceToken {
		return errors.New("workspace connections require a workspace JWT")
	}
	claims, err := a.verifyJWT(msg.Token)
	if err != nil {
		return err
	}
	if claims.Purpose != "workspace-device-connect" || claims.WorkspaceID != workspaceID || claims.Subject != workspaceID {
		return errors.New("workspace token claims do not match connection")
	}
	return nil
}

func verifyAPIKey(ctx context.Context, serverURL string, token string) (string, error) {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/v1/users/me"
	parsed.RawQuery = "includeCount=0"
	parsed.Fragment = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var body struct {
		Data *struct {
			ID     string `json:"id"`
			UserID string `json:"userId"`
		} `json:"data"`
		Error   string `json:"error"`
		Message string `json:"message"`
		Success *bool  `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("Failed to parse response from %s.", parsed.String())
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 || (body.Success != nil && !*body.Success) {
		if body.Error != "" {
			return "", errors.New(body.Error)
		}
		if body.Message != "" {
			return "", errors.New(body.Message)
		}
		return "", fmt.Errorf("Request failed with status %d.", resp.StatusCode)
	}
	if body.Data == nil {
		return "", errors.New("Current user response did not include a user id.")
	}
	if body.Data.ID != "" {
		return body.Data.ID, nil
	}
	if body.Data.UserID != "" {
		return body.Data.UserID, nil
	}
	return "", errors.New("Current user response did not include a user id.")
}

type jwtClaims struct{ Purpose, Subject, WorkspaceID string }

type jwk struct {
	Alg string `json:"alg"`
	E   string `json:"e"`
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	N   string `json:"n"`
}

func (a *authResolver) verifyJWT(tokenString string) (jwtClaims, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return jwtClaims{}, errors.New("invalid token")
	}

	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return jwtClaims{}, err
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return jwtClaims{}, err
	}
	if header.Alg != "RS256" {
		return jwtClaims{}, fmt.Errorf("unexpected signing method: %s", header.Alg)
	}
	key, err := a.publicKey(header.Kid)
	if err != nil {
		return jwtClaims{}, err
	}

	signed := []byte(parts[0] + "." + parts[1])
	sum := sha256.Sum256(signed)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return jwtClaims{}, err
	}
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return jwtClaims{}, err
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return jwtClaims{}, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return jwtClaims{}, err
	}
	now := float64(time.Now().Unix())
	if exp, ok := claims["exp"].(float64); ok && now >= exp {
		return jwtClaims{}, fmt.Errorf(`"exp" claim timestamp check failed: %w`, errTokenExpired)
	}
	if nbf, ok := claims["nbf"].(float64); ok && now < nbf {
		return jwtClaims{}, errors.New(`"nbf" claim timestamp check failed`)
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return jwtClaims{}, errors.New("Missing sub claim")
	}
	purpose, _ := claims["purpose"].(string)
	workspaceID, _ := claims["workspace_id"].(string)
	return jwtClaims{Purpose: purpose, Subject: sub, WorkspaceID: workspaceID}, nil
}

func (a *authResolver) publicKey(kid string) (*rsa.PublicKey, error) {
	if a.cfg.JWKSPublicKey == "" {
		return nil, errors.New("JWKS_PUBLIC_KEY is required")
	}

	var jwks struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal([]byte(a.cfg.JWKSPublicKey), &jwks); err != nil {
		return nil, err
	}
	candidates := make([]jwk, 0, len(jwks.Keys))
	for _, key := range jwks.Keys {
		if key.Alg != "RS256" || key.Kty != "RSA" {
			continue
		}
		if kid != "" && key.Kid != kid {
			continue
		}
		candidates = append(candidates, key)
	}
	if len(candidates) != 1 {
		return nil, errors.New("No unambiguous RS256 key found in JWKS_PUBLIC_KEY")
	}
	for _, key := range candidates {
		nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, err
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, err
		}
		e := 0
		for _, b := range eBytes {
			e = e<<8 + int(b)
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
	}
	return nil, errors.New("No RS256 key found in JWKS_PUBLIC_KEY")
}
