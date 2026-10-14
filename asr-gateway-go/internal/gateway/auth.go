package gateway

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

var errTokenExpired = errors.New("token is expired")

type authResolver struct {
	cfg Config

	keyMu sync.Mutex
	key   *rsa.PublicKey
}

func newAuthResolver(cfg Config) *authResolver {
	return &authResolver{cfg: cfg}
}

func (a *authResolver) resolve(token string) (string, error) {
	if token == "" {
		return "", errors.New("missing token")
	}

	// 1. ServiceToken override / development fallback
	if a.cfg.ServiceToken != "" && token == a.cfg.ServiceToken {
		return "service-user", nil
	}

	// 2. JWT verification via JWKS_PUBLIC_KEY
	return a.verifyJWT(token)
}

func (a *authResolver) verifyJWT(tokenString string) (string, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid token format: must be three segments")
	}

	key, err := a.publicKey()
	if err != nil {
		// If JWKS_PUBLIC_KEY is not configured and ServiceToken didn't match, fail with config error
		return "", fmt.Errorf("public key error: %w", err)
	}

	var header struct {
		Alg string `json:"alg"`
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid token header: %w", err)
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", fmt.Errorf("unmarshal token header failed: %w", err)
	}
	if header.Alg != "RS256" {
		return "", fmt.Errorf("unexpected signing method: %s", header.Alg)
	}

	signed := []byte(parts[0] + "." + parts[1])
	sum := sha256.Sum256(signed)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("invalid token signature encoding: %w", err)
	}
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return "", fmt.Errorf("token signature verification failed: %w", err)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("invalid token payload encoding: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", fmt.Errorf("unmarshal token payload failed: %w", err)
	}

	now := float64(time.Now().Unix())
	if exp, ok := claims["exp"].(float64); ok && now >= exp {
		return "", fmt.Errorf(`"exp" claim timestamp check failed: %w`, errTokenExpired)
	}
	if nbf, ok := claims["nbf"].(float64); ok && now < nbf {
		return "", errors.New(`"nbf" claim timestamp check failed`)
	}

	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", errors.New("missing sub claim")
	}
	return sub, nil
}

func (a *authResolver) publicKey() (*rsa.PublicKey, error) {
	a.keyMu.Lock()
	defer a.keyMu.Unlock()
	if a.key != nil {
		return a.key, nil
	}
	if a.cfg.JWKSPublicKey == "" {
		return nil, errors.New("JWKS_PUBLIC_KEY is not configured")
	}

	var jwks struct {
		Keys []struct {
			Alg string `json:"alg"`
			E   string `json:"e"`
			Kty string `json:"kty"`
			N   string `json:"n"`
		} `json:"keys"`
	}
	if err := json.Unmarshal([]byte(a.cfg.JWKSPublicKey), &jwks); err != nil {
		return nil, fmt.Errorf("invalid JWKS_PUBLIC_KEY JSON: %w", err)
	}
	for _, k := range jwks.Keys {
		if k.Alg != "RS256" || k.Kty != "RSA" {
			continue
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			return nil, fmt.Errorf("invalid N in JWKS key: %w", err)
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			return nil, fmt.Errorf("invalid E in JWKS key: %w", err)
		}
		e := 0
		for _, b := range eBytes {
			e = e<<8 + int(b)
		}
		a.key = &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}
		return a.key, nil
	}
	return nil, errors.New("no RS256 RSA key found in JWKS_PUBLIC_KEY")
}
