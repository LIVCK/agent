package update

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
)

// ReleasePublicKeyPEM is the LIVCK release signing PUBLIC key, compiled into the
// binary so it is the immutable trust anchor: a download server can serve any
// bytes + any signature, but only an artifact signed by the matching PRIVATE key
// (held offline, never in this repo) verifies. Rotating it requires shipping a new
// signed agent version — which itself must be signed by the current key.
//
// cosign ECDSA-P256. Sign a release artifact with:
//
//	cosign sign-blob --key <private> <artifact> > <artifact>.sig
const ReleasePublicKeyPEM = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAElAdjSwPqyMIOcbecTIGw9La5CWJ0
o6vbZgr1ItyGmhX24WLeL92le/9NO/l8nzwROVhUYYQ6Cgnw4xQ9t7qhKQ==
-----END PUBLIC KEY-----
`

// verifyBlobSignature checks a cosign blob signature (base64-encoded ASN.1/DER
// ECDSA over the SHA-256 of content) against a PKIX ECDSA public key. Returns nil
// only on a valid signature; any parse/format/mismatch is a hard error so callers
// fail closed. Matches `cosign verify-blob --key <pub> --signature <sig>`.
func verifyBlobSignature(pubPEM string, content, sigBase64 []byte) error {
	sum := sha256.Sum256(content)
	return verifyDigestSignature(pubPEM, sum[:], sigBase64)
}

// verifyDigestSignature is the primitive: it verifies the cosign ECDSA signature
// against an ALREADY-COMPUTED SHA-256 digest, so the download path can verify the
// streamed artifact without re-reading it. verifyBlobSignature proves the format
// against a real cosign fixture in the tests.
func verifyDigestSignature(pubPEM string, sha256sum, sigBase64 []byte) error {
	block, _ := pem.Decode([]byte(pubPEM))
	if block == nil {
		return errors.New("release public key: invalid PEM")
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("release public key: %w", err)
	}
	pub, ok := pubAny.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("release public key is not ECDSA")
	}

	der, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(sigBase64)))
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	if !ecdsa.VerifyASN1(pub, sha256sum, der) {
		return errors.New("signature does not match the release public key")
	}
	return nil
}
