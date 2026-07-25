package update

import "testing"

// The fixture below is a REAL cosign signature, produced with:
//
//	printf 'livck-agent test artifact v0.3.0\n' > blob
//	cosign sign-blob --key cosign.key blob
//
// using the private key whose public half is ReleasePublicKeyPEM. It pins the
// native verifier to cosign's actual output format — if this passes, the agent
// verifies genuine cosign signatures; no format guessing.
const (
	fixtureContent = "livck-agent test artifact v0.3.0\n"
	fixtureSig     = "MEUCIE+d9F83KySSG5kwhQhEnA5uI4Btceztqac8uzYMIZMgAiEAkI+0vbSOa7faUvAFBBMRoQQdIL9QBIAIU9NijmN0sZI="
)

func TestVerifyBlobSignatureAcceptsRealCosignSignature(t *testing.T) {
	if err := verifyBlobSignature(ReleasePublicKeyPEM, []byte(fixtureContent), []byte(fixtureSig)); err != nil {
		t.Fatalf("a genuine cosign signature must verify against the baked-in key: %v", err)
	}
}

func TestVerifyBlobSignatureRejectsTamperedContent(t *testing.T) {
	if err := verifyBlobSignature(ReleasePublicKeyPEM, []byte("tampered artifact"), []byte(fixtureSig)); err == nil {
		t.Fatal("a swapped artifact must FAIL verification (fail-closed)")
	}
}

func TestVerifyBlobSignatureRejectsBadSignature(t *testing.T) {
	if err := verifyBlobSignature(ReleasePublicKeyPEM, []byte(fixtureContent), []byte("not-base64-or-a-sig")); err == nil {
		t.Fatal("a garbage signature must FAIL")
	}
	// A validly-encoded but wrong signature must also fail.
	if err := verifyBlobSignature(ReleasePublicKeyPEM, []byte(fixtureContent), []byte("MEUCIQD0000000000000000000000000000000000000000000000000000000000AiEA00000000000000000000000000000000000000000000000000000000000000")); err == nil {
		t.Fatal("a wrong signature must FAIL")
	}
}

func TestVerifyBlobSignatureRejectsBadKey(t *testing.T) {
	if err := verifyBlobSignature("not a pem", []byte(fixtureContent), []byte(fixtureSig)); err == nil {
		t.Fatal("an invalid public key must be a hard error")
	}
}
