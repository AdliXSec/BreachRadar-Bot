
package main

import (
	"testing"
	"strings"
)

// TestGenerateFingerprint tests the generateFingerprint function.
func TestGenerateFingerprint(t *testing.T) {
	testData := "test data"
	
	// A real sha256 hash for "test data"
	expectedFingerprint := "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"

	fingerprint := generateFingerprint(testData)

	if fingerprint != expectedFingerprint {
		t.Errorf("generateFingerprint() = %s; want %s", fingerprint, expectedFingerprint)
	}

	// The hash generated is a sha256 hash, which is 64 characters long in hexadecimal format.
	// The test should check the length of the generated hash.
	if len(fingerprint) != 64 {
		t.Errorf("generateFingerprint() hash length = %d; want 64", len(fingerprint))
	}

	// Also, let's have a more stable test.
	// The fingerprint of the same data should be the same.
	fingerprint2 := generateFingerprint(testData)
	if fingerprint != fingerprint2 {
		t.Errorf("generateFingerprint() for the same data produced different fingerprints: %s and %s", fingerprint, fingerprint2)
	}
}

// TestGenerateInviteKey tests the generateInviteKey function.
func TestGenerateInviteKey(t *testing.T) {
	key := generateInviteKey()
	if len(key) != 8 {
		t.Errorf("generateInviteKey() len = %d; want 8", len(key))
	}
	if !strings.HasPrefix(key, "BR-") {
		t.Errorf("generateInviteKey() prefix = %s; want BR-", key[:3])
	}
}

// TestIsSensitive tests the isSensitive function.
func TestIsSensitive(t *testing.T) {
	sensitiveTests := []struct {
		input    string
		expected bool
	}{
		{"password", true},
		{"my_password", true},
		{"pass", true},
		{"pwd", true},
		{"secret", true},
		{"token", true},
		{"auth_token", true},
		{"insecure", false},
		{"test", false},
		{"data", false},
	}

	for _, tt := range sensitiveTests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isSensitive(tt.input); got != tt.expected {
				t.Errorf("isSensitive() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestMaskPassword tests the maskPassword function.
func TestMaskPassword(t *testing.T) {
	maskTests := []struct {
		input    string
		expected string
	}{
		{"password: mysecretpassword", "password: ********"},
		{"pass=123456", "pass=********"},
		{"pwd: secret", "pwd: ********"},
		{"my long secret is: verysecret", "my long secret is: ********"},
		{"username: admin", "username: admin"},
	}

	for _, tt := range maskTests {
		t.Run(tt.input, func(t *testing.T) {
			if got := maskPassword(tt.input); got != tt.expected {
				t.Errorf("maskPassword() = %q, want %q", got, tt.expected)
			}
		})
	}
}
