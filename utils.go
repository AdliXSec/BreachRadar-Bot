package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"strings"
)

// Mengamankan teks agar tidak merusak format Markdown Telegram
func escapeMarkdown(text string) string {
	replacer := strings.NewReplacer("_", "\\_", "*", "\\*", "[", "\\[", "`", "\\`")
	return replacer.Replace(text)
}

// Cek apakah field mengandung data sensitif
func isSensitive(key string) bool {
	k := strings.ToLower(key)
	return strings.Contains(k, "pass") || strings.Contains(k, "hash") || strings.Contains(k, "pwd")
}

// Sensor password
func maskPassword(val string) string {
	if len(val) > 3 {
		return val[:3] + "***"
	}
	return "***"
}

// Membuat ID unik untuk deduplikasi
func generateFingerprint(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func generateInviteKey() string {
	bytes := make([]byte, 4) // 4 byte entropy
	rand.Read(bytes)
	// Output contoh: BR-J5M2A
	return "BR-" + strings.ToUpper(strings.TrimRight(base32.StdEncoding.EncodeToString(bytes), "=")[:5])
}