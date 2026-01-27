package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func handleAuditLog(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client, keyword string) {
	bot.Send(tgbotapi.NewMessage(chatID, "🕵️‍♂️ _Mengaudit Log Aktivitas..._"))

	queryBody := fmt.Sprintf(`{
		"query": {
			"multi_match": { "query": "%s", "fields": ["username", "first_name", "last_name", "query_content"], "fuzziness": "AUTO" }
		},
		"sort": [ { "timestamp": "desc" } ]
	}`, keyword)

	result, err := executeSearch(es, "user_logs", queryBody, 20)
	if err != nil || len(result.Hits.Hits) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Data log tidak ditemukan."))
		return
	}

	latestLog := result.Hits.Hits[0].Source
	var idUser string
	switch v := latestLog["user_id"].(type) {
	case string:
		idUser = v
	case float64:
		idUser = fmt.Sprintf("%.0f", v)
	default:
		idUser = fmt.Sprintf("%v", v)
	}

	msg := fmt.Sprintf("🆔 **USER PROFILE**\nID: `%s`\nUsername: `@%v`\n\n📜 **LOG:**\n", idUser, latestLog["username"])

	for _, hit := range result.Hits.Hits {
		src := hit.Source
		tStr := fmt.Sprintf("%v", src["timestamp"])
		parsedTime, _ := time.Parse(time.RFC3339, tStr)
		humanTime := parsedTime.Format("02 Jan 15:04")
		msg += fmt.Sprintf("`%s` | *%v*\n`%v`\n\n", humanTime, src["action_type"], src["query_content"])
	}

	reply := tgbotapi.NewMessage(chatID, msg)
	reply.ParseMode = "Markdown"
	bot.Send(reply)
}

// --- ACCESS CONTROL HANDLERS (ADMIN) ---

func handleAccessControl(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client, command string) {
	// command di sini berisi msg.Text dari main.go

	switch {
	case command == "/open":
		config := getSystemConfig(es)
		config.Mode = "OPEN"
		saveSystemConfig(es, config)
		bot.Send(tgbotapi.NewMessage(chatID, "🔓 **SYSTEM OPEN**\nSekarang semua orang bisa mengakses bot."))

	case command == "/close":
		config := getSystemConfig(es)
		config.Mode = "CLOSE"
		saveSystemConfig(es, config)
		bot.Send(tgbotapi.NewMessage(chatID, "🔒 **SYSTEM CLOSED**\nHanya Admin & User yang memiliki Key yang bisa akses."))

	case command == "/genkey":
		key := generateInviteKey()
		saveAccessKey(es, key)
		msg := fmt.Sprintf("🎟 **NEW ACCESS KEY**\nKey: `%s`\n\nBerikan key ini ke user. Gunakan `/redeem %s`", key, key)
		reply := tgbotapi.NewMessage(chatID, msg)
		reply.ParseMode = "Markdown"
		bot.Send(reply)

	case command == "/delkey":
		resetAllAccess(es)
		bot.Send(tgbotapi.NewMessage(chatID, "💥 **RESET SUCCESS**\nSemua Key dihapus.\nSemua User (kecuali Admin) telah dikeluarkan dari whitelist."))

	// FITUR BERSIH-BERSIH (FIXED ERROR MSG)
	case strings.HasPrefix(command, "/cleansource"):
		filename := strings.TrimSpace(strings.Replace(command, "/cleansource", "", 1))

		if filename == "" {
			bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/cleansource nama_file.json`"))
			return
		}

		// 1. Kirim Pesan Loading
		loadingMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ _Sedang menghapus data dari database..._"))

		// 2. Eksekusi Penghapusan
		deletedCount := deleteBySource(es, filename)

		// 3. Edit Pesan Jadi Sukses
		resultText := ""
		if deletedCount > 0 {
			resultText = fmt.Sprintf("✅ **PENGHAPUSAN SUKSES**\n\n📁 File: `%s`\n🗑️ Total Dihapus: %d records", filename, deletedCount)
		} else {
			resultText = fmt.Sprintf("❌ **GAGAL / DATA KOSONG**\nTidak ditemukan data dengan source: `%s`", filename)
		}

		edit := tgbotapi.NewEditMessageText(chatID, loadingMsg.MessageID, resultText)
		edit.ParseMode = "Markdown"
		bot.Send(edit)
	}
}

func handleStats(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client) {
	msgLoading, _ := bot.Send(tgbotapi.NewMessage(chatID, "📊 _Mengambil data statistik..._"))
	stats := getClusterStats(es)
	config := getSystemConfig(es)

	statusIcon := "🔓"
	if config.Mode == "CLOSE" {
		statusIcon = "🔒"
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	ramUsage := m.Alloc / 1024 / 1024

	topSearchStr := "-"
	if len(stats.TopSearches) > 0 {
		quoted := []string{}
		for _, s := range stats.TopSearches {
			quoted = append(quoted, fmt.Sprintf(`"%s"`, s))
		}
		topSearchStr = strings.Join(quoted, ", ")
	}

	msg := fmt.Sprintf(`📊 **SYSTEM STATUS**
----------------
🔐 System Mode: *%s %s*
⚡ Rate Limit: *%d req/menit*
💾 Total Data: *%d records*
📁 Total Sources: *%d files*
👥 Verified Users: *%d users*
🔥 Top Search: %s
🖥 RAM Usage: *%d MB*`,
		statusIcon, config.Mode,
		config.RateLimit,
		stats.TotalRecords,
		stats.TotalSources,
		stats.TotalUsers,
		topSearchStr,
		ramUsage)

	editMsg := tgbotapi.NewEditMessageText(chatID, msgLoading.MessageID, msg)
	editMsg.ParseMode = "Markdown"
	bot.Send(editMsg)
}

// --- LOGIC UPLOAD (Smart Router) ---
func handleURLUpload(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	url := msg.Text
	parts := strings.Split(url, "/")
	fileName := "url_" + parts[len(parts)-1]

	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "🌐 _Downloading stream..._"))

	resp, err := http.Get(url)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❌ Gagal download URL."))
		return
	}
	defer resp.Body.Close()

	// ROUTING PINTAR BERDASARKAN EKSTENSI
	total := 0
	lowerName := strings.ToLower(fileName)

	if strings.HasSuffix(lowerName, ".csv") {
		total = ingestStreamCSV(resp.Body, fileName, es)
	} else if strings.HasSuffix(lowerName, ".json") {
		// JSON Array [...] -> Pakai Decoder Baru
		total = ingestStandardJSON(resp.Body, fileName, es)
	} else {
		// TXT, SQL, JSONL, Combo List -> Pakai Scanner Pintar
		total = ingestStreamText(resp.Body, fileName, es)
	}

	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, fmt.Sprintf("✅ **SELESAI!**\nFile: `%s`\nTotal: %d baris", fileName, total)))
}

func handleFileUpload(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, token string, es *elasticsearch.Client) {
	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "📥 _Menerima file..._"))
	fileURL, err := bot.GetFileDirectURL(msg.Document.FileID)
	if err != nil {
		return
	}

	resp, err := http.Get(fileURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	fileName := msg.Document.FileName

	// ROUTING PINTAR BERDASARKAN EKSTENSI
	total := 0
	lowerName := strings.ToLower(fileName)

	if strings.HasSuffix(lowerName, ".csv") {
		total = ingestStreamCSV(resp.Body, fileName, es)
	} else if strings.HasSuffix(lowerName, ".json") {
		total = ingestStandardJSON(resp.Body, fileName, es)
	} else {
		total = ingestStreamText(resp.Body, fileName, es)
	}

	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, fmt.Sprintf("✅ **UPLOAD SELESAI!**\nFile: `%s`\nTotal: %d", fileName, total)))
}

// --- HELPER INGESTION ---

// 1. CSV
func ingestStreamCSV(r io.Reader, filename string, es *elasticsearch.Client) int {
	reader := csv.NewReader(r)
	headers, _ := reader.Read()
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		doc := make(map[string]interface{})
		doc["leak_source"] = filename
		var txtBuf []string
		for j, val := range record {
			if j < len(headers) {
				cleanHeader := strings.TrimSpace(headers[j])
				doc[cleanHeader] = val
				txtBuf = append(txtBuf, val)
			}
		}
		doc["full_text"] = strings.Join(txtBuf, " ")
		indexDocument(es, doc, generateFingerprint(doc["full_text"].(string)+filename))
		count++
	}
	return count
}

// 2. TEXT / COMBO / JSONL
func ingestStreamText(r io.Reader, filename string, es *elasticsearch.Client) int {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)
	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) < 5 {
			continue
		}

		doc := make(map[string]interface{})
		doc["leak_source"] = filename
		doc["full_text"] = line
		doc["raw_content"] = line

		// LOGIC 1: JSON LINES (.jsonl)
		if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
			var jsonDoc map[string]interface{}
			if err := json.Unmarshal([]byte(line), &jsonDoc); err == nil {
				// Flatten nested JSON agar field terbaca di root
				flattenMap("", jsonDoc, doc)
				goto Indexing
			}
		}

		// LOGIC 2: COMBO LIST (:)
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				val1 := strings.TrimSpace(parts[0])
				val2 := strings.TrimSpace(parts[1])
				doc["identity"] = val1
				doc["password"] = val2
				if strings.Contains(val1, "@") && strings.Contains(val1, ".") {
					doc["email"] = val1
				} else {
					doc["username"] = val1
				}
				if len(val2) == 32 || len(val2) == 40 || len(val2) > 50 {
					doc["password_hash"] = val2
				}
			}
		} else if strings.Contains(line, "|") {
			parts := strings.SplitN(line, "|", 2)
			if len(parts) == 2 {
				doc["identity"] = strings.TrimSpace(parts[0])
				doc["password"] = strings.TrimSpace(parts[1])
			}
		} else if strings.Contains(strings.ToUpper(line), "INSERT INTO") {
			doc["data_type"] = "sql_query"
		}

	Indexing:
		indexDocument(es, doc, generateFingerprint(line+filename))
		count++
	}
	return count
}

// 3. STANDARD JSON ARRAY [...] (BARU + FLATTEN)
func ingestStandardJSON(r io.Reader, filename string, es *elasticsearch.Client) int {
	decoder := json.NewDecoder(r)

	// Cek Token Awal
	t, err := decoder.Token()
	if err != nil {
		return 0
	}

	// Harus diawali '['
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return 0
	}

	count := 0
	for decoder.More() {
		var rawDoc map[string]interface{}
		if err := decoder.Decode(&rawDoc); err != nil {
			continue
		}

		finalDoc := make(map[string]interface{})

		// FLATTEN: Ratakan JSON bersarang
		flattenMap("", rawDoc, finalDoc)

		finalDoc["leak_source"] = filename
		if jsonBytes, err := json.Marshal(rawDoc); err == nil {
			finalDoc["full_text"] = string(jsonBytes)
			finalDoc["raw_content"] = string(jsonBytes)
		}

		// Index
		indexDocument(es, finalDoc, generateFingerprint(fmt.Sprintf("%v", finalDoc)+filename))
		count++
	}
	return count
}

// Helper Flatten
func flattenMap(prefix string, src map[string]interface{}, dest map[string]interface{}) {
	for k, v := range src {
		key := k
		// Jika ingin pakai prefix (misal contact_email), uncomment baris bawah:
		// if prefix != "" { key = prefix + "_" + k }

		if childMap, ok := v.(map[string]interface{}); ok {
			flattenMap(key, childMap, dest)
		} else {
			dest[key] = v
		}
	}
}

// --- BROADCAST & NOTIF ---

func handleBroadcast(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	chatID := msg.Chat.ID
	text := strings.TrimSpace(strings.Replace(msg.Text, "/broadcast", "", 1))
	if text == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/broadcast Pesan...`"))
		return
	}

	bot.Send(tgbotapi.NewMessage(chatID, "📢 _Memulai broadcast..._"))
	targets := getAllVerifiedUserIDs(es)
	if len(targets) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Tidak ada Verified User."))
		return
	}

	success := 0
	failed := 0
	for _, targetID := range targets {
		broadcastMsg := fmt.Sprintf("📢 **PENGUMUMAN ADMIN**\n\n%s", text)
		msgToSend := tgbotapi.NewMessage(targetID, broadcastMsg)
		msgToSend.ParseMode = "Markdown"
		_, err := bot.Send(msgToSend)
		if err == nil {
			success++
		} else {
			failed++
		}
		time.Sleep(50 * time.Millisecond)
	}

	report := fmt.Sprintf("✅ **BROADCAST SELESAI**\n\n📨 Terkirim: %d\n🚫 Gagal: %d\n👥 Total Target: %d", success, failed, len(targets))
	bot.Send(tgbotapi.NewMessage(chatID, report))
}

func handleNotification(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	chatID := msg.Chat.ID
	text := strings.TrimSpace(strings.Replace(msg.Text, "/notif", "", 1))
	if text == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/notif Pesan...`"))
		return
	}

	bot.Send(tgbotapi.NewMessage(chatID, "🔔 _Mengumpulkan data semua user..._"))
	targets := getAllUniqueLogUserIDs(es)
	if len(targets) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Belum ada history user."))
		return
	}

	success := 0
	failed := 0
	for _, targetID := range targets {
		notifMsg := fmt.Sprintf("🔔 **INFO DARI BOT**\n\n%s", text)
		msgToSend := tgbotapi.NewMessage(targetID, notifMsg)
		msgToSend.ParseMode = "Markdown"
		_, err := bot.Send(msgToSend)
		if err == nil {
			success++
		} else {
			failed++
		}
		time.Sleep(50 * time.Millisecond)
	}

	report := fmt.Sprintf("✅ **NOTIFIKASI SELESAI**\n\n📨 Terkirim: %d\n🚫 Gagal: %d\n👥 Total Target: %d", success, failed, len(targets))
	bot.Send(tgbotapi.NewMessage(chatID, report))
}

func handleGetUsers(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client) {
	bot.Send(tgbotapi.NewMessage(chatID, "👥 _Sedang merekap data pengguna..._"))
	users := generateUserReport(es)
	if len(users) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Belum ada data pengguna."))
		return
	}

	b := &bytes.Buffer{}
	w := csv.NewWriter(b)
	w.Write([]string{"USER ID", "USERNAME", "FIRST NAME", "LAST NAME", "STATUS"})
	countVerified := 0
	for _, u := range users {
		w.Write([]string{u.UserID, "@" + u.Username, u.FirstName, u.LastName, u.Status})
		if u.Status == "VERIFIED" {
			countVerified++
		}
	}
	w.Flush()

	fileName := fmt.Sprintf("users_report_%s.csv", time.Now().Format("20060102_150405"))
	fileBytes := tgbotapi.FileBytes{Name: fileName, Bytes: b.Bytes()}
	docMsg := tgbotapi.NewDocument(chatID, fileBytes)
	docMsg.Caption = fmt.Sprintf("✅ **REKAP SELESAI**\n\n👥 Total User: %d\n✅ Verified: %d\n👤 Guest: %d", len(users), countVerified, len(users)-countVerified)
	docMsg.ParseMode = "Markdown"
	bot.Send(docMsg)
}

func handleDirectMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	parts := strings.SplitN(msg.Text, " ", 3)
	if len(parts) < 3 {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/sendto <UserID> <Pesan>`"))
		return
	}
	targetIDStr := strings.TrimSpace(parts[1])
	content := parts[2]
	targetID, err := strconv.ParseInt(targetIDStr, 10, 64)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ ID User harus angka."))
		return
	}

	finalMsg := fmt.Sprintf("📩 **PESAN DARI ADMIN**\n\n%s", content)
	msgToSend := tgbotapi.NewMessage(targetID, finalMsg)
	msgToSend.ParseMode = "Markdown"
	_, errSend := bot.Send(msgToSend)

	if errSend != nil {
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("❌ Gagal kirim ke `%d`", targetID)))
	} else {
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Terkirim ke `%d`", targetID)))
	}
}

func handleBanSystem(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client, cmd string) {
	chatID := msg.Chat.ID
	args := strings.TrimSpace(strings.Replace(msg.Text, cmd, "", 1))
	if args == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/ban <UserID> [Alasan]`"))
		return
	}
	parts := strings.SplitN(args, " ", 2)
	targetID := parts[0]
	reason := "Pelanggaran Rules"
	if len(parts) > 1 {
		reason = parts[1]
	}

	if _, err := strconv.ParseInt(targetID, 10, 64); err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ User ID harus angka."))
		return
	}

	if cmd == "/ban" {
		banUser(es, targetID, reason)
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("⛔ **BANNED** `%s`", targetID)))
		if uid, err := strconv.ParseInt(targetID, 10, 64); err == nil {
			bot.Send(tgbotapi.NewMessage(uid, "🚫 **AKUN DIBEKUKAN**\nAlasan: "+reason))
		}
	} else if cmd == "/unban" {
		unbanUser(es, targetID)
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ **UNBANNED** `%s`", targetID)))
		if uid, err := strconv.ParseInt(targetID, 10, 64); err == nil {
			bot.Send(tgbotapi.NewMessage(uid, "✅ **AKSES DIPULIHKAN**"))
		}
	}
}
