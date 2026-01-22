package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
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

	// Query khusus log
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

	// Formatting Output
	latestLog := result.Hits.Hits[0].Source

	// Handle User ID type safety
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
	switch command {
	case "/open":
		// FIX: Gunakan getSystemConfig & saveSystemConfig
		config := getSystemConfig(es)
		config.Mode = "OPEN"
		saveSystemConfig(es, config)
		bot.Send(tgbotapi.NewMessage(chatID, "🔓 **SYSTEM OPEN**\nSekarang semua orang bisa mengakses bot."))

	case "/close":
		// FIX: Gunakan getSystemConfig & saveSystemConfig
		config := getSystemConfig(es)
		config.Mode = "CLOSE"
		saveSystemConfig(es, config)
		bot.Send(tgbotapi.NewMessage(chatID, "🔒 **SYSTEM CLOSED**\nHanya Admin & User yang memiliki Key yang bisa akses."))

	case "/genkey":
		key := generateInviteKey()
		saveAccessKey(es, key)
		msg := fmt.Sprintf("🎟 **NEW ACCESS KEY**\nKey: `%s`\n\nBerikan key ini ke user. Gunakan `/redeem %s`", key, key)
		reply := tgbotapi.NewMessage(chatID, msg)
		reply.ParseMode = "Markdown"
		bot.Send(reply)

	case "/delkey":
		resetAllAccess(es)
		bot.Send(tgbotapi.NewMessage(chatID, "💥 **RESET SUCCESS**\nSemua Key dihapus.\nSemua User (kecuali Admin) telah dikeluarkan dari whitelist."))
	}
}

func handleStats(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client) {
	msgLoading, _ := bot.Send(tgbotapi.NewMessage(chatID, "📊 _Mengambil data statistik..._"))

	stats := getClusterStats(es)

	// Ambil Config terbaru
	config := getSystemConfig(es)

	statusIcon := "🔓"
	if config.Mode == "CLOSE" {
		statusIcon = "🔒"
	}

	// Info RAM
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

// --- LOGIC UPLOAD (Streaming) ---
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

	total := 0
	if strings.HasSuffix(strings.ToLower(fileName), ".csv") {
		total = ingestStreamCSV(resp.Body, fileName, es)
	} else {
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

	resp, err := http.Get(fileURL) // Download dari server telegram
	if err != nil {
		return
	}
	defer resp.Body.Close()

	fileName := msg.Document.FileName
	total := 0
	if strings.HasSuffix(strings.ToLower(fileName), ".csv") {
		total = ingestStreamCSV(resp.Body, fileName, es)
	} else {
		total = ingestStreamText(resp.Body, fileName, es)
	}
	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, fmt.Sprintf("✅ **UPLOAD SELESAI!**\nFile: `%s`\nTotal: %d", fileName, total)))
}

// --- HELPER INGESTION ---
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

func ingestStreamText(r io.Reader, filename string, es *elasticsearch.Client) int {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 5*1024*1024)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 5 {
			continue
		}
		doc := map[string]interface{}{"leak_source": filename, "raw_content": line, "full_text": line}
		indexDocument(es, doc, generateFingerprint(line+filename))
		count++
	}
	return count
}

func handleBroadcast(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	chatID := msg.Chat.ID

	// 1. Ambil isi pesan setelah "/broadcast "
	text := strings.TrimSpace(strings.Replace(msg.Text, "/broadcast", "", 1))
	if text == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/broadcast Pesan Pengumuman...`"))
		return
	}

	bot.Send(tgbotapi.NewMessage(chatID, "📢 _Memulai broadcast..._"))

	// 2. Ambil Daftar Penerima
	targets := getAllVerifiedUserIDs(es)
	if len(targets) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Tidak ada Verified User ditemukan."))
		return
	}

	// 3. Looping Kirim Pesan
	success := 0
	failed := 0

	for _, targetID := range targets {
		// Format Pesan agar terlihat resmi
		broadcastMsg := fmt.Sprintf("📢 **PENGUMUMAN ADMIN**\n\n%s", text)

		msgToSend := tgbotapi.NewMessage(targetID, broadcastMsg)
		msgToSend.ParseMode = "Markdown"

		_, err := bot.Send(msgToSend)
		if err == nil {
			success++
		} else {
			failed++ // Biasanya karena user nge-block bot
		}

		// Jeda 50ms per pesan agar aman dari limit Telegram (30 msg/sec)
		time.Sleep(50 * time.Millisecond)
	}

	// 4. Laporan Selesai
	report := fmt.Sprintf("✅ **BROADCAST SELESAI**\n\n📨 Terkirim: %d\n🚫 Gagal: %d\n👥 Total Target: %d", success, failed, len(targets))
	bot.Send(tgbotapi.NewMessage(chatID, report))
}

func handleNotification(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	chatID := msg.Chat.ID

	// 1. Ambil isi pesan
	text := strings.TrimSpace(strings.Replace(msg.Text, "/notif", "", 1))
	if text == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/notif Pesan untuk semua user...`"))
		return
	}

	bot.Send(tgbotapi.NewMessage(chatID, "🔔 _Mengumpulkan data semua user..._"))

	// 2. Ambil User Unik dari Log
	targets := getAllUniqueLogUserIDs(es)
	if len(targets) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Belum ada history user di log."))
		return
	}

	// 3. Looping Kirim
	success := 0
	failed := 0

	for _, targetID := range targets {
		// Header beda: INFO UMUM
		notifMsg := fmt.Sprintf("🔔 **INFO DARI BOT**\n\n%s", text)

		msgToSend := tgbotapi.NewMessage(targetID, notifMsg)
		msgToSend.ParseMode = "Markdown"

		_, err := bot.Send(msgToSend)
		if err == nil {
			success++
		} else {
			failed++ // User mungkin sudah blokir bot
		}

		time.Sleep(50 * time.Millisecond) // Anti-Flood
	}

	// 4. Laporan
	report := fmt.Sprintf("✅ **NOTIFIKASI SELESAI**\n\n📨 Terkirim: %d\n🚫 Gagal: %d\n👥 Total Target (Unik): %d", success, failed, len(targets))
	bot.Send(tgbotapi.NewMessage(chatID, report))
}

func handleGetUsers(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client) {
	bot.Send(tgbotapi.NewMessage(chatID, "👥 _Sedang merekap data pengguna..._"))

	users := generateUserReport(es)
	if len(users) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Belum ada data pengguna."))
		return
	}

	// Buat CSV di Memori
	b := &bytes.Buffer{}
	w := csv.NewWriter(b)

	// Header
	w.Write([]string{"USER ID", "USERNAME", "FIRST NAME", "LAST NAME", "STATUS"})

	// Isi Data
	countVerified := 0
	for _, u := range users {
		w.Write([]string{
			u.UserID,
			"@" + u.Username,
			u.FirstName,
			u.LastName,
			u.Status,
		})
		if u.Status == "VERIFIED" {
			countVerified++
		}
	}
	w.Flush()

	// Kirim File
	fileName := fmt.Sprintf("users_report_%s.csv", time.Now().Format("20060102_150405"))
	fileBytes := tgbotapi.FileBytes{Name: fileName, Bytes: b.Bytes()}

	docMsg := tgbotapi.NewDocument(chatID, fileBytes)
	docMsg.Caption = fmt.Sprintf("✅ **REKAP PENGGUNA SELESAI**\n\n👥 Total User Unik: %d\n✅ Verified: %d\n👤 Guest: %d",
		len(users), countVerified, len(users)-countVerified)
	docMsg.ParseMode = "Markdown"

	bot.Send(docMsg)
}

func handleDirectMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID

	// 1. Parsing Input
	// Format: /sendto <ID> <Pesan>
	// Kita split menjadi 3 bagian: Command, ID, Pesan
	parts := strings.SplitN(msg.Text, " ", 3)

	if len(parts) < 3 {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Format salah.\nGunakan: `/sendto <UserID> <Pesan Anda>`\nContoh: `/sendto 12345678 Selamat Anda menang!`"))
		return
	}

	targetIDStr := strings.TrimSpace(parts[1])
	content := parts[2]

	// 2. Validasi ID (Harus Angka)
	targetID, err := strconv.ParseInt(targetIDStr, 10, 64)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ ID User harus berupa angka."))
		return
	}

	// 3. Kirim Pesan ke Target
	// Kita tambahkan header agar user tahu ini pesan manual dari Admin
	finalMsg := fmt.Sprintf("📩 **PESAN DARI ADMIN**\n\n%s", content)

	msgToSend := tgbotapi.NewMessage(targetID, finalMsg)
	msgToSend.ParseMode = "Markdown"

	_, errSend := bot.Send(msgToSend)

	// 4. Laporan ke Admin
	if errSend != nil {
		// Error biasanya terjadi jika User memblokir bot atau ID salah
		errMsg := fmt.Sprintf("❌ **GAGAL KIRIM**\nKe ID: `%d`\nError: %v", targetID, errSend)
		bot.Send(tgbotapi.NewMessage(chatID, errMsg))
	} else {
		successMsg := fmt.Sprintf("✅ **TERKIRIM**\nKe ID: `%d`\nIsi: _%s_", targetID, content)
		msgRep := tgbotapi.NewMessage(chatID, successMsg)
		msgRep.ParseMode = "Markdown"
		bot.Send(msgRep)
	}
}

func handleBanSystem(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client, cmd string) {
	chatID := msg.Chat.ID
	args := strings.TrimSpace(strings.Replace(msg.Text, cmd, "", 1))

	if args == "" {
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("⚠️ Gunakan format:\n`%s <UserID> [Alasan]`", cmd)))
		return
	}

	// Parsing ID dan Alasan
	parts := strings.SplitN(args, " ", 2)
	targetID := parts[0]
	reason := "Pelanggaran Rules" // Default reason
	if len(parts) > 1 {
		reason = parts[1]
	}

	// Validasi ID Angka
	if _, err := strconv.ParseInt(targetID, 10, 64); err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ User ID harus berupa angka."))
		return
	}

	if cmd == "/ban" {
		banUser(es, targetID, reason)

		// Info ke Admin
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("⛔ **USER BANNED**\nID: `%s`\nReason: _%s_", targetID, reason)))

		// Opsional: Kirim 'Surat Cinta' ke User yang di-ban
		if uid, err := strconv.ParseInt(targetID, 10, 64); err == nil {
			msgBan := tgbotapi.NewMessage(uid, fmt.Sprintf("🚫 **AKUN ANDA DIBEKUKAN**\n\nAdmin telah memblokir akses Anda ke bot ini secara permanen.\nAlasan: _%s_", reason))
			msgBan.ParseMode = "Markdown"
			bot.Send(msgBan)
		}

	} else if cmd == "/unban" {
		unbanUser(es, targetID)
		bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ **USER UNBANNED**\nID: `%s` telah dipulihkan.", targetID)))

		// Info ke User
		if uid, err := strconv.ParseInt(targetID, 10, 64); err == nil {
			bot.Send(tgbotapi.NewMessage(uid, "✅ **AKSES DIPULIHKAN**\nAnda dapat menggunakan bot kembali."))
		}
	}
}
