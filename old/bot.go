package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

// Struktur Data Log Aktivitas User
type UserActivity struct {
	Timestamp  time.Time `json:"timestamp"`
	UserID     string     `json:"user_id"`
	Username   string    `json:"username"`
	FirstName  string    `json:"first_name"`
	LastName   string    `json:"last_name"` // <--- DITAMBAHKAN
	ActionType string    `json:"action_type"` 
	Query      string    `json:"query_content"`
}

// Struktur Respon ES
type ESResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func main() {
	// 1. CONFIG
	godotenv.Load()
	botToken := os.Getenv("BOT_TOKEN")
	ownerIDStr := os.Getenv("OWNER_ID")
	elasticURL := os.Getenv("ELASTIC_URL")

	if botToken == "" { log.Fatal("❌ .env belum lengkap!") }
	ownerID, _ := strconv.ParseInt(ownerIDStr, 10, 64)

	// 2. ELASTICSEARCH (Satu-satunya Database)
	if elasticURL == "" { elasticURL = "http://localhost:9200" }
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{elasticURL}})
	if err != nil { log.Fatal("❌ Gagal konek ES:", err) }

	// 3. TELEGRAM BOT
	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil { log.Panic(err) }
	bot.Debug = true
	log.Printf("🤖 Super Bot (ES Only) Online: %s", bot.Self.UserName)

	os.MkdirAll("./leaks_data", os.ModePerm)

	// 4. MAIN LOOP
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil { continue }
		msg := update.Message
		chatID := msg.Chat.ID
		user := msg.From

		// --- FITUR ADMIN: AUDIT LOGS (/audit keyword) ---
		if user.ID == ownerID && strings.HasPrefix(msg.Text, "/audit") {
			keyword := strings.TrimSpace(strings.Replace(msg.Text, "/audit", "", 1))
			if keyword == "" {
				bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/audit nama_user` atau `/audit keyword_pencarian`"))
				continue
			}
			handleAuditLog(bot, chatID, es, keyword)
			continue
		}

		// --- LOGIC OWNER: UPLOAD ---
		if user.ID == ownerID {
			if msg.Document != nil {
				// Log Aktivitas Upload
				logActivity(es, user, "UPLOAD", msg.Document.FileName)
				handleFileUpload(bot, msg, botToken, es)
				continue
			}
			if strings.HasPrefix(msg.Text, "http") {
				logActivity(es, user, "UPLOAD_URL", msg.Text)
				handleURLUpload(bot, msg, es)
				continue
			}
		}

		// --- LOGIC SEARCH (SEMUA USER) ---
		if msg.Text != "" && !strings.HasPrefix(msg.Text, "/") {
			// 1. Catat Dulu Siapa yang Mencari (Log ke ES)
			logActivity(es, user, "SEARCH", msg.Text)
			
			// 2. Lakukan Pencarian
			handleSearch(bot, msg, es)
		}
	}
}

// ================= FUNGSI BARU: LOGGING & AUDIT =================

// Fungsi mencatat kegiatan user ke index "user_logs"
func logActivity(es *elasticsearch.Client, user *tgbotapi.User, action string, content string) {
	idString := strconv.FormatInt(user.ID, 10)

	logEntry := UserActivity{
		Timestamp:  time.Now(),
		UserID:     idString,
		Username:   user.UserName,
		FirstName:  user.FirstName,
		LastName:   user.LastName,
		ActionType: action,
		Query:      content,
	}

	body, _ := json.Marshal(logEntry)
	req := esapi.IndexRequest{
		Index:   "user_logs",
		Body:    bytes.NewReader(body),
		Refresh: "false",
	}
	go req.Do(context.Background(), es)
}

// Fungsi Admin untuk melihat log (FORMAT BARU)
func handleAuditLog(bot *tgbotapi.BotAPI, chatID int64, es *elasticsearch.Client, keyword string) {
	bot.Send(tgbotapi.NewMessage(chatID, "🕵️‍♂️ _Mengaudit Log Aktivitas..._"))

	// Cari 20 log terakhir
	queryBody := fmt.Sprintf(`{
		"query": {
			"multi_match": {
				"query": "%s",
				"fields": ["username", "first_name", "last_name", "query_content"],
				"fuzziness": "AUTO"
			}
		},
		"sort": [ { "timestamp": "desc" } ]
	}`, keyword)

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("user_logs"), 
		es.Search.WithBody(strings.NewReader(queryBody)),
		es.Search.WithSize(20), // Tampilkan 20 log terakhir
	)

	if err != nil { return }
	defer res.Body.Close()

	var result ESResponse
	json.NewDecoder(res.Body).Decode(&result)

	if len(result.Hits.Hits) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Tidak ditemukan aktivitas untuk keyword tersebut."))
		return
	}

	// --- LOGIC FORMATTING BARU ---
	
	// 1. Ambil Data Profil dari Log TERBARU (Hit pertama)
	latestLog := result.Hits.Hits[0].Source

	// --- LOGIC PINTAR (HYBRID SUPPORT) ---
	var idUser string
	
	switch v := latestLog["user_id"].(type) {
	case string:
		idUser = v 
	case float64:
		idUser = fmt.Sprintf("%.0f", v) 
	default:
		idUser = fmt.Sprintf("%v", v) 
	}
	
	fName := fmt.Sprintf("%v", latestLog["first_name"])
	lName := fmt.Sprintf("%v", latestLog["last_name"])
	uName := fmt.Sprintf("%v", latestLog["username"])

	// Header Profil
	msg := fmt.Sprintf("🆔 **USER PROFILE**\n")
	msg += fmt.Sprintf("ID User: `%s`\n", idUser)
	msg += fmt.Sprintf("First Name: `%s`\n", escapeMarkdown(fName))
	msg += fmt.Sprintf("Last Name: `%s`\n", escapeMarkdown(lName))
	msg += fmt.Sprintf("Username: `@%s`\n\n", escapeMarkdown(uName))
	
	msg += "📜 **--- LOG AKTIVITAS ---**\n"

	// 2. Loop Daftar Log
	for _, hit := range result.Hits.Hits {
		src := hit.Source
		
		// Parsing Waktu
		tStr := fmt.Sprintf("%v", src["timestamp"])
		parsedTime, _ := time.Parse(time.RFC3339, tStr)
		// Format: 20 Jan 23:03
		humanTime := parsedTime.Format("02 Jan 15:04") 
		
		action := fmt.Sprintf("%v", src["action_type"])
		content := fmt.Sprintf("%v", src["query_content"])

		// Format per baris: [Waktu] [Action] Content
		// Contoh: 20 Jan 23:03 [SEARCH] ip:10.0.0.1
		msg += fmt.Sprintf("`%s` | *%s*\n`%s`\n\n", 
			humanTime, 
			escapeMarkdown(action), 
			escapeMarkdown(content))
	}

	// Kirim Pesan
	reply := tgbotapi.NewMessage(chatID, msg)
	reply.ParseMode = "Markdown"
	bot.Send(reply)
}

// ================= FUNGSI LAMA (SEARCH & UPLOAD) =================
// (Tidak ada perubahan logika, hanya dirapikan)

// Handler: Pencarian Canggih (FINAL FIX - ANTI ERROR FORMATTING)
func handleSearch(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	query := msg.Text
	chatID := msg.Chat.ID
	
	loading, _ := bot.Send(tgbotapi.NewMessage(chatID, "🔍 _Sedang mencari..._"))

	var esQuery string
	
	if strings.Contains(query, ":") {
		// --- MODE EXACT MATCH ---
		parts := strings.SplitN(query, ":", 2)
		rawKey := strings.TrimSpace(parts[0]) 
		value := strings.TrimSpace(parts[1])  

		patternLower := fmt.Sprintf("*%s*", strings.ToLower(rawKey)) 
		patternUpper := fmt.Sprintf("*%s*", strings.ToUpper(rawKey)) 

		esQuery = fmt.Sprintf(`{
			"query": {
				"multi_match": {
					"query": "%s",
					"fields": ["%s", "%s"], 
					"operator": "and", 
					"fuzziness": "0" 
				}
			}
		}`, value, patternLower, patternUpper)
		
	} else {
		// --- MODE FULL TEXT ---
		esQuery = fmt.Sprintf(`{
			"query": { "match": { "full_text": { "query": "%s", "operator": "and", "fuzziness": "AUTO" } } }
		}`, query)
	}

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("breach_data"),
		es.Search.WithBody(strings.NewReader(esQuery)),
		es.Search.WithTrackTotalHits(true),
	)

	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Error Database."))
		return
	}
	defer res.Body.Close()

	var result ESResponse
	json.NewDecoder(res.Body).Decode(&result)

	totalFound := result.Hits.Total.Value
	var replyText string

	if totalFound > 0 {
		replyText = fmt.Sprintf("🚨 *DATA FOUND!*\nKeyword: `%s`\nResult: %d Data\n\n", escapeMarkdown(query), totalFound)
		for i, hit := range result.Hits.Hits {
			if i >= 5 { break }
			replyText += "📂 *RECORD:*\n"
			for k, v := range hit.Source {
				// Skip field internal
				if k == "full_text" || k == "raw_content" || k == "upload_date" || k == "leak_source" { continue }
				
				valStr := fmt.Sprintf("%v", v)
				if isSensitive(k) { valStr = maskPassword(valStr) }
				
				replyText += fmt.Sprintf("▪️ `%s`: `%s`\n", escapeMarkdown(strings.ToUpper(k)), escapeMarkdown(valStr))
			}
			
			// --- PERBAIKAN FINAL DISINI ---
			// Jangan pakai _ (italic) untuk pembungkus luar. 
			// Cukup pakai ` (code block) untuk nama filenya saja.
			// Ini jauh lebih stabil.
			sourceName := fmt.Sprintf("%v", hit.Source["leak_source"])
			replyText += fmt.Sprintf("📁 Source: `%s`\n", escapeMarkdown(sourceName))
			// ------------------------------
			
			replyText += "------------------\n"
		}
		if totalFound > 5 { replyText += fmt.Sprintf("_(...%d data lainnya)_", totalFound-5) }
	} else {
		replyText = fmt.Sprintf("✅ *AMAN!*\nNihil: `%s`", escapeMarkdown(query))
	}

	bot.Request(tgbotapi.NewDeleteMessage(chatID, loading.MessageID))
	msgRep := tgbotapi.NewMessage(chatID, replyText)
	msgRep.ParseMode = "Markdown"
	bot.Send(msgRep)
}

// ... (Sertakan kembali fungsi handleFileUpload, handleURLUpload, ingestCSV, ingestText, sendToES, generateFingerprint dari kode sebelumnya di sini)
// Agar tidak kepanjangan, fungsi helper di bawah ini SAMA PERSIS dengan sebelumnya.
// Pastikan Anda meng-copy fungsi helpernya juga.

func handleFileUpload(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, token string, es *elasticsearch.Client) {
	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "📥 _Menerima file..._"))
	fileURL, err := bot.GetFileDirectURL(msg.Document.FileID)
	if err != nil { return }
	processStream(bot, msg.Chat.ID, fileURL, msg.Document.FileName, es)
}

func handleURLUpload(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	url := msg.Text
	parts := strings.Split(url, "/")
	fileName := "url_import_" + parts[len(parts)-1]
	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "🌐 _Mendownload URL..._"))
	processStream(bot, msg.Chat.ID, url, fileName, es)
}

func processStream(bot *tgbotapi.BotAPI, chatID int64, url string, filename string, es *elasticsearch.Client) {
	resp, err := http.Get(url)
	if err != nil { bot.Send(tgbotapi.NewMessage(chatID, "❌ Gagal download.")); return }
	defer resp.Body.Close()
	bot.Send(tgbotapi.NewMessage(chatID, "⚙️ _Ingesting..._"))
	total := 0
	if strings.HasSuffix(strings.ToLower(filename), ".csv") { total = ingestCSV(resp.Body, filename, es) } else { total = ingestText(resp.Body, filename, es) }
	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ **SELESAI!**\nFile: `%s`\nTotal: %d", filename, total)))
}

func ingestCSV(r io.Reader, filename string, es *elasticsearch.Client) int {
	reader := csv.NewReader(r)
	headers, _ := reader.Read()
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF { break }
		if err != nil { continue }
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
		fullText := strings.Join(txtBuf, " ")
		doc["full_text"] = fullText
		sendToES(es, doc, generateFingerprint(fullText+filename))
		count++
	}
	return count
}

func ingestText(r io.Reader, filename string, es *elasticsearch.Client) int {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024); scanner.Buffer(buf, 5*1024*1024)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 5 { continue }
		doc := map[string]interface{}{"leak_source": filename, "raw_content": line, "full_text": line}
		sendToES(es, doc, generateFingerprint(line+filename))
		count++
	}
	return count
}

func sendToES(es *elasticsearch.Client, doc map[string]interface{}, id string) {
	body, _ := json.Marshal(doc)
	req := esapi.IndexRequest{Index: "breach_data", DocumentID: id, Body: bytes.NewReader(body), Refresh: "false"}
	req.Do(context.Background(), es)
}
func generateFingerprint(data string) string { hash := sha256.Sum256([]byte(data)); return hex.EncodeToString(hash[:]) }
func escapeMarkdown(text string) string { return strings.NewReplacer("_", "\\_", "*", "\\*", "[", "\\[", "`", "\\`").Replace(text) }
func isSensitive(key string) bool { k := strings.ToLower(key); return strings.Contains(k, "pass") || strings.Contains(k, "hash") || strings.Contains(k, "pwd") }
func maskPassword(val string) string { if len(val) > 3 { return val[:3] + "***" }; return "***" }