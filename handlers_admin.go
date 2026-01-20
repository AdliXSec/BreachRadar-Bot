package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
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
	case string: idUser = v
	case float64: idUser = fmt.Sprintf("%.0f", v)
	default: idUser = fmt.Sprintf("%v", v)
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
	if err != nil { return }
	
	resp, err := http.Get(fileURL) // Download dari server telegram
	if err != nil { return }
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

// --- HELPER INGESTION (Dipindah dari es_queries biar logic parsing ada disini) ---
func ingestStreamCSV(r io.Reader, filename string, es *elasticsearch.Client) int {
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
		doc["full_text"] = strings.Join(txtBuf, " ")
		indexDocument(es, doc, generateFingerprint(doc["full_text"].(string)+filename))
		count++
	}
	return count
}

func ingestStreamText(r io.Reader, filename string, es *elasticsearch.Client) int {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024); scanner.Buffer(buf, 5*1024*1024)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 5 { continue }
		doc := map[string]interface{}{"leak_source": filename, "raw_content": line, "full_text": line}
		indexDocument(es, doc, generateFingerprint(line+filename))
		count++
	}
	return count
}