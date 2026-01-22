package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func handleSearch(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	query := msg.Text
	chatID := msg.Chat.ID
	loading, _ := bot.Send(tgbotapi.NewMessage(chatID, "🔍 _Sedang mencari..._"))

	// Gunakan fungsi dari es_queries.go
	esQuery := buildSearchQuery(query, true)
	result, err := executeSearch(es, "breach_data", esQuery, 10) // Ambil 10

	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Error Database."))
		return
	}

	totalFound := result.Hits.Total.Value
	var replyText string

	if totalFound > 0 {
		replyText = fmt.Sprintf("🚨 *DATA FOUND!*\nKeyword: `%s`\nResult: %d Data\n\n", escapeMarkdown(query), totalFound)
		for i, hit := range result.Hits.Hits {
			if i >= 5 { break } // Limit tampilan chat
			replyText += "📂 *RECORD:*\n"
			for k, v := range hit.Source {
				if k == "full_text" || k == "raw_content" || k == "upload_date" || k == "leak_source" { continue }
				valStr := fmt.Sprintf("%v", v)
				if isSensitive(k) { valStr = maskPassword(valStr) }
				replyText += fmt.Sprintf("▪️ `%s`: `%s`\n", escapeMarkdown(strings.ToUpper(k)), escapeMarkdown(valStr))
			}
			sourceName := fmt.Sprintf("%v", hit.Source["leak_source"])
			replyText += fmt.Sprintf("📁 Source: `%s`\n", escapeMarkdown(sourceName))
			replyText += "------------------\n"
		}
		if totalFound > 5 {
			replyText += fmt.Sprintf("_(...%d data lainnya. Gunakan /export untuk download)_", totalFound-5)
		}
	} else {
		replyText = fmt.Sprintf("✅ *AMAN!*\nNihil: `%s`", escapeMarkdown(query))
	}

	bot.Request(tgbotapi.NewDeleteMessage(chatID, loading.MessageID))
	msgRep := tgbotapi.NewMessage(chatID, replyText)
	msgRep.ParseMode = "Markdown"
	bot.Send(msgRep)
}

func handleExport(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client, keyword string) {
	chatID := msg.Chat.ID
	bot.Send(tgbotapi.NewMessage(chatID, "📄 _Menyiapkan file laporan..._"))

	// 1. Query ES
	esQuery := buildSearchQuery(keyword, true)
	result, err := executeSearch(es, "breach_data", esQuery, 1000)

	if err != nil || result.Hits.Total.Value == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gagal export atau data kosong."))
		return
	}

	// 2. ANALISA KOLOM (Cari semua kemungkinan kolom)
	// Kita pakai Map agar tidak ada duplikat nama kolom
	headerMap := make(map[string]bool)

	for _, hit := range result.Hits.Hits {
		for k := range hit.Source {
			// Kita skip field internal yang bikin CSV berantakan/berat
			if k == "full_text" || k == "raw_content" { 
				continue 
			}
			headerMap[k] = true
		}
	}

	// 3. URUTKAN HEADER (Agar konsisten A-Z)
	var headers []string
	for k := range headerMap {
		headers = append(headers, k)
	}
	sort.Strings(headers) // Wajib sort agar urutan tidak acak!

	// 4. BUAT CSV
	b := &bytes.Buffer{}
	w := csv.NewWriter(b)

	// Tulis Header Sekali Saja
	w.Write(headers)

	// Tulis Isi Data (Mapping Sesuai Header)
	for _, hit := range result.Hits.Hits {
		src := hit.Source
		var record []string

		// Loop berdasarkan HEADERS yang sudah diurutkan (Bukan loop map src)
		for _, colName := range headers {
			val, exists := src[colName]
			if exists {
				// Ambil datanya jika ada
				record = append(record, fmt.Sprintf("%v", val))
			} else {
				// Isi kosong jika data ini tidak punya kolom tersebut
				record = append(record, "")
			}
		}
		w.Write(record)
	}
	w.Flush()

	// 5. KIRIM FILE
	fileName := fmt.Sprintf("result_%s.csv", strings.ReplaceAll(keyword, " ", "_"))
	fileBytes := tgbotapi.FileBytes{Name: fileName, Bytes: b.Bytes()}
	docMsg := tgbotapi.NewDocument(chatID, fileBytes)
	docMsg.Caption = fmt.Sprintf("✅ Export Selesai: %d data", len(result.Hits.Hits))
	bot.Send(docMsg)
}

func handleRedeem(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, es *elasticsearch.Client) {
	chatID := msg.Chat.ID
	input := strings.TrimSpace(strings.Replace(msg.Text, "/redeem", "", 1))
	input = strings.TrimSpace(input) // Bersihkan spasi
	
	if input == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan format: `/redeem BR-XXXXX`"))
		return
	}

	// 1. Cek Apakah Key Valid?
	if getKeyStatus(es, input) {
		// 2. Masukkan User ke Whitelist
		userID := fmt.Sprintf("%d", msg.From.ID)
		authorizeUser(es, userID, input)

		// 3. Hapus Key (Agar tidak bisa dipakai orang lain)
		deleteAccessKey(es, input)

		bot.Send(tgbotapi.NewMessage(chatID, "✅ **AKSES DITERIMA!**\nSelamat, Anda sekarang bisa menggunakan bot ini sepuasnya."))
	} else {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ **KEY INVALID**\nKode salah atau sudah digunakan."))
	}
}