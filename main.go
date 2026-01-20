package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

func main() {
	// 1. CONFIG
	godotenv.Load()
	botToken := os.Getenv("BOT_TOKEN")
	ownerIDStr := os.Getenv("OWNER_ID")
	elasticURL := os.Getenv("ELASTIC_URL")
	if elasticURL == "" { elasticURL = "http://localhost:9200" }

	ownerID, _ := strconv.ParseInt(ownerIDStr, 10, 64)

	// 2. INIT
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{elasticURL}})
	if err != nil { log.Fatal("Gagal konek ES:", err) }

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil { log.Fatal("Gagal konek Telegram:", err) }
	bot.Debug = true
	log.Printf("🤖 Super Bot Modular Online: %s", bot.Self.UserName)

	// 3. MAIN LOOP
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil { continue }
		msg := update.Message
		chatID := msg.Chat.ID
		user := msg.From

		// --- ROUTING LOGIC ---

		// A. Fitur ADMIN (Audit & Upload)
		if user.ID == ownerID {
			// Audit
			if strings.HasPrefix(msg.Text, "/audit") {
				keyword := strings.TrimSpace(strings.Replace(msg.Text, "/audit", "", 1))
				if keyword == "" {
					bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/audit keyword`"))
				} else {
					handleAuditLog(bot, chatID, es, keyword)
				}
				continue
			}
			// Upload URL
			if strings.HasPrefix(msg.Text, "http") {
				logActivity(es, user, "UPLOAD_URL", msg.Text)
				handleURLUpload(bot, msg, es)
				continue
			}
			// Upload File
			if msg.Document != nil {
				logActivity(es, user, "UPLOAD_FILE", msg.Document.FileName)
				handleFileUpload(bot, msg, botToken, es)
				continue
			}
		}

		// B. Fitur USER (Export & Search)
		if strings.HasPrefix(msg.Text, "/export") {
			logActivity(es, user, "EXPORT", msg.Text)
			keyword := strings.TrimSpace(strings.Replace(msg.Text, "/export", "", 1))
			if keyword != "" {
				handleExport(bot, msg, es, keyword)
			}
			continue
		}

		if msg.Text != "" && !strings.HasPrefix(msg.Text, "/") {
			logActivity(es, user, "SEARCH", msg.Text)
			handleSearch(bot, msg, es)
		}
	}
}