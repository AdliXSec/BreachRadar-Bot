package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
)

// Struct sederhana untuk Rate Limiter (Disimpan di RAM)
type UserLimiter struct {
	Count     int
	ResetTime time.Time
}

func main() {
	// 1. CONFIG
	godotenv.Load()
	botToken := os.Getenv("BOT_TOKEN")
	ownerIDStr := os.Getenv("OWNER_ID")
	elasticURL := os.Getenv("ELASTIC_URL")
	if elasticURL == "" {
		elasticURL = "http://localhost:9200"
	}

	ownerID, _ := strconv.ParseInt(ownerIDStr, 10, 64)

	// 2. INIT
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{elasticURL}})
	if err != nil {
		log.Fatal("Gagal konek ES:", err)
	}

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatal("Gagal konek Telegram:", err)
	}
	bot.Debug = true
	log.Printf("🤖 Super Bot Enterprise Online: %s", bot.Self.UserName)

	globalConfig := getSystemConfig(es)
	log.Printf("⚙️ Config Loaded: Mode=%s, Limit=%d/min", globalConfig.Mode, globalConfig.RateLimit)

	rateLimitMap := make(map[int64]*UserLimiter)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}
		msg := update.Message
		chatID := msg.Chat.ID
		user := msg.From
		userIDStr := fmt.Sprintf("%d", user.ID)
		isAdmin := (user.ID == ownerID)

		if !isAdmin {
			if isUserBanned(es, userIDStr) {
				bot.Send(tgbotapi.NewMessage(chatID, "🚫 **AKSES DIBLOKIR**\nAkun Anda masuk dalam daftar hitam (Blacklist)."))
				continue
			}
		}

		if msg.Text == "/help" || msg.Text == "/start" {
			// Kita oper status isAdmin ke fungsi
			handleHelp(bot, chatID, isAdmin)
			continue
		}

		if user.ID == ownerID {
			// Handle /setlimit <angka>
			if strings.HasPrefix(msg.Text, "/setlimit") {
				parts := strings.Fields(msg.Text)
				if len(parts) < 2 {
					bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan: `/setlimit 300`"))
				} else {
					newLimit, err := strconv.Atoi(parts[1])
					if err != nil || newLimit < 1 {
						bot.Send(tgbotapi.NewMessage(chatID, "❌ Angka tidak valid."))
					} else {
						// Update Config di RAM & Database
						globalConfig.RateLimit = newLimit  // Update RAM
						saveSystemConfig(es, globalConfig) // Update DB
						bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("⚡ **LIMIT UPDATED**\nBatas request user: %d per menit.", newLimit)))
					}
				}
				continue
			}

			// Handle Mode /open /close
			if msg.Text == "/open" {
				globalConfig.Mode = "OPEN"         // Update RAM
				saveSystemConfig(es, globalConfig) // Update DB
				handleAccessControl(bot, chatID, es, msg.Text)
				continue
			}
			if msg.Text == "/close" {
				globalConfig.Mode = "CLOSE"        // Update RAM
				saveSystemConfig(es, globalConfig) // Update DB
				handleAccessControl(bot, chatID, es, msg.Text)
				continue
			}

			// Command Reset
			if msg.Text == "/delkey" {
				handleAccessControl(bot, chatID, es, msg.Text)
				continue
			}

			// Command Admin Lainnya
			switch msg.Text {
			case "/genkey":
				handleAccessControl(bot, chatID, es, msg.Text)
				continue
			case "/stats":
				handleStats(bot, chatID, es)
				continue
			case "/getusers":
				handleGetUsers(bot, chatID, es)
				continue
			}

			if strings.HasPrefix(msg.Text, "/broadcast") {
				handleBroadcast(bot, msg, es)
				continue
			}

			if strings.HasPrefix(msg.Text, "/notif") {
				handleNotification(bot, msg, es)
				continue
			}

			if strings.HasPrefix(msg.Text, "/sendto") {
				handleDirectMessage(bot, msg)
				continue
			}

			if strings.HasPrefix(msg.Text, "/ban") || strings.HasPrefix(msg.Text, "/unban") {
				cmd := strings.Split(msg.Text, " ")[0]
				handleBanSystem(bot, msg, es, cmd)
				continue
			}

			if strings.HasPrefix(msg.Text, "/audit") {
				keyword := strings.TrimSpace(strings.Replace(msg.Text, "/audit", "", 1))
				handleAuditLog(bot, chatID, es, keyword)
				continue
			}
			if strings.HasPrefix(msg.Text, "http") {
				logActivity(es, user, "UPLOAD_URL", msg.Text)
				handleURLUpload(bot, msg, es)
				continue
			}
			if msg.Document != nil {
				logActivity(es, user, "UPLOAD_FILE", msg.Document.FileName)
				handleFileUpload(bot, msg, botToken, es)
				continue
			}
		}

		if user.ID != ownerID {
			limiter, exists := rateLimitMap[user.ID]
			if !exists {
				limiter = &UserLimiter{Count: 0, ResetTime: time.Now().Add(1 * time.Minute)}
				rateLimitMap[user.ID] = limiter
			}
			if time.Now().After(limiter.ResetTime) {
				limiter.Count = 0
				limiter.ResetTime = time.Now().Add(1 * time.Minute)
			}

			// Gunakan globalConfig yang sudah terupdate
			if limiter.Count >= globalConfig.RateLimit {
				if limiter.Count == globalConfig.RateLimit {
					bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("⛔ **RATE LIMIT**\nBatas: %d request/menit.", globalConfig.RateLimit)))
				}
				limiter.Count++
				continue
			}
			limiter.Count++
		}

		isAuthorized := isUserAuthorized(es, userIDStr)

		// [FIX] Gunakan globalConfig yang selalu update
		canAccess := (user.ID == ownerID) || globalConfig.Mode == "OPEN" || isAuthorized

		if strings.HasPrefix(msg.Text, "/redeem") {
			handleRedeem(bot, msg, es)
			continue
		}

		if !canAccess {
			bot.Send(tgbotapi.NewMessage(chatID, "🔒 **AKSES DITOLAK**\nBot dalam mode PRIVAT. Silakan `/redeem` kode akses."))
			continue
		}

		// --- USER FEATURES ---

		if strings.HasPrefix(msg.Text, "/export") {
			logActivity(es, user, "EXPORT", msg.Text)
			keyword := strings.TrimSpace(strings.Replace(msg.Text, "/export", "", 1))
			if keyword != "" {
				handleExport(bot, msg, es, keyword)
			}
			continue
		}

		if strings.HasPrefix(msg.Text, "/s") {
			keyword := strings.TrimSpace(strings.Replace(msg.Text, "/s", "", 1))

			if keyword == "" {
				bot.Send(tgbotapi.NewMessage(chatID, "⚠️ Gunakan format: `/s <keyword>`\nContoh: `/s sudi` atau `/s email:sudi@gmail.com`"))
			} else {
				logActivity(es, user, "SEARCH", keyword) // Log keyword bersih
				handleSearch(bot, msg, es, keyword)      // Panggil fungsi dengan keyword
			}
			continue
		}
	}
}
