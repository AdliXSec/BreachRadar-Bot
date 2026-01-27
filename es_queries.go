package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"strconv"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type SystemStats struct {
	TotalRecords int64
	TotalSources int
	TotalUsers   int64
	TopSearches  []string
}

type UserReport struct {
	UserID    string
	Username  string
	FirstName string
	LastName  string
	Status    string
}

type BlacklistEntry struct {
	UserID   string    `json:"user_id"`
	BannedAt time.Time `json:"banned_at"`
	Reason   string    `json:"reason"`
	BannedBy string    `json:"banned_by"`
}

// --- QUERY PENCARIAN DATA ---
func buildSearchQuery(keyword string, exactMatch bool) string {
	if strings.Contains(keyword, ":") {
		parts := strings.SplitN(keyword, ":", 2)
		rawKey := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		patternLower := fmt.Sprintf("*%s*", strings.ToLower(rawKey))
		patternUpper := fmt.Sprintf("*%s*", strings.ToUpper(rawKey))

		fuzziness := "AUTO"
		if exactMatch {
			fuzziness = "0"
		}

		return fmt.Sprintf(`{
			"query": {
				"multi_match": {
					"query": "%s",
					"fields": ["%s", "%s"], 
					"operator": "and", 
					"fuzziness": "%s" 
				}
			}
		}`, value, patternLower, patternUpper, fuzziness)
	}

	return fmt.Sprintf(`{
		"query": { "match": { "full_text": { "query": "%s", "operator": "and", "fuzziness": "AUTO" } } }
	}`, keyword)
}

func executeSearch(es *elasticsearch.Client, index string, queryBody string, size int) (*ESResponse, error) {
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(index),
		es.Search.WithBody(strings.NewReader(queryBody)),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithSize(size),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result ESResponse
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// --- LOGGING ---
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

// --- INGESTION (Insert Data) ---
func indexDocument(es *elasticsearch.Client, doc map[string]interface{}, id string) {
	body, _ := json.Marshal(doc)
	req := esapi.IndexRequest{
		Index:      "breach_data",
		DocumentID: id,
		Body:       bytes.NewReader(body),
		Refresh:    "false",
	}
	req.Do(context.Background(), es)
}

// --- ACCESS CONTROL MANAGEMENT ---

// 1. Cek Mode Sistem (Default OPEN jika belum diset)
// func getSystemMode(es *elasticsearch.Client) string {
// 	res, err := es.Get("system_config", "current_mode")
// 	if err != nil || res.IsError() {
// 		return "OPEN" // Default aman
// 	}
// 	defer res.Body.Close()

// 	var result map[string]interface{}
// 	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
// 		return "OPEN"
// 	}

// 	src, ok := result["_source"].(map[string]interface{})
// 	if !ok { return "OPEN" }

// 	return fmt.Sprintf("%v", src["mode"])
// }

// // 2. Set Mode Sistem
// func setSystemMode(es *elasticsearch.Client, mode string) {
// 	doc := SystemConfig{Mode: mode}
// 	body, _ := json.Marshal(doc)
// 	req := esapi.IndexRequest{
// 		Index:      "system_config",
// 		DocumentID: "current_mode",
// 		Body:       bytes.NewReader(body),
// 		Refresh:    "true",
// 	}
// 	req.Do(context.Background(), es)
// }

func getSystemConfig(es *elasticsearch.Client) SystemConfig {
	// Default Value
	config := SystemConfig{
		Mode:      "OPEN",
		RateLimit: 10,
	}

	res, err := es.Get("system_config", "current_config")
	if err != nil || res.IsError() {
		return config // Return default jika belum ada di DB
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return config
	}

	src, ok := result["_source"].(map[string]interface{})
	if !ok {
		return config
	}

	// Parse Mode
	if m, ok := src["mode"].(string); ok {
		config.Mode = m
	}

	// Parse Limit (Handle float64 from JSON)
	if l, ok := src["rate_limit"].(float64); ok {
		config.RateLimit = int(l)
	}

	return config
}

// Simpan Config (Mode & Limit)
func saveSystemConfig(es *elasticsearch.Client, config SystemConfig) {
	body, _ := json.Marshal(config)
	req := esapi.IndexRequest{
		Index:      "system_config",
		DocumentID: "current_config", // Satu ID untuk semua config
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}
	req.Do(context.Background(), es)
}

// 3. Simpan Key Baru
func saveAccessKey(es *elasticsearch.Client, key string) {
	doc := AccessKey{Key: key, CreatedAt: time.Now(), Active: true}
	body, _ := json.Marshal(doc)
	// Gunakan Key sebagai DocumentID agar pencarian cepat & mencegah duplikat
	req := esapi.IndexRequest{
		Index:      "access_keys",
		DocumentID: key,
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}
	req.Do(context.Background(), es)
}

// 4. Validasi & Pakai Key (Atomic Logic handled in handler usually, but here helper)
func getKeyStatus(es *elasticsearch.Client, key string) bool {
	res, err := es.Get("access_keys", key)
	if err != nil || res.IsError() {
		return false
	}
	return true // Jika key ditemukan (nanti dihapus setelah dipakai)
}

// 5. Whitelist User
func authorizeUser(es *elasticsearch.Client, userID string, key string) {
	doc := AuthorizedUser{UserID: userID, RedeemedAt: time.Now(), UsedKey: key}
	body, _ := json.Marshal(doc)
	req := esapi.IndexRequest{
		Index:      "authorized_users",
		DocumentID: userID,
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}
	req.Do(context.Background(), es)
}

// 6. Cek Apakah User Whitelisted?
func isUserAuthorized(es *elasticsearch.Client, userID string) bool {
	res, err := es.Get("authorized_users", userID)
	if err != nil || res.IsError() {
		return false
	}
	return true
}

// 7. Hapus Key (Dipakai saat redeem)
func deleteAccessKey(es *elasticsearch.Client, key string) {
	req := esapi.DeleteRequest{Index: "access_keys", DocumentID: key, Refresh: "true"}
	req.Do(context.Background(), es)
}

// 8. RESET TOTAL (/delkey)
func resetAllAccess(es *elasticsearch.Client) {
	// Hapus index keys dan authorized users
	esapi.IndicesDeleteRequest{Index: []string{"access_keys", "authorized_users"}}.Do(context.Background(), es)
}

func getClusterStats(es *elasticsearch.Client) SystemStats {
	var stats SystemStats

	// 1. Hitung Total Data (Breach Data)
	res, err := es.Count(es.Count.WithIndex("breach_data"))
	if err == nil && !res.IsError() {
		var countRes map[string]interface{}
		json.NewDecoder(res.Body).Decode(&countRes)

		// Safe Assertion: Cek dulu apakah "count" ada dan berupa angka
		if val, ok := countRes["count"].(float64); ok {
			stats.TotalRecords = int64(val)
		}
		res.Body.Close()
	} else {
		// Jika index belum ada, anggap 0 (Jangan Panic)
		stats.TotalRecords = 0
	}

	// 2. Hitung Total User Terdaftar (Whitelist)
	resUsers, err := es.Count(es.Count.WithIndex("authorized_users"))
	if err == nil && !resUsers.IsError() {
		var userRes map[string]interface{}
		json.NewDecoder(resUsers.Body).Decode(&userRes)

		// Safe Assertion lagi
		if val, ok := userRes["count"].(float64); ok {
			stats.TotalUsers = int64(val)
		}
		resUsers.Body.Close()
	} else {
		stats.TotalUsers = 0 // Index belum ada? Yasudah 0 user.
	}

	// 3. Aggregation: Hitung Total Source & Top Search
	queryBody := `{
		"size": 0,
		"aggs": {
			"unique_sources": {
				"cardinality": { "field": "leak_source.keyword" }
			},
			"top_keywords": {
				"terms": { "field": "query_content.keyword", "size": 3 }
			}
		}
	}`

	resAggs, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("breach_data", "user_logs"),
		es.Search.WithBody(strings.NewReader(queryBody)),
	)

	if err == nil && !resAggs.IsError() { // Tambahan cek !IsError()
		defer resAggs.Body.Close()
		var aggRes map[string]interface{}
		if err := json.NewDecoder(resAggs.Body).Decode(&aggRes); err == nil {

			// Parse Aggregations dengan pengecekan aman
			if aggregations, ok := aggRes["aggregations"].(map[string]interface{}); ok {

				// A. Total Sources
				if sources, ok := aggregations["unique_sources"].(map[string]interface{}); ok {
					if val, ok := sources["value"].(float64); ok {
						stats.TotalSources = int(val)
					}
				}

				// B. Top Search
				if topKeys, ok := aggregations["top_keywords"].(map[string]interface{}); ok {
					if buckets, ok := topKeys["buckets"].([]interface{}); ok {
						for _, b := range buckets {
							if bucket, ok := b.(map[string]interface{}); ok {
								stats.TopSearches = append(stats.TopSearches, fmt.Sprintf("%v", bucket["key"]))
							}
						}
					}
				}
			}
		}
	}

	return stats
}

func getAllVerifiedUserIDs(es *elasticsearch.Client) []int64 {
	var userIDs []int64

	// Query ambil semua data, hanya field 'user_id'
	queryBody := `{
		"_source": ["user_id"],
		"query": { "match_all": {} },
		"size": 10000 
	}`

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("authorized_users"),
		es.Search.WithBody(strings.NewReader(queryBody)),
	)

	if err != nil || res.IsError() {
		return userIDs
	}
	defer res.Body.Close()

	var result ESResponse
	json.NewDecoder(res.Body).Decode(&result)

	for _, hit := range result.Hits.Hits {
		// Parse UserID (yang disimpan sebagai string) balikin ke int64 buat Telegram
		uidStr := fmt.Sprintf("%v", hit.Source["user_id"])
		if uid, err := strconv.ParseInt(uidStr, 10, 64); err == nil {
			userIDs = append(userIDs, uid)
		}
	}
	return userIDs
}

func getAllUniqueLogUserIDs(es *elasticsearch.Client) []int64 {
	var userIDs []int64

	// Kita gunakan Aggregation "Terms" untuk mengelompokkan user_id yang sama
	// Size 10000 artinya kita ambil maksimal 10.000 user unik terakhir
	queryBody := `{
		"size": 0,
		"aggs": {
			"distinct_users": {
				"terms": { "field": "user_id.keyword", "size": 10000 }
			}
		}
	}`

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("user_logs"),
		es.Search.WithBody(strings.NewReader(queryBody)),
	)

	if err != nil || res.IsError() {
		return userIDs
	}
	defer res.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(res.Body).Decode(&result)

	// Parsing Aggregation Result
	if aggs, ok := result["aggregations"].(map[string]interface{}); ok {
		if distinct, ok := aggs["distinct_users"].(map[string]interface{}); ok {
			if buckets, ok := distinct["buckets"].([]interface{}); ok {
				for _, b := range buckets {
					item := b.(map[string]interface{})
					// Key adalah User ID (string)
					uidStr := fmt.Sprintf("%v", item["key"])

					if uid, err := strconv.ParseInt(uidStr, 10, 64); err == nil {
						userIDs = append(userIDs, uid)
					}
				}
			}
		}
	}
	return userIDs
}

func generateUserReport(es *elasticsearch.Client) []UserReport {
	// Map untuk menyimpan user unik (Key: UserID) agar tidak duplikat
	userMap := make(map[string]UserReport)

	// 1. Ambil Data VERIFIED USERS
	// (Kita query index authorized_users)
	// Namun, index ini cuma simpan ID & Tanggal Redeem. Kita butuh Nama/Username.
	// Trik: Kita ambil detail profilnya dari index 'user_logs' nanti.
	// Jadi langkah pertama: Tandai dulu siapa yang verified.
	verifiedIDs := make(map[string]bool)

	queryVerified := `{"query": { "match_all": {} }, "size": 10000}`
	resV, _ := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("authorized_users"),
		es.Search.WithBody(strings.NewReader(queryVerified)),
	)
	if resV != nil && !resV.IsError() {
		var res map[string]interface{}
		json.NewDecoder(resV.Body).Decode(&res)
		if hits, ok := res["hits"].(map[string]interface{}); ok {
			if hitList, ok := hits["hits"].([]interface{}); ok {
				for _, h := range hitList {
					src := h.(map[string]interface{})["_source"].(map[string]interface{})
					uid := fmt.Sprintf("%v", src["user_id"])
					verifiedIDs[uid] = true
				}
			}
		}
		resV.Body.Close()
	}

	// 2. Ambil Data PROFIL dari USER_LOGS
	// Kita gunakan Aggregation "Top Hits" untuk mengambil data profil TERBARU setiap user
	queryLogs := `{
		"size": 0,
		"aggs": {
			"users": {
				"terms": { "field": "user_id.keyword", "size": 10000 },
				"aggs": {
					"latest_data": {
						"top_hits": {
							"sort": [ { "timestamp": { "order": "desc" } } ],
							"_source": { "includes": [ "username", "first_name", "last_name" ] },
							"size": 1
						}
					}
				}
			}
		}
	}`

	resL, _ := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("user_logs"),
		es.Search.WithBody(strings.NewReader(queryLogs)),
	)

	if resL != nil && !resL.IsError() {
		var res map[string]interface{}
		json.NewDecoder(resL.Body).Decode(&res)

		if aggs, ok := res["aggregations"].(map[string]interface{}); ok {
			if users, ok := aggs["users"].(map[string]interface{}); ok {
				if buckets, ok := users["buckets"].([]interface{}); ok {
					for _, b := range buckets {
						bucket := b.(map[string]interface{})
						uid := fmt.Sprintf("%v", bucket["key"])

						// Ambil detail nama dari latest_data
						var uName, fName, lName string
						if latest, ok := bucket["latest_data"].(map[string]interface{}); ok {
							if hits, ok := latest["hits"].(map[string]interface{}); ok {
								if list, ok := hits["hits"].([]interface{}); ok && len(list) > 0 {
									src := list[0].(map[string]interface{})["_source"].(map[string]interface{})
									uName = fmt.Sprintf("%v", src["username"])
									fName = fmt.Sprintf("%v", src["first_name"])
									lName = fmt.Sprintf("%v", src["last_name"])
								}
							}
						}

						// Tentukan Status
						status := "GUEST"
						if verifiedIDs[uid] {
							status = "VERIFIED"
						}

						userMap[uid] = UserReport{
							UserID:    uid,
							Username:  uName,
							FirstName: fName,
							LastName:  lName,
							Status:    status,
						}
					}
				}
			}
		}
		resL.Body.Close()
	}

	// Konversi Map ke Slice
	var report []UserReport
	for _, u := range userMap {
		report = append(report, u)
	}
	return report
}

func isUserBanned(es *elasticsearch.Client, userID string) bool {
	// Kita gunakan UserID sebagai Document ID agar pengecekan sangat cepat (O(1))
	res, err := es.Get("user_blacklist", userID)

	// Jika error atau 404 Not Found, berarti TIDAK di-ban
	if err != nil || res.IsError() {
		return false
	}
	return true
}

func banUser(es *elasticsearch.Client, userID string, reason string) {
	entry := BlacklistEntry{
		UserID:   userID,
		BannedAt: time.Now(),
		Reason:   reason,
		BannedBy: "ADMIN",
	}
	body, _ := json.Marshal(entry)

	req := esapi.IndexRequest{
		Index:      "user_blacklist",
		DocumentID: userID, // ID Dokumen = ID User
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}
	req.Do(context.Background(), es)
}

func unbanUser(es *elasticsearch.Client, userID string) {
	req := esapi.DeleteRequest{
		Index:      "user_blacklist",
		DocumentID: userID,
		Refresh:    "true",
	}
	req.Do(context.Background(), es)
}

func deleteBySource(es *elasticsearch.Client, filename string) int {
	// Query: Hapus semua data yang leak_source == filename
	query := fmt.Sprintf(`{
		"query": {
			"term": { "leak_source.keyword": "%s" }
		}
	}`, filename)

	req := esapi.DeleteByQueryRequest{
		Index:   []string{"breach_data"},
		Body:    strings.NewReader(query),
		Refresh: boolPtr(true), // Paksa refresh index agar data hilang seketika
	}

	res, err := req.Do(context.Background(), es)
	if err != nil || res.IsError() {
		return 0
	}
	defer res.Body.Close()

	// Parse Response untuk mengambil jumlah "deleted"
	var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return 0
	}

	// Ambil field "deleted" dari JSON Elasticsearch
	if deleted, ok := response["deleted"].(float64); ok {
		return int(deleted)
	}

	return 0
}

func boolPtr(b bool) *bool {
	return &b
}
