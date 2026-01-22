package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"strconv"
)

type SystemStats struct {
	TotalRecords int64
	TotalSources int
	TotalUsers   int64
	TopSearches  []string
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
	if !ok { return config }
	
	// Parse Mode
	if m, ok := src["mode"].(string); ok { config.Mode = m }
	
	// Parse Limit (Handle float64 from JSON)
	if l, ok := src["rate_limit"].(float64); ok { config.RateLimit = int(l) }

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
	if err != nil || res.IsError() { return false }
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
	if err != nil || res.IsError() { return false }
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