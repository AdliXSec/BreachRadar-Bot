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