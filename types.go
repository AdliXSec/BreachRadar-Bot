package main

import "time"

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

type UserActivity struct {
	Timestamp  time.Time `json:"timestamp"`
	UserID     string    `json:"user_id"`
	Username   string    `json:"username"`
	FirstName  string    `json:"first_name"`
	LastName   string    `json:"last_name"`
	ActionType string    `json:"action_type"`
	Query      string    `json:"query_content"`
}

type AccessKey struct {
	Key       string    `json:"key"`
	CreatedAt time.Time `json:"created_at"`
	Active    bool      `json:"active"`
}

type AuthorizedUser struct {
	UserID    string    `json:"user_id"`
	RedeemedAt time.Time `json:"redeemed_at"`
	UsedKey   string    `json:"used_key"`
}

type SystemConfig struct {
	Mode      string `json:"mode"`       // "OPEN" atau "CLOSE"
	RateLimit int    `json:"rate_limit"` // Contoh: 10, 60, 300
}