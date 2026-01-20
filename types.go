package main

import "time"

// Struktur Respon Elasticsearch
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

// Struktur Log Aktivitas User
type UserActivity struct {
	Timestamp  time.Time `json:"timestamp"`
	UserID     string    `json:"user_id"`
	Username   string    `json:"username"`
	FirstName  string    `json:"first_name"`
	LastName   string    `json:"last_name"`
	ActionType string    `json:"action_type"`
	Query      string    `json:"query_content"`
}