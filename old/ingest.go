package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256" // Library untuk Hashing
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
)

func main() {
	// 1. Setup Elasticsearch
	cfg := elasticsearch.Config{Addresses: []string{"http://localhost:9200"}}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	dataFolder := "./leaks_data"
	files, err := os.ReadDir(dataFolder)
	if err != nil {
		log.Fatalf("❌ Gagal membaca folder '%s'. Buat folder itu dulu!", dataFolder)
	}

	log.Printf("📂 Memproses folder %s dengan fitur ANTI-DUPLIKASI...", dataFolder)

	for _, file := range files {
		if file.IsDir() { continue }
		
		filePath := filepath.Join(dataFolder, file.Name())
		ext := strings.ToLower(filepath.Ext(file.Name()))

		log.Printf("➡️ Memproses file: %s", file.Name())

		if ext == ".csv" {
			processCSV(es, filePath, file.Name())
		} else if ext == ".sql" || ext == ".txt" {
			processTextBased(es, filePath, file.Name())
		}
	}
}

// --- FUNGSI GENERATE FINGERPRINT (RAHASIA DEDUPLIKASI) ---
func generateFingerprint(data string) string {
	// Kita hash string input menggunakan SHA-256
	// Hasilnya adalah ID unik yang konsisten
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func processCSV(es *elasticsearch.Client, path string, filename string) {
	f, _ := os.Open(path)
	defer f.Close()

	reader := csv.NewReader(f)
	headers, err := reader.Read() 
	if err != nil {
		log.Println("❌ Gagal baca header CSV:", err)
		return
	}

	records, _ := reader.ReadAll()

	for i, record := range records {
		doc := make(map[string]interface{})
		doc["leak_source"] = filename
		doc["upload_date"] = "2026-01-20"

		var textBuffer []string

		// Ambil semua isi kolom
		for j, val := range record {
			if j < len(headers) {
				cleanHeader := strings.TrimSpace(headers[j])
				doc[cleanHeader] = val
				textBuffer = append(textBuffer, val)
			}
		}
		
		// Gabung semua teks jadi satu string panjang
		fullContent := strings.Join(textBuffer, " ")
		doc["full_text"] = fullContent

		// --- LOGIC ANTI-DUPLIKASI ---
		// Kita buat ID berdasarkan isi konten + nama file
		// Jika konten sama persis di file yang sama, ID-nya akan sama
		uniqueID := generateFingerprint(fullContent + filename)

		sendToES(es, doc, uniqueID)
		
		if i%1000 == 0 { fmt.Print(".") }
	}
	fmt.Println(" Done!")
}

func processTextBased(es *elasticsearch.Client, path string, filename string) {
	f, _ := os.Open(path)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 5 { continue }

		doc := make(map[string]interface{})
		doc["leak_source"] = filename
		doc["raw_content"] = line
		doc["full_text"] = line
		
		// --- LOGIC ANTI-DUPLIKASI ---
		uniqueID := generateFingerprint(line + filename)
		
		sendToES(es, doc, uniqueID)
		
		lineCount++
		if lineCount%1000 == 0 { fmt.Print(".") }
	}
	fmt.Println(" Done!")
}

func sendToES(es *elasticsearch.Client, doc map[string]interface{}, docID string) {
	jsonBody, _ := json.Marshal(doc)
	
	req := esapi.IndexRequest{
		Index:      "breach_data",
		DocumentID: docID,            // <--- KUNCI DEDUPLIKASI: Kita paksa pakai ID ini
		Body:       bytes.NewReader(jsonBody),
		Refresh:    "false",
	}
	req.Do(context.Background(), es)
}