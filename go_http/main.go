package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/gocql/gocql"
)

const (
	base62Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

	// Syclla config

	host        = "127.0.0.1"
	port        = 9042
	keyspace    = "url_shortener"
	consistency = gocql.Quorum
	timeout     = 5 * time.Second
)

func conn() *gocql.Session {
	cluster := gocql.NewCluster(host)
	cluster.Port = port
	cluster.Keyspace = keyspace
	cluster.Consistency = consistency
	cluster.Timeout = timeout

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Failed to connect to ScyllaDB:", err)
	}
	fmt.Println("✅ Connected to ScyllaDB!")

	return session
}

func HandleResponse(w http.ResponseWriter, body interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/json")

	bodyByte, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bodyByte)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(bodyByte)
}

type Shortener interface {
	Insert() error
	Find() (bool, error)
	GenerateShortKey() error
	ShortenURL() error
}

type Service struct {
	Session *gocql.Session
}

func (s *Service) GenerateShortKey(longURL string) (string, error) {
	hash := md5.Sum([]byte(longURL))
	hashStr := hex.EncodeToString(hash[:])
	partialHash := hashStr[:16]
	hashInt := new(big.Int)
	hashInt.SetString(partialHash, 16)

	var shortKey string
	base := big.NewInt(62)
	zero := big.NewInt(0)

	for hashInt.Cmp(zero) > 0 {
		rem := new(big.Int)
		hashInt.DivMod(hashInt, base, rem)
		shortKey = string(base62Chars[rem.Int64()]) + shortKey
	}

	if len(shortKey) > 8 {
		shortKey = shortKey[:8]
	} else if len(shortKey) < 6 {
		shortKey = fmt.Sprintf("%s%0*d", shortKey, 6-len(shortKey), 0)
	}

	return shortKey, nil
}

func (s *Service) Insert(shortURL, longURL string) error {
	query := `INSERT INTO urls (short_url, long_url) VALUES (?, ?)`
	err := s.Session.Query(query, shortURL, longURL).Exec()
	if err != nil {
		return fmt.Errorf("failed to insert URL into DB: %w", err)
	}
	return nil
}

func (s *Service) Find(longURL string) (string, bool, error) {
	var shortUrl string
	query := `SELECT short_url FROM urls WHERE long_url = ? LIMIT 1`
	err := s.Session.Query(query, longURL).Scan(&shortUrl)

	if err == gocql.ErrNotFound {
		return "", false, nil
	}

	if err != nil {
		return "", false, fmt.Errorf("failed to find URL: %w", err)
	}

	return shortUrl, true, nil
}

func (s *Service) shortenURLHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return

	}

	longURL := r.URL.Query().Get("long_url")
	if longURL == "" {
		http.Error(w, "Missing 'long_url' parameter", http.StatusBadRequest)
		return
	}

	shortURL, exist, err := s.Find(longURL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error checking URL: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if exist {
		HandleResponse(w, map[string]string{"short_url": "http://localhost:8080/" + shortURL}, http.StatusOK)
		return
	}

	shortURL, err = s.GenerateShortKey(longURL)
	if err != nil {
		http.Error(w, "Error generating short URL", http.StatusInternalServerError)
		return
	}

	if err := s.Insert(shortURL, longURL); err != nil {
		http.Error(w, "Error saving URL to database", http.StatusInternalServerError)
		return
	}

	HandleResponse(w, map[string]string{"short_url": "http://localhost:8080/" + shortURL}, http.StatusOK)

}
func (s *Service) findShortUrl(shortURL string) (string, bool, error) {
	var longURL string
	query := `SELECT long_url FROM urls WHERE short_url = ? LIMIT 1`
	err := s.Session.Query(query, shortURL).Scan(&longURL)

	if err == gocql.ErrNotFound {
		return "", false, nil
	}

	if err != nil {
		return "", false, fmt.Errorf("failed to find URL: %w", err)
	}
	return longURL, true, nil
}
func (s *Service) redirectHandler(w http.ResponseWriter, r *http.Request) {
	shortURL := r.URL.Path[len("/"):]

	longURL, exist, err := s.findShortUrl(shortURL)
	if err != nil || !exist {
		http.Error(w, "Short URL not found", http.StatusNotFound)
		return
	}

	// fmt.Println("Redirecting to:", longURL)
	http.Redirect(w, r, longURL, http.StatusMovedPermanently)
}

func main() {

	service := &Service{Session: conn()}
	defer service.Session.Close()

	defer func() {
		if err := recover(); err != nil {
			log.Println("Recovered from panic:", err)
		}
	}()

	http.HandleFunc("/shorten", service.shortenURLHandler)
	http.HandleFunc("/", service.redirectHandler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Failed to start server:", err)
	}
	fmt.Println("✅ Server started on :8080")

}
