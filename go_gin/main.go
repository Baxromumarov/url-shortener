package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

const (
	base62Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
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

func main() {
	service := &Service{Session: conn()}
	defer service.Session.Close()

	router := gin.Default()

	router.GET("/shorten", func(c *gin.Context) {
		longURL := c.Query("long_url")
		if longURL == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'long_url' parameter"})
			return
		}

		shortURL, exist, err := service.Find(longURL)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Error checking URL: %v", err)})
			return
		}

		if exist {
			c.JSON(http.StatusOK, gin.H{"short_url": "http://localhost:8080/" + shortURL})
			return
		}

		shortURL, err = service.GenerateShortKey(longURL)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error generating short URL"})
			return
		}

		if err := service.Insert(shortURL, longURL); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error saving URL to database"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"short_url": "http://localhost:8080/" + shortURL})
	})

	router.GET("/:shortURL", func(c *gin.Context) {
		shortURL := c.Param("shortURL")

		longURL, exist, err := service.findShortUrl(shortURL)
		if err != nil || !exist {
			c.JSON(http.StatusNotFound, gin.H{"error": "Short URL not found"})
			return
		}

		c.Redirect(http.StatusMovedPermanently, longURL)
	})

	if err := router.Run(":8080"); err != nil {
		log.Fatal("Failed to start server:", err)
	}
	fmt.Println("✅ Server started on :8080")
}
