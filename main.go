package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()
)

// Sliding Window Lua Script
const rateLimitLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local clear_before = now - window

redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)
local current_requests = redis.call('ZCARD', key)

if current_requests < limit then
    redis.call('ZADD', key, now, now)
    redis.call('PEXPIRE', key, window)
    return 1
else
    return 0
end
`

type PaymentRequest struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type PaymentResponse struct {
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	ProcessedAt   time.Time `json:"processed_at"`
}

func main() {
	// Connect to PostgreSQL
	dbDSN := os.Getenv("DATABASE_URL")
	if dbDSN == "" {
		dbDSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	var err error
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", dbDSN)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		log.Printf("Waiting for Postgres... error: %v", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not connect to Postgres: %v", err)
	}
	log.Println("Connected to Postgres.")

	// Connect to Redis
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	for i := 0; i < 10; i++ {
		_, err = redisClient.Ping(ctx).Result()
		if err == nil {
			break
		}
		log.Printf("Waiting for Redis... error: %v", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	log.Println("Connected to Redis.")

	// Setup HTTP Handlers
	http.HandleFunc("/payments", handlePayments)
	http.Handle("/", http.FileServer(http.Dir("./public")))

	log.Println("Payment Processing Server listening on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handlePayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Sliding Window Rate Limiting (10 requests per 10 seconds)
	clientID := r.Header.Get("X-Client-Id")
	if clientID == "" {
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		clientID = ip
	}

	limitKey := fmt.Sprintf("rate_limit:%s", clientID)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	windowMs := int64(10000) // 10 seconds
	limit := int64(10)       // max 10 requests

	allowed, err := redisClient.Eval(ctx, rateLimitLua, []string{limitKey}, now, windowMs, limit).Int()
	if err != nil {
		log.Printf("Rate limiter Redis error: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	log.Printf("ClientID: %s, LimitKey: %s, Now: %d, WindowMs: %d, Limit: %d, Allowed: %d", clientID, limitKey, now, windowMs, limit, allowed)

	if allowed == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]string{"error": "Too Many Requests (Rate limit exceeded)"})
		return
	}

	// 2. Idempotency Key Check
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		http.Error(w, "Missing Idempotency-Key header", http.StatusBadRequest)
		return
	}
	keyUUID, err := uuid.Parse(idempotencyKey)
	if err != nil {
		http.Error(w, "Invalid Idempotency-Key format (must be UUID)", http.StatusBadRequest)
		return
	}

	// Check if idempotency key exists in DB (Atomic Check-and-Create concept)
	var cachedCode int
	var cachedBody string
	err = db.QueryRow("SELECT response_code, response_body FROM idempotency_keys WHERE key_uuid = $1", keyUUID).Scan(&cachedCode, &cachedBody)
	if err == nil {
		// Key exists, return cached response
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.WriteHeader(cachedCode)
		w.Write([]byte(cachedBody))
		return
	} else if err != sql.ErrNoRows {
		log.Printf("Database check error: %v", err)
		http.Error(w, "Internal Database Error", http.StatusInternalServerError)
		return
	}

	// 3. Process new payment
	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid Request Body", http.StatusBadRequest)
		return
	}

	// Simulate payment logic execution time
	time.Sleep(100 * time.Millisecond)

	resp := PaymentResponse{
		TransactionID: fmt.Sprintf("TXN-%d", rand.Int63n(9000000000)+1000000000),
		Status:        "SUCCESS",
		Amount:        req.Amount,
		Currency:      req.Currency,
		ProcessedAt:   time.Now().UTC(),
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// 4. Atomic SQL Transaction to Write Idempotency Key
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Internal Database Error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec("INSERT INTO idempotency_keys (key_uuid, response_code, response_body) VALUES ($1, $2, $3)", keyUUID, http.StatusOK, string(respBytes))
	if err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" { // unique_violation
			// A concurrent request finished first. Get the result from it.
			tx.Rollback()
			err = db.QueryRow("SELECT response_code, response_body FROM idempotency_keys WHERE key_uuid = $1", keyUUID).Scan(&cachedCode, &cachedBody)
			if err != nil {
				http.Error(w, "Internal Server Error during race recovery", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(cachedCode)
			w.Write([]byte(cachedBody))
			return
		}
		http.Error(w, "Internal Database Error during write", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Internal Database Error during commit", http.StatusInternalServerError)
		return
	}

	// Return new success response
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}
