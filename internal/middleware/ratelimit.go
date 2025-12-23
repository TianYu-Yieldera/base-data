package middleware

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// RateLimiter stores request counts per client
type RateLimiter struct {
	clients map[string]*clientInfo
	mu      sync.RWMutex
	rps     int
	done    chan struct{}
}

type clientInfo struct {
	count    int
	lastSeen time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rps int) *RateLimiter {
	rl := &RateLimiter{
		clients: make(map[string]*clientInfo),
		rps:     rps,
		done:    make(chan struct{}),
	}

	// Cleanup routine with graceful shutdown support
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rl.cleanup()
			case <-rl.done:
				return
			}
		}
	}()

	return rl
}

// Stop stops the cleanup goroutine
func (rl *RateLimiter) Stop() {
	close(rl.done)
}

func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for ip, client := range rl.clients {
		if now.Sub(client.lastSeen) > time.Minute {
			delete(rl.clients, ip)
		}
	}
}

func (rl *RateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	client, exists := rl.clients[ip]

	if !exists {
		rl.clients[ip] = &clientInfo{
			count:    1,
			lastSeen: now,
		}
		return true
	}

	// Reset if more than 1 second has passed
	if now.Sub(client.lastSeen) > time.Second {
		client.count = 1
		client.lastSeen = now
		return true
	}

	// Check rate limit
	if client.count >= rl.rps {
		return false
	}

	client.count++
	return true
}

// RateLimitWithContext returns a gin middleware for rate limiting with context support
func RateLimitWithContext(ctx context.Context, rps int) gin.HandlerFunc {
	limiter := NewRateLimiter(rps)

	// Stop limiter when context is cancelled
	go func() {
		<-ctx.Done()
		limiter.Stop()
	}()

	return func(c *gin.Context) {
		ip := c.ClientIP()

		if !limiter.allow(ip) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    "RATE_LIMITED",
				"message": "Too many requests. Please try again later.",
			})
			return
		}

		c.Next()
	}
}

// RateLimit returns a gin middleware for rate limiting (for backwards compatibility)
func RateLimit(rps int) gin.HandlerFunc {
	limiter := NewRateLimiter(rps)

	return func(c *gin.Context) {
		ip := c.ClientIP()

		if !limiter.allow(ip) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    "RATE_LIMITED",
				"message": "Too many requests. Please try again later.",
			})
			return
		}

		c.Next()
	}
}
