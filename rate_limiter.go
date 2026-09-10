package garden

import (
	"context"
	"sync"
	"time"
)

type rateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	fillRate   float64
	lastUpdate time.Time
}

func newRateLimiter(limit int, interval time.Duration) *rateLimiter {
	rate := float64(limit) / interval.Seconds()
	return &rateLimiter{
		tokens:     float64(limit),
		maxTokens:  float64(limit),
		fillRate:   rate,
		lastUpdate: time.Now(),
	}
}

func (r *rateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()

	now := time.Now()
	elapsed := now.Sub(r.lastUpdate).Seconds()

	if elapsed > 0 {
		r.tokens += elapsed * r.fillRate
		if r.tokens > r.maxTokens {
			r.tokens = r.maxTokens
		}
		r.lastUpdate = now
	}

	r.tokens -= 1.0

	var waitTime time.Duration
	if r.tokens < 0 {
		deficit := -r.tokens
		waitTime = time.Duration((deficit / r.fillRate) * float64(time.Second))
	}

	r.mu.Unlock()

	if waitTime > 0 {
		select {
		case <-time.After(waitTime):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
