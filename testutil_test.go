package endure

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var (
	migrateOnce sync.Once
	migrateErr  error
)

// testPool connects to DATABASE_URL and skips the test when it is unset.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	migrateOnce.Do(func() { migrateErr = Migrate(context.Background(), pool) })
	require.NoError(t, migrateErr)
	return pool
}

// uniqueName keeps tests from claiming each other's leftover rows.
func uniqueName(prefix string) string { return prefix + "-" + uuid.NewString() }

func uuidString() string { return uuid.NewString() }

func testOptions() Options {
	return Options{LeaseDuration: 300 * time.Millisecond, PollInterval: 10 * time.Millisecond}
}

type runner struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startEngine(t *testing.T, e *Engine) *runner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		_ = e.Run(ctx)
	}()
	t.Cleanup(r.stop)
	return r
}

func (r *runner) stop() {
	r.cancel()
	<-r.done
}

func waitCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type counters struct {
	mu sync.Mutex
	m  map[string]int
}

func newCounters() *counters { return &counters{m: map[string]int{}} }

func (c *counters) inc(k string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k]++
	return c.m[k]
}

func (c *counters) get(k string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[k]
}
