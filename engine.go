// Package endure provides durable execution of ordinary Go functions.
//
// A workflow is a Go function whose side effects are wrapped in Step calls. Each step's result
// is checkpointed; when a worker dies, another worker re-runs the function and completed steps
// return their stored results instead of executing again. Step execution is at-least-once:
// a crash between a step's side effect and its checkpoint re-runs that step, so side effects
// should be idempotent (see StepInfo.IdempotencyKey).
//
// Step, Sleep, Wait and Defer names must be stable and unique within a workflow, and the
// workflow function must make the same sequence of calls on every execution. The name prefixes
// "sleep:", "signal:" and "compensate:" are reserved.
package endure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/appex-uk/go-endure/storage"
	"github.com/appex-uk/go-endure/storage/postgres"
)

// Options configure an Engine. Zero values select defaults.
type Options struct {
	WorkerID      string        // default: random UUID
	LeaseDuration time.Duration // default: 30s
	PollInterval  time.Duration // default: 500ms
	MaxConcurrent int           // default: 16
	Logger        *slog.Logger  // default: slog.Default()
}

// Engine registers workflows, starts them and runs the worker loop.
type Engine struct {
	st   *store
	opts Options

	mu   sync.RWMutex
	regs map[string]registration

	wake chan struct{}

	// fault is a test hook; returning true simulates process death at the given point.
	fault func(workflowID, point string) bool
}

type registration struct {
	run func(ctx Context, input []byte) ([]byte, error)
}

// New creates an Engine backed by PostgreSQL. Call Migrate once before use.
// Use NewWithStorage to select SQLite, filesystem or a custom storage adapter.
func New(pool *pgxpool.Pool, opts Options) *Engine {
	return NewWithStorage(postgres.New(pool), opts)
}

// NewWithStorage creates an Engine using storage. Call backend.Migrate once before use,
// or set AutoMigrate when opening a built-in adapter with OpenStorage.
func NewWithStorage(backend storage.Storage, opts Options) *Engine {
	if backend == nil {
		panic("endure: nil storage")
	}
	if opts.WorkerID == "" {
		opts.WorkerID = uuid.NewString()
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = 30 * time.Second
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 500 * time.Millisecond
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 16
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{
		st:   &store{backend: backend},
		opts: opts,
		regs: map[string]registration{},
		wake: make(chan struct{}, 1),
	}
}

func (e *Engine) register(name string, r registration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.regs[name]; ok {
		panic(fmt.Sprintf("endure: workflow %q registered twice", name))
	}
	e.regs[name] = r
}

func (e *Engine) lookup(name string) (registration, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.regs[name]
	return r, ok
}

func (e *Engine) names() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.regs))
	for n := range e.regs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (e *Engine) crashAt(workflowID, point string) error {
	if e.fault != nil && e.fault(workflowID, point) {
		return errCrash
	}
	return nil
}

// Signal delivers a named signal to a workflow, waking it if it is waiting. Only the first
// signal per name is kept.
func (e *Engine) Signal(ctx context.Context, workflowID, name string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("endure: encode signal: %w", err)
	}
	return e.st.signal(ctx, workflowID, name, b)
}

// Run claims and executes workflows until ctx is cancelled. In-flight executions are
// cancelled and released for other workers before Run returns.
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	var inflight int
	var mu sync.Mutex
	names := e.names()
	ticker := time.NewTicker(e.opts.PollInterval)
	defer ticker.Stop()

	for {
		mu.Lock()
		free := e.opts.MaxConcurrent - inflight
		mu.Unlock()

		if free > 0 {
			claims, err := e.st.claim(ctx, e.opts.WorkerID, e.opts.LeaseDuration, names, free)
			if err != nil {
				if ctx.Err() == nil {
					e.opts.Logger.Error("endure: claim failed", "error", err)
				}
				claims = nil
			}
			for _, c := range claims {
				mu.Lock()
				inflight++
				mu.Unlock()
				wg.Add(1)
				go func() {
					defer wg.Done()
					e.execute(ctx, c)
					mu.Lock()
					inflight--
					mu.Unlock()
					select {
					case e.wake <- struct{}{}:
					default:
					}
				}()
			}
			if len(claims) == free {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-e.wake:
		}
	}
}

func (e *Engine) execute(parent context.Context, c claimed) {
	log := e.opts.Logger.With("workflow_id", c.id, "workflow", c.name, "attempt", c.attempt)

	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		e.heartbeat(hbCtx, cancel, c.lease, log)
	}()
	defer func() {
		stopHeartbeat()
		<-hbDone
	}()

	reg, ok := e.lookup(c.name)
	if !ok {
		return
	}
	ex := &execution{Context: ctx, e: e, lease: c.lease, attempt: c.attempt, names: map[string]struct{}{}}

	out, err := e.runWorkflow(reg, ex, c.input)

	// Terminal writes must survive shutdown of the worker context.
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer wcancel()

	var susp *suspendError
	switch {
	case errors.Is(err, errCrash):
		return
	case errors.As(err, &susp):
		if susp.waitingOn != "" {
			err = e.st.waitFor(wctx, c.lease, susp.waitingOn)
		} else {
			err = e.st.requeue(wctx, c.lease, susp.until)
		}
	case ctx.Err() != nil:
		if errors.Is(context.Cause(ctx), ErrLeaseLost) {
			log.Warn("endure: lease lost, abandoning execution")
			return
		}
		err = e.st.requeue(wctx, c.lease, time.Now())
	case err == nil:
		err = e.st.finish(wctx, c.lease, "completed", out, "")
	default:
		err = e.st.finish(wctx, c.lease, "failed", nil, err.Error())
	}
	if err != nil && !errors.Is(err, ErrLeaseLost) {
		log.Error("endure: persist outcome failed; lease will expire and the workflow will be retried", "error", err)
	}
}

func (e *Engine) heartbeat(ctx context.Context, cancel context.CancelCauseFunc, l lease, log *slog.Logger) {
	t := time.NewTicker(e.opts.LeaseDuration / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := e.st.renew(ctx, l, e.opts.LeaseDuration)
			if errors.Is(err, ErrLeaseLost) {
				cancel(ErrLeaseLost)
				return
			}
			if err != nil && ctx.Err() == nil {
				log.Warn("endure: lease renewal failed", "error", err)
			}
		}
	}
}

// runWorkflow runs the workflow function and, if it fails, its registered compensations in reverse order.
func (e *Engine) runWorkflow(reg registration, ex *execution, input []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("panic: %v", r)
		}
	}()

	out, err = reg.run(ex, input)
	if err == nil || ex.interrupted(err) {
		return out, err
	}
	if cerr := ex.compensate(); cerr != nil {
		if ex.interrupted(cerr) {
			return nil, cerr
		}
		err = errors.Join(err, cerr)
	}
	return nil, err
}
