package endure

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/appex-uk/go-endure/storage"
)

func TestStorageBackendsImplementCommonContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(root string) storage.Config
	}{
		{
			name: "filesystem",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendFile,
					File:    storage.FileConfig{Path: filepath.Join(root, "state")},
				}
			},
		},
		{
			name: "sqlite",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendSQLite,
					SQLite:  storage.SQLiteConfig{Path: filepath.Join(root, "endure.sqlite")},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			st, err := OpenStorage(ctx, tt.config(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() {
				closer, ok := st.(io.Closer)
				if ok {
					require.NoError(t, closer.Close())
				}
			})
			require.NoError(t, st.Migrate(ctx))

			const (
				workflowID = "workflow-1"
				name       = "test-workflow"
			)
			input := []byte(`{"value":1}`)
			require.NoError(t, st.CreateWorkflow(ctx, workflowID, name, input))
			require.NoError(t, st.CreateWorkflow(ctx, workflowID, name, []byte(`{"value":2}`)))
			require.ErrorIs(t, st.CreateWorkflow(ctx, workflowID, "other-workflow", input), ErrNameMismatch)

			claims, err := st.ClaimWorkflows(ctx, storage.ClaimRequest{
				WorkerID: "worker-1",
				LeaseFor: time.Minute,
				Names:    []string{name},
				Limit:    1,
			})
			require.NoError(t, err)
			require.Len(t, claims, 1)
			assert.Equal(t, workflowID, claims[0].ID)
			assert.Equal(t, input, claims[0].Input)
			assert.Equal(t, 1, claims[0].Attempt)

			firstLease := claims[0].Lease
			require.NoError(t, st.RenewLease(ctx, firstLease, 2*time.Minute))
			require.NoError(t, st.WriteStep(ctx, firstLease, storage.StepWrite{
				Name:     "step-1",
				Status:   storage.StepCompleted,
				Attempts: 1,
				Output:   []byte(`"done"`),
			}))

			step, err := st.GetStep(ctx, workflowID, "step-1")
			require.NoError(t, err)
			require.NotNil(t, step)
			assert.Equal(t, storage.StepCompleted, step.Status)
			assert.Equal(t, []byte(`"done"`), step.Output)

			require.NoError(t, st.SignalWorkflow(ctx, workflowID, "approval", []byte(`true`)))
			payload, ok, err := st.GetSignal(ctx, workflowID, "approval")
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, []byte(`true`), payload)

			require.NoError(t, st.WaitForSignal(ctx, firstLease, "approval"))
			require.ErrorIs(t, st.WriteStep(ctx, firstLease, storage.StepWrite{
				Name:   "too-late",
				Status: storage.StepCompleted,
			}), ErrLeaseLost)

			claims, err = st.ClaimWorkflows(ctx, storage.ClaimRequest{
				WorkerID: "worker-2",
				LeaseFor: time.Minute,
				Names:    []string{name},
				Limit:    1,
			})
			require.NoError(t, err)
			require.Len(t, claims, 1)
			assert.Equal(t, 2, claims[0].Attempt)
			require.NoError(t, st.FinishWorkflow(ctx, claims[0].Lease, storage.WorkflowCompleted, []byte(`"finished"`), ""))

			workflow, err := st.GetWorkflow(ctx, workflowID)
			require.NoError(t, err)
			require.NotNil(t, workflow)
			assert.Equal(t, storage.WorkflowCompleted, workflow.Status)
			assert.Equal(t, []byte(`"finished"`), workflow.Output)
			assert.Empty(t, workflow.Error)

			require.ErrorIs(t, st.SignalWorkflow(ctx, "missing", "approval", nil), ErrNotFound)
		})
	}
}

func TestEngineRunsWithEveryLocalStorageBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(root string) storage.Config
	}{
		{
			name: "filesystem",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendFile,
					File:    storage.FileConfig{Path: filepath.Join(root, "state")},
				}
			},
		},
		{
			name: "sqlite",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendSQLite,
					SQLite:  storage.SQLiteConfig{Path: filepath.Join(root, "endure.sqlite")},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st, err := OpenStorage(t.Context(), tt.config(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() {
				closer, ok := st.(io.Closer)
				if ok {
					require.NoError(t, closer.Close())
				}
			})
			require.NoError(t, st.Migrate(t.Context()))

			var mu sync.Mutex
			executions := 0
			e := NewWithStorage(st, testOptions())
			wf := Register(e, "local-"+tt.name, func(ctx Context, input string) (string, error) {
				return Step(ctx, "work", func(context.Context) (string, error) {
					mu.Lock()
					executions++
					mu.Unlock()
					return input + "-done", nil
				})
			})
			startEngine(t, e)

			handle, err := wf.Start(t.Context(), "local-workflow", "input")
			require.NoError(t, err)
			output, err := handle.Wait(waitCtx(t))
			require.NoError(t, err)
			assert.Equal(t, "input-done", output)

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 1, executions)
		})
	}
}

func TestOpenStorageRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config storage.Config
		target error
	}{
		{
			name:   "requires a backend",
			config: storage.Config{},
			target: storage.ErrInvalidConfig,
		},
		{
			name: "rejects an unknown backend",
			config: storage.Config{
				Backend: "other",
			},
			target: storage.ErrUnsupportedBackend,
		},
		{
			name: "requires a filesystem path",
			config: storage.Config{
				Backend: storage.BackendFile,
			},
			target: storage.ErrInvalidConfig,
		},
		{
			name: "requires a PostgreSQL DSN",
			config: storage.Config{
				Backend: storage.BackendPostgres,
			},
			target: storage.ErrInvalidConfig,
		},
		{
			name: "requires a SQLite path",
			config: storage.Config{
				Backend: storage.BackendSQLite,
			},
			target: storage.ErrInvalidConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := OpenStorage(t.Context(), tt.config)
			require.Error(t, err)
			assert.True(t, errors.Is(err, tt.target))
		})
	}
}

func TestStorageBackendsSerializeClaimsAcrossAdapterInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(root string) storage.Config
	}{
		{
			name: "filesystem",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendFile,
					File:    storage.FileConfig{Path: filepath.Join(root, "state")},
				}
			},
		},
		{
			name: "sqlite",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendSQLite,
					SQLite:  storage.SQLiteConfig{Path: filepath.Join(root, "endure.sqlite")},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			config := tt.config(t.TempDir())
			first, err := OpenStorage(ctx, config)
			require.NoError(t, err)
			second, err := OpenStorage(ctx, config)
			require.NoError(t, err)
			t.Cleanup(func() {
				closer, ok := first.(io.Closer)
				if ok {
					require.NoError(t, closer.Close())
				}
				closer, ok = second.(io.Closer)
				if ok {
					require.NoError(t, closer.Close())
				}
			})
			require.NoError(t, first.Migrate(ctx))

			const workflowCount = 24
			for i := 0; i < workflowCount; i++ {
				require.NoError(t, first.CreateWorkflow(ctx, "workflow-"+string(rune('a'+i)), "shared", []byte(`{}`)))
			}

			type claimResult struct {
				claims []storage.Claim
				err    error
			}
			results := make(chan claimResult, 2)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i, st := range []storage.Storage{first, second} {
				wg.Add(1)
				go func(worker int, backend storage.Storage) {
					defer wg.Done()
					<-start
					claims, err := backend.ClaimWorkflows(ctx, storage.ClaimRequest{
						WorkerID: "worker-" + string(rune('1'+worker)),
						LeaseFor: time.Minute,
						Names:    []string{"shared"},
						Limit:    workflowCount,
					})
					results <- claimResult{claims: claims, err: err}
				}(i, st)
			}
			close(start)
			wg.Wait()
			close(results)

			claimed := map[string]struct{}{}
			for result := range results {
				require.NoError(t, result.err)
				for _, claim := range result.claims {
					_, duplicate := claimed[claim.ID]
					assert.False(t, duplicate, "workflow %s claimed more than once", claim.ID)
					claimed[claim.ID] = struct{}{}
				}
			}
			assert.Len(t, claimed, workflowCount)
		})
	}
}

func TestStorageBackendsFenceExpiredLeases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(root string) storage.Config
	}{
		{
			name: "filesystem",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendFile,
					File:    storage.FileConfig{Path: filepath.Join(root, "state")},
				}
			},
		},
		{
			name: "sqlite",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendSQLite,
					SQLite:  storage.SQLiteConfig{Path: filepath.Join(root, "endure.sqlite")},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			config := tt.config(t.TempDir())
			first, err := OpenStorage(ctx, config)
			require.NoError(t, err)
			second, err := OpenStorage(ctx, config)
			require.NoError(t, err)
			t.Cleanup(func() {
				for _, st := range []storage.Storage{first, second} {
					closer, ok := st.(io.Closer)
					if ok {
						require.NoError(t, closer.Close())
					}
				}
			})
			require.NoError(t, first.Migrate(ctx))
			require.NoError(t, first.CreateWorkflow(ctx, "workflow", "leased", []byte(`{}`)))

			claims, err := first.ClaimWorkflows(ctx, storage.ClaimRequest{
				WorkerID: "worker-1",
				LeaseFor: 20 * time.Millisecond,
				Names:    []string{"leased"},
				Limit:    1,
			})
			require.NoError(t, err)
			require.Len(t, claims, 1)
			original := claims[0].Lease

			time.Sleep(30 * time.Millisecond)
			claims, err = second.ClaimWorkflows(ctx, storage.ClaimRequest{
				WorkerID: "worker-2",
				LeaseFor: time.Minute,
				Names:    []string{"leased"},
				Limit:    1,
			})
			require.NoError(t, err)
			require.Len(t, claims, 1)
			assert.NotEqual(t, original.Token, claims[0].Token)
			assert.Equal(t, 2, claims[0].Attempt)
			assert.ErrorIs(t, first.WriteStep(ctx, original, storage.StepWrite{
				Name:   "stale",
				Status: storage.StepCompleted,
			}), ErrLeaseLost)
		})
	}
}

func TestLocalStorageBackendsRecoverRetriesSleepsAndSignals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(root string) storage.Config
	}{
		{
			name: "filesystem",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend:     storage.BackendFile,
					File:        storage.FileConfig{Path: filepath.Join(root, "state")},
					AutoMigrate: true,
				}
			},
		},
		{
			name: "sqlite",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend:     storage.BackendSQLite,
					SQLite:      storage.SQLiteConfig{Path: filepath.Join(root, "endure.sqlite")},
					AutoMigrate: true,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st, err := OpenStorage(t.Context(), tt.config(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() {
				closer, ok := st.(io.Closer)
				if ok {
					require.NoError(t, closer.Close())
				}
			})

			var attempts int
			var mu sync.Mutex
			e := NewWithStorage(st, testOptions())
			wf := Register(e, "durable-"+tt.name, func(ctx Context, input string) (string, error) {
				value, err := Step(ctx, "retry", func(context.Context) (string, error) {
					mu.Lock()
					defer mu.Unlock()
					attempts++
					if attempts == 1 {
						return "", errors.New("transient")
					}
					return input, nil
				}, Retry{Attempts: 3, Backoff: 10 * time.Millisecond})
				if err != nil {
					return "", err
				}
				if err := Sleep(ctx, "delay", 20*time.Millisecond); err != nil {
					return "", err
				}
				approval, err := Wait[string](ctx, "approval")
				if err != nil {
					return "", err
				}
				return value + approval, nil
			})
			startEngine(t, e)

			handle, err := wf.Start(t.Context(), "durable-workflow", "ready-")
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				workflow, err := st.GetWorkflow(t.Context(), "durable-workflow")
				return err == nil && workflow.Status == storage.WorkflowWaiting
			}, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, e.Signal(t.Context(), "durable-workflow", "approval", "approved"))
			output, err := handle.Wait(waitCtx(t))
			require.NoError(t, err)
			assert.Equal(t, "ready-approved", output)

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 2, attempts)
		})
	}
}

func TestLocalStorageBackendsRecoverAfterCrash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config func(root string) storage.Config
	}{
		{
			name: "filesystem",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendFile,
					File:    storage.FileConfig{Path: filepath.Join(root, "state")},
				}
			},
		},
		{
			name: "sqlite",
			config: func(root string) storage.Config {
				return storage.Config{
					Backend: storage.BackendSQLite,
					SQLite:  storage.SQLiteConfig{Path: filepath.Join(root, "endure.sqlite")},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st, err := OpenStorage(t.Context(), tt.config(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() {
				closer, ok := st.(io.Closer)
				if ok {
					require.NoError(t, closer.Close())
				}
			})
			require.NoError(t, st.Migrate(t.Context()))

			var once sync.Once
			executions := 0
			var mu sync.Mutex
			e := NewWithStorage(st, testOptions())
			e.fault = func(_, point string) bool {
				crash := false
				if point == "after-run:work" {
					once.Do(func() { crash = true })
				}
				return crash
			}
			wf := Register(e, "crash-"+tt.name, func(ctx Context, _ string) (string, error) {
				return Step(ctx, "work", func(context.Context) (string, error) {
					mu.Lock()
					executions++
					mu.Unlock()
					return "done", nil
				})
			})
			startEngine(t, e)

			handle, err := wf.Start(t.Context(), "crash-workflow", "")
			require.NoError(t, err)
			output, err := handle.Wait(waitCtx(t))
			require.NoError(t, err)
			assert.Equal(t, "done", output)

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 2, executions)
		})
	}
}
