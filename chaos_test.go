package endure

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var chaosSteps = []string{"A", "B", "C", "D"}

// chaosWorkflow runs A -> B -> C -> D. Each step records its execution and flags a violation
// if the step was already checkpointed as completed when its function starts.
func chaosWorkflow(pool *pgxpool.Pool, c *counters, violations *counters) func(Context, string) (string, error) {
	return func(ctx Context, _ string) (string, error) {
		for _, s := range chaosSteps {
			_, err := Step(ctx, s, func(context.Context) (string, error) {
				var n int
				_ = pool.QueryRow(context.Background(),
					`SELECT count(*) FROM endure_step WHERE workflow_id = $1 AND name = $2 AND status = 'completed'`,
					ctx.WorkflowID(), s).Scan(&n)
				if n > 0 {
					violations.inc(ctx.WorkflowID() + "/" + s)
				}
				c.inc(ctx.WorkflowID() + "/" + s)
				return s, nil
			})
			if err != nil {
				return "", err
			}
		}
		return "done", nil
	}
}

// A single simulated crash at every point of every step must still complete the workflow, and
// only a crash between side effect and checkpoint may re-run that step.
func TestChaosSingleCrashAtEveryPoint(t *testing.T) {
	pool := testPool(t)

	for _, step := range chaosSteps {
		for _, point := range []string{"before", "after-run", "after-commit"} {
			t.Run(point+"-"+step, func(t *testing.T) {
				c, violations := newCounters(), newCounters()
				e := New(pool, testOptions())
				wf := Register(e, uniqueName("chaos1"), chaosWorkflow(pool, c, violations))

				var once sync.Once
				target := point + ":" + step
				e.fault = func(_, p string) (crash bool) {
					if p == target {
						once.Do(func() { crash = true })
					}
					return crash
				}
				startEngine(t, e)

				id := uuidString()
				h, err := wf.Start(waitCtx(t), id, "")
				require.NoError(t, err)
				out, err := h.Wait(waitCtx(t))
				require.NoError(t, err)
				require.Equal(t, "done", out)

				for _, s := range chaosSteps {
					want := 1
					if s == step && point == "after-run" {
						want = 2
					}
					require.Equal(t, want, c.get(id+"/"+s), "executions of %s", s)
				}
				require.Zero(t, violations.get(id+"/"+step))
			})
		}
	}
}

// Two workers, random crashes, and one worker repeatedly restarted: every workflow must finish
// and no checkpointed step may ever be executed again.
func TestChaosTwoWorkersRandomCrashes(t *testing.T) {
	pool := testPool(t)
	c, violations := newCounters(), newCounters()
	name := uniqueName("chaos2")

	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(1))
	faults := 0
	fault := func(_, p string) bool {
		rngMu.Lock()
		defer rngMu.Unlock()
		if faults < 60 && rng.Float64() < 0.1 {
			faults++
			return true
		}
		return false
	}

	newEngine := func() *Engine {
		e := New(pool, testOptions())
		e.fault = fault
		Register(e, name, chaosWorkflow(pool, c, violations))
		return e
	}
	steady := newEngine()
	startEngine(t, steady)

	stopMurder := make(chan struct{})
	var murderDone sync.WaitGroup
	murderDone.Add(1)
	go func() {
		defer murderDone.Done()
		for {
			r := startEngine(t, newEngine())
			select {
			case <-stopMurder:
				r.stop()
				return
			case <-time.After(150 * time.Millisecond):
				r.stop()
			}
		}
	}()

	const n = 30
	ids := make([]string, n)
	for i := range ids {
		ids[i] = uuidString()
		_, err := pool.Exec(context.Background(),
			`INSERT INTO endure_workflow (id, name, input, status) VALUES ($1, $2, '""', 'queued')`, ids[i], name)
		require.NoError(t, err)
	}

	ctx := waitCtx(t)
	for _, id := range ids {
		h := &Handle[string]{e: steady, id: id}
		out, err := h.Wait(ctx)
		require.NoError(t, err, id)
		require.Equal(t, "done", out)
	}
	close(stopMurder)
	murderDone.Wait()

	for _, id := range ids {
		for _, s := range chaosSteps {
			require.GreaterOrEqual(t, c.get(fmt.Sprintf("%s/%s", id, s)), 1)
			require.Zero(t, violations.get(id+"/"+s), "checkpointed step %s/%s executed again", id, s)
		}
	}
}
