package endure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkflowRunsStepsOnceAndReturnsResult(t *testing.T) {
	pool := testPool(t)
	c := newCounters()
	e := New(pool, testOptions())
	wf := Register(e, uniqueName("basic"), func(ctx Context, in string) (string, error) {
		a, err := Step(ctx, "a", func(context.Context) (string, error) { c.inc("a"); return in + "-a", nil })
		if err != nil {
			return "", err
		}
		return Step(ctx, "b", func(context.Context) (string, error) { c.inc("b"); return a + "-b", nil })
	})
	startEngine(t, e)

	h, err := wf.Start(waitCtx(t), uuidString(), "x")
	require.NoError(t, err)
	out, err := h.Wait(waitCtx(t))
	require.NoError(t, err)
	require.Equal(t, "x-a-b", out)
	require.Equal(t, 1, c.get("a"))
	require.Equal(t, 1, c.get("b"))
}

func TestStartIsIdempotentAndRejectsNameReuse(t *testing.T) {
	pool := testPool(t)
	e := New(pool, testOptions())
	noop := func(Context, string) (string, error) { return "", nil }
	w1 := Register(e, uniqueName("idem1"), noop)
	w2 := Register(e, uniqueName("idem2"), noop)

	id := uuidString()
	_, err := w1.Start(waitCtx(t), id, "x")
	require.NoError(t, err)
	_, err = w1.Start(waitCtx(t), id, "x")
	require.NoError(t, err)
	_, err = w2.Start(waitCtx(t), id, "x")
	require.ErrorIs(t, err, ErrNameMismatch)
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	pool := testPool(t)
	c := newCounters()
	e := New(pool, testOptions())
	wf := Register(e, uniqueName("retry"), func(ctx Context, _ string) (int, error) {
		return Step(ctx, "flaky", func(context.Context) (int, error) {
			if n := c.inc("flaky"); n < 3 {
				return 0, errors.New("transient")
			}
			return 42, nil
		}, Retry{Attempts: 5, Backoff: 10 * time.Millisecond})
	})
	startEngine(t, e)

	h, err := wf.Start(waitCtx(t), uuidString(), "")
	require.NoError(t, err)
	out, err := h.Wait(waitCtx(t))
	require.NoError(t, err)
	require.Equal(t, 42, out)
	require.Equal(t, 3, c.get("flaky"))
}

func TestStepFailsAfterRetriesExhaustedAndPermanentSkipsRetry(t *testing.T) {
	pool := testPool(t)
	c := newCounters()
	e := New(pool, testOptions())
	exhausted := Register(e, uniqueName("exhaust"), func(ctx Context, _ string) (int, error) {
		return Step(ctx, "bad", func(context.Context) (int, error) {
			c.inc("bad")
			return 0, errors.New("boom")
		}, Retry{Attempts: 2, Backoff: time.Millisecond})
	})
	permanent := Register(e, uniqueName("perm"), func(ctx Context, _ string) (int, error) {
		return Step(ctx, "bad", func(context.Context) (int, error) {
			c.inc("perm")
			return 0, Permanent(errors.New("nope"))
		}, Retry{Attempts: 5, Backoff: time.Millisecond})
	})
	startEngine(t, e)

	h, err := exhausted.Start(waitCtx(t), uuidString(), "")
	require.NoError(t, err)
	_, err = h.Wait(waitCtx(t))
	var we *WorkflowError
	require.ErrorAs(t, err, &we)
	require.Contains(t, we.Message, "boom")
	require.Equal(t, 2, c.get("bad"))

	h2, err := permanent.Start(waitCtx(t), uuidString(), "")
	require.NoError(t, err)
	_, err = h2.Wait(waitCtx(t))
	require.ErrorAs(t, err, &we)
	require.Equal(t, 1, c.get("perm"))
}

func TestDeferRunsCompensationsInReverseOrder(t *testing.T) {
	pool := testPool(t)
	var mu sync.Mutex
	var order []string
	e := New(pool, testOptions())
	wf := Register(e, uniqueName("saga"), func(ctx Context, _ string) (string, error) {
		for _, n := range []string{"a", "b"} {
			if _, err := Step(ctx, n, func(context.Context) (string, error) { return n, nil }); err != nil {
				return "", err
			}
			if err := Defer(ctx, "undo-"+n, func(context.Context) error {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, n)
				return nil
			}); err != nil {
				return "", err
			}
		}
		return Step(ctx, "c", func(context.Context) (string, error) { return "", errors.New("c failed") })
	})
	startEngine(t, e)

	h, err := wf.Start(waitCtx(t), uuidString(), "")
	require.NoError(t, err)
	_, err = h.Wait(waitCtx(t))
	var we *WorkflowError
	require.ErrorAs(t, err, &we)
	require.Contains(t, we.Message, "c failed")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"b", "a"}, order)
}

func TestSleepSuspendsAndResumesWithoutRerunningEarlierSteps(t *testing.T) {
	pool := testPool(t)
	c := newCounters()
	e := New(pool, testOptions())
	wf := Register(e, uniqueName("sleep"), func(ctx Context, _ string) (string, error) {
		if _, err := Step(ctx, "before", func(context.Context) (string, error) { c.inc("before"); return "", nil }); err != nil {
			return "", err
		}
		if err := Sleep(ctx, "nap", 300*time.Millisecond); err != nil {
			return "", err
		}
		return Step(ctx, "after", func(context.Context) (string, error) { c.inc("after"); return "done", nil })
	})
	startEngine(t, e)

	start := time.Now()
	h, err := wf.Start(waitCtx(t), uuidString(), "")
	require.NoError(t, err)
	out, err := h.Wait(waitCtx(t))
	require.NoError(t, err)
	require.Equal(t, "done", out)
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	require.Equal(t, 1, c.get("before"))
	require.Equal(t, 1, c.get("after"))
}

func TestWaitReceivesSignalSentBeforeOrAfterSuspension(t *testing.T) {
	pool := testPool(t)
	e := New(pool, testOptions())
	wf := Register(e, uniqueName("signal"), func(ctx Context, _ string) (string, error) {
		return Wait[string](ctx, "approval")
	})

	// Signal before any worker runs.
	early := uuidString()
	_, err := wf.Start(waitCtx(t), early, "")
	require.NoError(t, err)
	require.NoError(t, e.Signal(waitCtx(t), early, "approval", "early"))

	startEngine(t, e)
	out, err := wf.Handle(early).Wait(waitCtx(t))
	require.NoError(t, err)
	require.Equal(t, "early", out)

	// Signal while the workflow is parked.
	late := uuidString()
	h, err := wf.Start(waitCtx(t), late, "")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var status string
		_ = pool.QueryRow(context.Background(), `SELECT status FROM endure_workflow WHERE id = $1`, late).Scan(&status)
		return status == "waiting"
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.Signal(waitCtx(t), late, "approval", "late"))
	out, err = h.Wait(waitCtx(t))
	require.NoError(t, err)
	require.Equal(t, "late", out)

	require.ErrorIs(t, e.Signal(waitCtx(t), uuidString(), "approval", "x"), ErrNotFound)
}

func TestDuplicateStepNameIsRejected(t *testing.T) {
	pool := testPool(t)
	e := New(pool, testOptions())
	wf := Register(e, uniqueName("dup"), func(ctx Context, _ string) (string, error) {
		fn := func(context.Context) (string, error) { return "", nil }
		if _, err := Step(ctx, "x", fn); err != nil {
			return "", err
		}
		return Step(ctx, "x", fn)
	})
	startEngine(t, e)

	h, err := wf.Start(waitCtx(t), uuidString(), "")
	require.NoError(t, err)
	_, err = h.Wait(waitCtx(t))
	var we *WorkflowError
	require.ErrorAs(t, err, &we)
	require.Contains(t, we.Message, ErrDuplicateStep.Error())
}
