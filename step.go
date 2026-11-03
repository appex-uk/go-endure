package endure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Retry configures a step's retry policy. Attempts is the total number of tries (values below 1 mean 1).
// Between tries the workflow is released and resumed after Backoff doubled per attempt, capped at MaxDelay.
type Retry struct {
	Attempts int
	Backoff  time.Duration
	MaxDelay time.Duration
}

// StepOption customises Step, StepVoid and Defer.
type StepOption interface{ applyStep(*stepConfig) }

type stepConfig struct{ retry Retry }

func (r Retry) applyStep(c *stepConfig) { c.retry = r }

func newStepConfig(opts []StepOption) stepConfig {
	var c stepConfig
	for _, o := range opts {
		o.applyStep(&c)
	}
	return c
}

func (c stepConfig) delay(attempt int) time.Duration {
	d := c.retry.Backoff
	for i := 1; i < attempt && d > 0; i++ {
		if c.retry.MaxDelay > 0 && d >= c.retry.MaxDelay {
			break
		}
		d *= 2
	}
	if c.retry.MaxDelay > 0 && d > c.retry.MaxDelay {
		d = c.retry.MaxDelay
	}
	return d
}

var reservedPrefixes = []string{"sleep:", "signal:", "compensate:", "defer:"}

func checkName(name string) error {
	if name == "" {
		return errors.New("endure: empty step name")
	}
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p) {
			return fmt.Errorf("endure: step name %q uses reserved prefix %q", name, p)
		}
	}
	return nil
}

// Step runs fn at most once to completion per workflow: its result is checkpointed and returned
// from storage on later executions. If fn fails and the Retry policy allows, the workflow is
// suspended and the step re-run later. Return the error unchanged from the workflow function
// so suspension can unwind it.
func Step[T any](ctx Context, name string, fn func(context.Context) (T, error), opts ...StepOption) (T, error) {
	if err := checkName(name); err != nil {
		var zero T
		return zero, err
	}
	return runStep(ctx.exec(), name, newStepConfig(opts), fn)
}

// StepVoid is Step for functions with no result.
func StepVoid(ctx Context, name string, fn func(context.Context) error, opts ...StepOption) error {
	_, err := Step(ctx, name, func(c context.Context) (struct{}, error) { return struct{}{}, fn(c) }, opts...)
	return err
}

func runStep[T any](x *execution, name string, cfg stepConfig, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := x.useName(name); err != nil {
		return zero, err
	}
	if err := x.Err(); err != nil {
		return zero, err
	}
	st := x.e.st

	row, err := st.getStep(x, x.lease.id, name)
	if err != nil {
		return zero, err
	}
	if row != nil {
		switch row.status {
		case stepCompleted:
			return decode[T](name, row.output)
		case stepFailed:
			return zero, &StepError{Step: name, Message: row.errMsg}
		case stepRetrying:
			if row.notBefore != nil && time.Now().Before(*row.notBefore) {
				return zero, &suspendError{until: *row.notBefore}
			}
		}
	}
	attempt := 1
	if row != nil {
		attempt = row.attempts + 1
	}

	if err := x.e.crashAt(x.lease.id, "before:"+name); err != nil {
		return zero, err
	}
	if err := st.writeStep(x, x.lease, name, stepRunning, attempt, nil, "", nil); err != nil {
		return zero, err
	}

	sctx := context.WithValue(x, stepInfoKey{}, StepInfo{WorkflowID: x.lease.id, Step: name, Attempt: attempt})
	v, ferr := callStep(sctx, fn)

	if err := x.e.crashAt(x.lease.id, "after-run:"+name); err != nil {
		return zero, err
	}
	if ferr == nil {
		out, merr := json.Marshal(v)
		if merr == nil {
			if err := st.writeStep(x, x.lease, name, stepCompleted, attempt, out, "", nil); err != nil {
				return zero, err
			}
			if err := x.e.crashAt(x.lease.id, "after-commit:"+name); err != nil {
				return zero, err
			}
			return v, nil
		}
		ferr = Permanent(fmt.Errorf("encode result: %w", merr))
	}

	// A cancelled context means shutdown or lease loss, not a step failure.
	if err := x.Err(); err != nil {
		return zero, err
	}

	var perm *permanentError
	if attempt >= cfg.retry.Attempts || errors.As(ferr, &perm) {
		if err := st.writeStep(x, x.lease, name, stepFailed, attempt, nil, ferr.Error(), nil); err != nil {
			return zero, err
		}
		return zero, &StepError{Step: name, Message: ferr.Error(), Err: ferr}
	}
	until := time.Now().Add(cfg.delay(attempt))
	if err := st.writeStep(x, x.lease, name, stepRetrying, attempt, nil, ferr.Error(), &until); err != nil {
		return zero, err
	}
	return zero, &suspendError{until: until}
}

func callStep[T any](ctx context.Context, fn func(context.Context) (T, error)) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in step: %v", r)
		}
	}()
	return fn(ctx)
}

func decode[T any](name string, b []byte) (T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return v, fmt.Errorf("endure: decode stored result of %q: %w", name, err)
	}
	return v, nil
}

// Sleep suspends the workflow for d without holding a worker. The wake time is persisted on first call.
func Sleep(ctx Context, name string, d time.Duration) error {
	if err := checkName(name); err != nil {
		return err
	}
	x := ctx.exec()
	step := "sleep:" + name
	if err := x.useName(step); err != nil {
		return err
	}
	if err := x.Err(); err != nil {
		return err
	}
	st := x.e.st

	row, err := st.getStep(x, x.lease.id, step)
	if err != nil {
		return err
	}
	var wake time.Time
	switch {
	case row == nil:
		wake = time.Now().Add(d)
		if err := st.writeStep(x, x.lease, step, stepRunning, 0, nil, "", &wake); err != nil {
			return err
		}
	case row.status == stepCompleted:
		return nil
	default:
		wake = *row.notBefore
	}

	if time.Now().Before(wake) {
		return &suspendError{until: wake}
	}
	return st.writeStep(x, x.lease, step, stepCompleted, 0, nil, "", &wake)
}

// Wait suspends the workflow until Engine.Signal delivers a signal with the given name and returns its payload.
func Wait[T any](ctx Context, name string) (T, error) {
	var zero T
	if err := checkName(name); err != nil {
		return zero, err
	}
	x := ctx.exec()
	step := "signal:" + name
	if err := x.useName(step); err != nil {
		return zero, err
	}
	if err := x.Err(); err != nil {
		return zero, err
	}
	st := x.e.st

	row, err := st.getStep(x, x.lease.id, step)
	if err != nil {
		return zero, err
	}
	if row != nil && row.status == stepCompleted {
		return decode[T](name, row.output)
	}
	payload, ok, err := st.getSignal(x, x.lease.id, name)
	if err != nil {
		return zero, err
	}
	if !ok {
		return zero, &suspendError{waitingOn: name}
	}
	if err := st.writeStep(x, x.lease, step, stepCompleted, 0, payload, "", nil); err != nil {
		return zero, err
	}
	return decode[T](name, payload)
}

// Defer registers a compensation. If the workflow function later returns an error, registered
// compensations run in reverse order as durable steps named "compensate:"+name. Compensation
// failures are collected and do not stop later compensations. Call Defer after the step it undoes.
func Defer(ctx Context, name string, fn func(context.Context) error, opts ...StepOption) error {
	if err := checkName(name); err != nil {
		return err
	}
	x := ctx.exec()
	if err := x.useName("defer:" + name); err != nil {
		return err
	}
	x.mu.Lock()
	x.compensations = append(x.compensations, compensation{name: name, fn: fn, opts: opts})
	x.mu.Unlock()
	return nil
}
