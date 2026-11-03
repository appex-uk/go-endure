package endure

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Context is passed to workflow functions. It is cancelled when the lease is lost or the worker shuts down.
type Context interface {
	context.Context
	WorkflowID() string
	// Attempt is the number of times this workflow has been claimed by a worker, including this one.
	Attempt() int
	exec() *execution
}

type compensation struct {
	name string
	fn   func(context.Context) error
	opts []StepOption
}

type execution struct {
	context.Context
	e       *Engine
	lease   lease
	attempt int

	mu            sync.Mutex
	names         map[string]struct{}
	compensations []compensation
}

func (x *execution) WorkflowID() string { return x.lease.id }
func (x *execution) Attempt() int       { return x.attempt }
func (x *execution) exec() *execution   { return x }

// useName reserves a step name for this execution.
func (x *execution) useName(name string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if _, dup := x.names[name]; dup {
		return fmt.Errorf("%w: %q", ErrDuplicateStep, name)
	}
	x.names[name] = struct{}{}
	return nil
}

// interrupted reports whether err means the execution must stop without recording a failure.
func (x *execution) interrupted(err error) bool {
	var susp *suspendError
	return errors.As(err, &susp) || errors.Is(err, errCrash) || x.Err() != nil
}

func (x *execution) compensate() error {
	x.mu.Lock()
	comps := append([]compensation(nil), x.compensations...)
	x.mu.Unlock()

	var errs []error
	for i := len(comps) - 1; i >= 0; i-- {
		c := comps[i]
		_, err := runStep(x, "compensate:"+c.name, newStepConfig(c.opts), func(ctx context.Context) (struct{}, error) {
			return struct{}{}, c.fn(ctx)
		})
		if err != nil {
			if x.interrupted(err) {
				return err
			}
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type stepInfoKey struct{}

// StepInfo describes the step currently executing; retrieve it with StepInfoFrom inside a step function.
type StepInfo struct {
	WorkflowID string
	Step       string
	Attempt    int
}

// IdempotencyKey is stable across retries and re-executions of the step; pass it to external APIs.
func (i StepInfo) IdempotencyKey() string { return i.WorkflowID + "/" + i.Step }

// StepInfoFrom returns the StepInfo for a context passed to a step function.
func StepInfoFrom(ctx context.Context) (StepInfo, bool) {
	i, ok := ctx.Value(stepInfoKey{}).(StepInfo)
	return i, ok
}
