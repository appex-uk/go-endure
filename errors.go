package endure

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrLeaseLost means another worker took over the workflow; the local execution is abandoned.
	ErrLeaseLost = errors.New("endure: lease lost")
	// ErrNotFound means the workflow does not exist.
	ErrNotFound = errors.New("endure: workflow not found")
	// ErrNameMismatch means a workflow ID already exists under a different workflow name.
	ErrNameMismatch = errors.New("endure: workflow id already used by a different workflow")
	// ErrDuplicateStep means a step, sleep or wait name was used twice in one workflow.
	ErrDuplicateStep = errors.New("endure: duplicate step name")

	// errCrash simulates abrupt process death in tests: the execution is dropped without any writes.
	errCrash = errors.New("endure: simulated crash")
)

// StepError is returned by Step when a step has permanently failed. Err is nil when the
// failure was loaded from the database rather than produced in this process.
type StepError struct {
	Step    string
	Message string
	Err     error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("endure: step %q failed: %s", e.Step, e.Message)
}
func (e *StepError) Unwrap() error { return e.Err }

// WorkflowError is returned by Handle.Wait when the workflow ended in the failed state.
type WorkflowError struct {
	ID      string
	Message string
}

func (e *WorkflowError) Error() string {
	return fmt.Sprintf("endure: workflow %q failed: %s", e.ID, e.Message)
}

// Permanent marks err as non-retryable regardless of the step's Retry policy.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// suspendError unwinds the workflow function so the worker can release it until later.
// Workflow code must return it (or an error wrapping it) unchanged.
type suspendError struct {
	until     time.Time
	waitingOn string
}

func (e *suspendError) Error() string { return "endure: workflow suspended" }
