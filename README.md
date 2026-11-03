# go-endure

Durable execution for ordinary Go functions, backed by PostgreSQL.

Wrap the side effects in your function with `endure.Step`. Each result is checkpointed in Postgres. If the process dies, another replica picks the workflow up, runs your function again from the top, and completed steps return their stored results instead of executing a second time.

There is no orchestration server, no YAML, no DSL and no extra broker. Postgres is the control plane; your pods are just workers.

```go
func Provision(ctx endure.Context, req Request) (Cluster, error) {
    account, err := endure.Step(ctx, "create-account", func(ctx context.Context) (Account, error) {
        return createAccount(ctx, req)
    })
    if err != nil {
        return Cluster{}, err
    }

    return endure.Step(ctx, "create-cluster", func(ctx context.Context) (Cluster, error) {
        return createCluster(ctx, account)
    }, endure.Retry{Attempts: 5, Backoff: time.Second, MaxDelay: time.Minute})
}
```

## Contents

- [Features](#features)
- [Installation](#installation)
- [Getting started](#getting-started)
- [Core concepts](#core-concepts)
- [Guide](#guide)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Guarantees and limitations](#guarantees-and-limitations)
- [Operations](#operations)
- [Testing](#testing)
- [Non-goals](#non-goals)

## Features

| Feature | API |
|---------|-----|
| Checkpointed steps | `Step`, `StepVoid` |
| Durable retries with exponential backoff | `Retry`, `Permanent` |
| Durable timers that do not hold a worker | `Sleep` |
| Wait for an external event | `Wait`, `Engine.Signal` |
| Saga-style compensation | `Defer` |
| Idempotent start by workflow ID | `Workflow.Start` |
| Multi-replica failover via leases | `Engine.Run` |

## Installation

```bash
go get github.com/appex-uk/go-endure
```

Requirements:

- PostgreSQL 13 or newer (uses `gen_random_uuid()` and `FOR UPDATE SKIP LOCKED`).
- A `*pgxpool.Pool` from [pgx v5](https://github.com/jackc/pgx).
- The Go version in [go.mod](go.mod).

Nothing needs to be copied between repositories. The whole library is one Go package (`endure`) at the module root, and `schema.sql` is embedded in the binary, so `endure.Migrate` needs no extra files. Import it like any other dependency:

```go
import "github.com/appex-uk/go-endure"
```

The module path in [go.mod](go.mod) must match the repository URL. Push this repository to GitHub and tag a release (`git tag v0.1.0 && git push --tags`) so consumers can pin a version. For a private repository, consumers also need:

```bash
go env -w GOPRIVATE=github.com/appex-uk/*
# plus git credentials for github.com, e.g. gh auth setup-git
```

During local development of go-endure itself, point a consumer at your checkout with `go mod edit -replace github.com/appex-uk/go-endure=../go-endure`.

## Getting started

### 1. Start a Postgres

```bash
docker run -d --name endure-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=endure \
  postgres:17
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/endure
```

### 2. Write a workflow

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/appex-uk/go-endure"
)

type Order struct{ ID string }

func Fulfil(ctx endure.Context, o Order) (string, error) {
    reservation, err := endure.Step(ctx, "reserve-stock", func(ctx context.Context) (string, error) {
        info, _ := endure.StepInfoFrom(ctx)
        // Pass the key to the downstream API so a re-run does not double-reserve.
        return reserveStock(ctx, o.ID, info.IdempotencyKey())
    }, endure.Retry{Attempts: 5, Backoff: 500 * time.Millisecond})
    if err != nil {
        return "", err
    }

    if err := endure.Defer(ctx, "release-stock", func(ctx context.Context) error {
        return releaseStock(ctx, reservation)
    }); err != nil {
        return "", err
    }

    if _, err := endure.Wait[bool](ctx, "payment-confirmed"); err != nil {
        return "", err
    }

    return endure.Step(ctx, "ship", func(ctx context.Context) (string, error) {
        return ship(ctx, reservation)
    })
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    if err := endure.Migrate(ctx, pool); err != nil {
        log.Fatal(err)
    }

    engine := endure.New(pool, endure.Options{})
    fulfil := endure.Register(engine, "fulfil-order", Fulfil)

    // Run the worker loop in the background; it stops when ctx is cancelled.
    go func() { _ = engine.Run(ctx) }()

    // Starting is idempotent: the same ID never creates a second workflow.
    h, err := fulfil.Start(ctx, "order-123", Order{ID: "123"})
    if err != nil {
        log.Fatal(err)
    }

    // Elsewhere (an HTTP handler, a Kafka consumer, ...):
    //   engine.Signal(ctx, "order-123", "payment-confirmed", true)

    trackingID, err := h.Wait(ctx)
    fmt.Println(trackingID, err)
}
```

### 3. Run more than one replica

Run the same binary several times against the same database. Each replica registers the same workflows and calls `engine.Run`. Kill any of them mid-workflow; another replica takes over once the dead replica's lease expires.

Processes that only start workflows or send signals (for example an API server) can create an `Engine` and call `Register`, `Start` and `Signal` without ever calling `Run`.

## Core concepts

| Concept | Meaning |
|---------|---------|
| **Workflow** | A Go function `func(endure.Context, I) (O, error)` registered under a name. Identified at runtime by a caller-chosen **workflow ID**. |
| **Step** | A named unit of side-effecting work inside a workflow. Its result is stored and replayed from storage on later executions. |
| **Execution** | One worker running the workflow function from the top. A workflow can be executed many times (after crashes, sleeps, retries, signals). |
| **Lease** | A time-limited claim a worker holds on a workflow. Renewed by a heartbeat; if it expires, another worker may claim the workflow. |
| **Signal** | A named JSON payload sent from outside to wake a workflow blocked in `Wait`. |
| **Compensation** | An undo action registered with `Defer`, run in reverse order if the workflow fails. |

Workflow states: `queued` → `running` → (`queued` again after a sleep or retry, or `waiting` for a signal) → `completed` or `failed`.

## Guide

### Workflows

```go
engine := endure.New(pool, endure.Options{})
wf := endure.Register(engine, "provision", Provision) // before engine.Run
```

- Inputs and outputs are serialised with `encoding/json`.
- `Register` panics if the name is registered twice.
- Workflow IDs are your idempotency keys. `wf.Start(ctx, id, input)` on an existing ID returns a handle to the existing workflow; reusing an ID under a different workflow name returns `ErrNameMismatch`.
- `wf.Handle(id)` returns a handle for an existing workflow, for example from a different process.
- `Handle.Wait(ctx)` polls until the workflow finishes. A failed workflow returns `*WorkflowError`.

### Steps

```go
v, err := endure.Step(ctx, "name", func(ctx context.Context) (T, error) { ... })
err := endure.StepVoid(ctx, "name", func(ctx context.Context) error { ... })
```

- The step name identifies the checkpoint. It must be **unique within the workflow** and **stable across deployments** while workflows using it are in flight. Duplicate names return `ErrDuplicateStep`.
- Results are JSON-encoded. Use types that round-trip through JSON.
- Inside a step function, `endure.StepInfoFrom(ctx)` returns the workflow ID, step name and attempt. `StepInfo.IdempotencyKey()` is `workflowID/stepName`, stable across retries and re-executions. Send it to external APIs that support idempotency keys.
- A panic inside a step is converted into a step error.
- If the worker shuts down or loses its lease mid-step, the context passed to the step is cancelled. Honour it. A cancelled step is not recorded as a failure and runs again elsewhere.

### Retries

```go
endure.Step(ctx, "call-api", fn, endure.Retry{
    Attempts: 10,               // total tries, including the first
    Backoff:  time.Second,      // delay after the first failure, doubled each attempt
    MaxDelay: time.Minute,      // cap on the delay
})
```

- Without a `Retry` option a step runs once; a failure fails the workflow.
- Retries are durable. After a failed attempt the workflow is released and resumed after the backoff, so waiting for a retry does not hold a worker and survives restarts.
- Wrap an error with `endure.Permanent(err)` to fail the step immediately regardless of the policy.
- When attempts are exhausted `Step` returns a `*StepError` and the workflow function normally returns it, which fails the workflow.

### Sleep

```go
if err := endure.Sleep(ctx, "cooling-off", 24*time.Hour); err != nil {
    return err
}
```

The wake time is persisted the first time `Sleep` is reached. The workflow is released and consumes no worker capacity until then. Re-executions do not restart the timer.

### Signals

```go
// In the workflow
approval, err := endure.Wait[Approval](ctx, "manager-approval")

// From anywhere that has an Engine on the same database
err := engine.Signal(ctx, workflowID, "manager-approval", Approval{By: "alice"})
```

- A signal sent before the workflow reaches `Wait` is stored and delivered when it does.
- Delivery to a parked workflow wakes it immediately; wakeups cannot be lost.
- Only the first signal per `(workflow, name)` is kept. Later ones are ignored.
- Signalling an unknown workflow ID returns `ErrNotFound`.

Example HTTP handler:

```go
mux.HandleFunc("POST /workflows/{id}/signals/{name}", func(w http.ResponseWriter, r *http.Request) {
    var payload json.RawMessage
    if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    switch err := engine.Signal(r.Context(), r.PathValue("id"), r.PathValue("name"), payload); {
    case errors.Is(err, endure.ErrNotFound):
        http.NotFound(w, r)
    case err != nil:
        http.Error(w, "internal error", http.StatusInternalServerError)
    default:
        w.WriteHeader(http.StatusAccepted)
    }
})
```

### Compensation (sagas)

```go
cluster, err := endure.Step(ctx, "create-cluster", createCluster)
if err != nil {
    return err
}
if err := endure.Defer(ctx, "delete-cluster", func(ctx context.Context) error {
    return deleteCluster(ctx, cluster.ID)
}); err != nil {
    return err
}
```

- Call `Defer` after the step it undoes.
- If the workflow function returns an error, registered compensations run in reverse order. Each is a durable step named `compensate:<name>`, so a crash mid-compensation resumes where it left off and finished compensations are not repeated.
- `Defer` accepts the same options as `Step`, including `Retry`.
- A failing compensation is recorded and does not stop the others. All errors are joined into the workflow's failure message.
- Compensations do not run when the workflow succeeds, is suspended, or is cancelled by shutdown.

### Errors

| Error | When |
|-------|------|
| `*StepError` | A step failed permanently. `Err` is set when the failure happened in this process and is nil when loaded from storage. |
| `*WorkflowError` | Returned by `Handle.Wait` for a failed workflow. |
| `ErrNotFound` | Unknown workflow ID in `Signal` or `Handle.Wait`. |
| `ErrNameMismatch` | `Start` with an ID already used by another workflow name. |
| `ErrDuplicateStep` | A step, sleep, wait or defer name used twice in one workflow. |
| `ErrLeaseLost` | Another worker took the workflow; the local execution is abandoned (internal, logged). |

**Return errors from `Step`, `Sleep` and `Wait` unchanged**, or wrapped with `%w`. Retries, sleeps and signal waits suspend the workflow by returning a special internal error that unwinds your function. Swallowing or replacing it breaks suspension.

### Rules for workflow functions

The workflow function is re-run from the top on every execution, so:

1. **Make the same sequence of `Step`/`Sleep`/`Wait`/`Defer` calls each time**, given the same stored results. Branch on step results, not on unstable inputs.
2. **Put anything non-deterministic inside a step** — `time.Now()`, `rand`, UUID generation, reads of mutable external state. Otherwise a re-execution may take a different path than the first.
3. **Do not run side effects outside steps.** They will run again on every execution.
4. **Do not use reserved name prefixes**: `sleep:`, `signal:`, `compensate:`, `defer:`.
5. Loops are fine; derive stable names from the loop key: `fmt.Sprintf("notify-%s", user.ID)`.

## Configuration

`endure.Options` (zero values select the defaults):

| Field | Default | Description |
|-------|---------|-------------|
| `WorkerID` | random UUID | Identifies the worker in `endure_workflow.worker_id`. |
| `LeaseDuration` | `30s` | How long a claim lasts without renewal. The heartbeat renews every third of this. Also the worst-case failover delay after a hard crash. |
| `PollInterval` | `500ms` | How often idle workers look for work. Also the polling interval of `Handle.Wait`. |
| `MaxConcurrent` | `16` | Maximum workflows executed concurrently by this engine. |
| `Logger` | `slog.Default()` | Structured logger. |

Tuning notes:

- Shorter leases fail over faster but need a healthier database connection; the heartbeat must complete well inside the lease.
- Size the `pgxpool` for roughly `MaxConcurrent` plus a few connections for heartbeats and claims.

## How it works

```
                 +-----------------------------+
 Start / Signal  |         PostgreSQL          |
  ------------>  |  endure_workflow            |
                 |  endure_step                |
                 |  endure_signal              |
                 +--------------+--------------+
                                ^
              claim (SKIP LOCKED), heartbeat, checkpoint
                                |
      +-------------------------+-------------------------+
      |                         |                         |
  replica A                 replica B                 replica C
  engine.Run                engine.Run                engine.Run
```

**Claiming.** Workers run `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT n)` over workflows that are `queued` and due, or `running` with an expired lease. Replicas never receive the same workflow. Workers only claim workflow names they have registered, so rolling deployments with different workflow sets are safe.

**Leases and fencing.** A claim stamps a fresh lease token. A heartbeat extends the lease. Every write — checkpoint, completion, suspension — runs in a transaction that locks the workflow row and checks the token, so a worker that lost its lease (paused, partitioned, evicted) can no longer change anything, and its local execution is cancelled.

**Checkpointing.** Before a step runs, an attempt row is written. After it returns, the result is committed. On the next execution, `Step` finds the completed row and returns the stored value.

**Suspension.** Sleeps, retry backoffs and signal waits release the workflow: sleeps and retries go back to `queued` with a future `run_at`; signal waits go to `waiting`. A row lock orders `Signal` against suspension so a signal is never lost between check and park.

**Shutdown.** When the context passed to `Run` is cancelled, in-flight executions are cancelled and their workflows are released immediately for other workers, without waiting for lease expiry.

### Schema

`Migrate` applies [schema.sql](schema.sql), which is idempotent and safe to run concurrently from several replicas:

| Table | Purpose |
|-------|---------|
| `endure_workflow` | One row per workflow: name, input, status, output/error, `run_at`, lease fields. |
| `endure_step` | One row per step: status, attempts, JSON output, error, `not_before` (retry/sleep time). |
| `endure_signal` | Buffered signals keyed by `(workflow_id, name)`. |

## Guarantees and limitations

- **Steps are at-least-once.** If a process dies after a side effect but before its checkpoint commits, the step runs again. No library can make an external call atomic with a database commit. Use `StepInfo.IdempotencyKey()` with APIs that support idempotency keys; for those that don't, make the step reconcile (check, then act).
- **Checkpointed steps are never re-executed.** Once a step's result is committed, later executions return it from storage. This is what the chaos tests assert.
- **Workflows eventually finish** as long as at least one worker is running and the database is available.
- **Failover time after a hard crash is up to `LeaseDuration`.** Graceful shutdown releases work immediately.
- **Results are JSON.** Changing a step's result type incompatibly while workflows are in flight breaks decoding of stored results.
- **Step names are part of your persistent state.** Renaming or reordering steps while workflows are in flight changes what existing executions do.
- **A step that always crashes the process is retried forever.** Crashes do not count as failures. Monitor `endure_workflow.attempt`.
- **No built-in retention.** Completed workflows stay in the tables until you delete them (see below).
- **Timers use the worker's clock** when computing wake times and the database clock when claiming. Keep clocks reasonably in sync.
- **Signals keep the first payload only** per name.

## Operations

Useful queries:

```sql
-- Workflows by state
SELECT status, count(*) FROM endure_workflow GROUP BY status;

-- Workflows that keep getting re-claimed (crash loops, stuck steps)
SELECT id, name, attempt FROM endure_workflow
WHERE status <> 'completed' AND attempt > 10 ORDER BY attempt DESC;

-- Currently running with an expired lease (waiting for failover)
SELECT id, worker_id, lease_expires_at FROM endure_workflow
WHERE status = 'running' AND lease_expires_at < now();

-- Step history for one workflow
SELECT name, status, attempts, error, not_before FROM endure_step
WHERE workflow_id = 'order-123' ORDER BY started_at;
```

Cleaning up finished workflows (steps and signals cascade):

```sql
DELETE FROM endure_workflow
WHERE status IN ('completed', 'failed') AND updated_at < now() - interval '30 days';
```

Failed workflows are not retried automatically. To rerun one, delete it and `Start` it again with the same ID, or start a new ID.

## Testing

Tests need a real PostgreSQL. Set `DATABASE_URL`; without it the tests skip.

```bash
docker run -d --name endure-test -p 5433:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=endure_test postgres:17

export DATABASE_URL=postgres://postgres:postgres@localhost:5433/endure_test
go vet ./...
go test -race -count=1 ./...
```

The suite covers the happy path, idempotent start, retries, permanent failures, compensation order, sleeps, signals before and after parking, and duplicate step names.

### Chaos tests

[chaos_test.go](chaos_test.go) is the most important part of the suite.

- **Single crash at every point.** For a workflow `A -> B -> C -> D`, an in-process fault hook simulates abrupt process death before, after the side effect, and after the checkpoint of each step. The execution is dropped with no cleanup. The test asserts the workflow still completes and that only a crash between side effect and checkpoint re-runs a step.
- **Two workers under random crashes.** Thirty workflows run on two workers with random crashes while one worker is repeatedly stopped and restarted. The test asserts that every workflow finishes and that no already-checkpointed step is executed again.

When testing your own workflows, run them against a real Postgres with short `LeaseDuration` and `PollInterval` values, e.g. `endure.Options{LeaseDuration: 300 * time.Millisecond, PollInterval: 10 * time.Millisecond}`, and give each test unique workflow names or IDs.

## Non-goals

go-endure deliberately stays small. It does not provide, and does not plan to provide:

- Deterministic event-history replay (it uses checkpointed steps instead)
- A separate server, Kubernetes operator or CRDs
- A workflow definition language or visual designer
- A message broker, leader election or custom storage engine
- Multi-language SDKs
- Exactly-once guarantees for external side effects

Postgres already provides persistence, transactions, locking, indexing, replication and backup. go-endure uses them directly rather than abstracting over them.
