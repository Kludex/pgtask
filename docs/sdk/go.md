# Go SDK

## Enqueue a task

Define the task once, then enqueue it with a typed payload:

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Kludex/pgtask/sdks/go"
)

type renderRequest struct {
	ReportID string `json:"report_id"`
}

type renderResult struct {
	Rendered string `json:"rendered"`
}

func main() {
	ctx := context.Background()
	render, err := pgtask.DefineTask[renderRequest, renderResult](
		"reports.render",
		pgtask.DefinitionOptions{QueueName: "reports"},
	)
	if err != nil {
		panic(err)
	}
	client, err := pgtask.Connect(ctx, os.Getenv("PGTASK_DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	idempotencyKey := "report-123:v1"
	task, err := render.Enqueue(
		ctx,
		client,
		renderRequest{ReportID: "report-123"},
		pgtask.EnqueueOptions{IdempotencyKey: &idempotencyKey},
	)
	if err != nil {
		panic(err)
	}
	waitContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := task.Result(waitContext)
	if err != nil {
		panic(err)
	}
	if result == nil || result.State != "succeeded" || result.Result == nil {
		panic("task did not succeed")
	}
	fmt.Println(result.Result.Rendered)
}
```

Install the producer client with `go get github.com/Kludex/pgtask/sdks/go`. Run migrations with the `pgtask` CLI before
enqueueing. The Rust and Python runtimes execute handlers. The Go package is a typed producer and result client.

`TaskHandle.Result()` uses a dedicated PostgreSQL session. It subscribes to a deterministic result shard before reading task state.
This ordering prevents a completion from being lost between subscription and inspection. A transaction-pooling proxy
cannot provide this session.

Use `ConnectWithConfig` to set a separate `ListenerURL`, `MaxQueryConnections`, and `MaxListenerConnections`. The
listener URL defaults to the query URL. Use a direct PostgreSQL endpoint or a PgBouncer session pool for listeners.

## Enqueue a batch

Use one database round trip when you already have several tasks:

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Kludex/pgtask/sdks/go"
)

type renderRequest struct {
	ReportID string `json:"report_id"`
}

type renderResult struct {
	Rendered string `json:"rendered"`
}

func main() {
	ctx := context.Background()
	render, err := pgtask.DefineTask[renderRequest, renderResult](
		"reports.render",
		pgtask.DefinitionOptions{QueueName: "reports"},
	)
	if err != nil {
		panic(err)
	}
	client, err := pgtask.Connect(ctx, os.Getenv("PGTASK_DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	tasks, err := render.EnqueueMany(ctx, client, []pgtask.EnqueueRequest[renderRequest]{
		{Payload: renderRequest{ReportID: "report-123"}},
		{Payload: renderRequest{ReportID: "report-456"}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(tasks[0].ID, tasks[1].ID)
}
```

`EnqueueMany()` preserves request order. PostgreSQL accepts the complete batch in one transaction, so another session
never sees a partial batch.

## Enqueue in a transaction

Pass an existing transaction and the task commits with your application writes:

```go
package producer

import (
	"context"

	"github.com/Kludex/pgtask/sdks/go"
	"github.com/jackc/pgx/v5/pgxpool"
)

type renderRequest struct {
	ReportID string `json:"report_id"`
}

type renderResult struct {
	Rendered string `json:"rendered"`
}

func saveAndEnqueue(ctx context.Context, pool *pgxpool.Pool) error {
	if err := pgtask.CheckStorageProtocol(ctx, pool); err != nil {
		return err
	}
	render, err := pgtask.DefineTask[renderRequest, renderResult](
		"reports.render",
		pgtask.DefinitionOptions{QueueName: "reports"},
	)
	if err != nil {
		return err
	}
	transaction, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	if _, err := transaction.Exec(ctx, "INSERT INTO reports (id) VALUES ($1)", "report-123"); err != nil {
		return err
	}
	idempotencyKey := "report-123:v1"
	_, err = render.EnqueueOn(
		ctx,
		transaction,
		renderRequest{ReportID: "report-123"},
		pgtask.EnqueueOptions{IdempotencyKey: &idempotencyKey},
	)
	if err != nil {
		return err
	}
	return transaction.Commit(ctx)
}
```

`EnqueueOn()` accepts a pgx pool, connection, or transaction. It does not open another connection or commit for you.
Call `CheckStorageProtocol()` once when you build a low-level transactional producer. `Connect()` and normal client
operations perform this check for you.

Use `EnqueueManyOn()` to enqueue a batch through the same pool, connection, or transaction.

## OpenTelemetry

The client injects the active OpenTelemetry context into task headers with the global text-map propagator. Configure an
OpenTelemetry SDK or OTLP exporter in your application before enqueueing. The package does not install an exporter or
choose a telemetry backend.

## Signals and cancellation

A producer can resolve a waiting task or cancel it outright:

```go
task := pgtask.Task[renderResult](client, taskID)
_, err := task.Signal(ctx, "approval", 0, map[string]bool{"approved": true})
if err != nil {
	return err
}
_, err = task.Cancel(ctx)
return err
```

Signals require a producer database role. Cancellation is an administrative operation and requires the pgtask
administrator role.
