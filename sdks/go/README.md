# pgtask for Go

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Kludex/pgtask/sdks/go"
)

type request struct {
	ReportID string `json:"report_id"`
}

type result struct {
	Rendered string `json:"rendered"`
}

func main() {
	ctx := context.Background()
	render, err := pgtask.DefineTask[request, result]("reports.render", pgtask.DefinitionOptions{})
	if err != nil {
		panic(err)
	}
	client, err := pgtask.Connect(ctx, os.Getenv("PGTASK_DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	task, err := render.Enqueue(ctx, client, request{ReportID: "report-123"}, pgtask.EnqueueOptions{})
	if err != nil {
		panic(err)
	}
	taskResult, err := task.Result(ctx)
	if err != nil {
		panic(err)
	}
	if taskResult == nil || taskResult.Result == nil {
		panic("task did not finish")
	}
	fmt.Println(taskResult.Result.Rendered)
}
```

This is the typed Go producer and result client for pgtask. See the
[Go SDK documentation](https://github.com/Kludex/pgtask/blob/main/docs/go.md) for transactions, OpenTelemetry
propagation, signals, and cancellation.
