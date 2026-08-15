package pgtask_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Kludex/pgtask/sdks/go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type reportPayload struct {
	ReportID string `json:"report_id"`
}

type reportResult struct {
	Rendered string `json:"rendered"`
}

type testPropagator struct{}

func (testPropagator) Inject(_ context.Context, carrier propagation.TextMapCarrier) {
	carrier.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
}

func (testPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (testPropagator) Fields() []string {
	return []string{"traceparent"}
}

type badJSON struct{}

func (badJSON) MarshalJSON() ([]byte, error) {
	return nil, errors.New("cannot encode")
}

type errorExecutor struct{}

func (errorExecutor) QueryRow(context.Context, string, ...any) pgx.Row {
	return errorRow{}
}

type errorRow struct{}

func (errorRow) Scan(...any) error {
	return errors.New("database unavailable")
}

func TestTaskDefinitions(t *testing.T) {
	t.Parallel()
	invalid := []struct {
		name    string
		task    string
		options pgtask.DefinitionOptions
	}{
		{name: "empty task"},
		{name: "invalid task", task: "bad task"},
		{name: "long task", task: string(make([]byte, 256))},
		{name: "invalid queue", task: "reports.render", options: pgtask.DefinitionOptions{QueueName: "bad queue"}},
		{name: "long queue", task: "reports.render", options: pgtask.DefinitionOptions{QueueName: string(make([]byte, 129))}},
		{name: "invalid version", task: "reports.render", options: pgtask.DefinitionOptions{HandlerVersion: -1}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := pgtask.DefineTask[reportPayload, reportResult](test.task, test.options)
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	definition, err := pgtask.DefineTask[reportPayload, reportResult]("reports.render", pgtask.DefinitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = definition.EnqueueOn(
		context.Background(),
		errorExecutor{},
		reportPayload{},
		pgtask.EnqueueOptions{},
	)
	if err == nil {
		t.Fatal("expected database error")
	}
	_, err = definition.EnqueueOn(
		context.Background(),
		errorExecutor{},
		reportPayload{},
		pgtask.EnqueueOptions{MaxAttempts: -1},
	)
	if err == nil {
		t.Fatal("expected max attempts error")
	}
	badPayloadDefinition, err := pgtask.DefineTask[badJSON, reportResult]("reports.bad-payload", pgtask.DefinitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = badPayloadDefinition.EnqueueOn(
		context.Background(),
		errorExecutor{},
		badJSON{},
		pgtask.EnqueueOptions{},
	)
	if err == nil {
		t.Fatal("expected payload encoding error")
	}
	_, err = definition.EnqueueOn(
		context.Background(),
		errorExecutor{},
		reportPayload{},
		pgtask.EnqueueOptions{Headers: map[string]any{"invalid": badJSON{}}},
	)
	if err == nil {
		t.Fatal("expected header encoding error")
	}
}

func TestClient(t *testing.T) {
	databaseURL := os.Getenv("PGTASK_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("PGTASK_DATABASE_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	client := pgtask.NewClient(pool)
	client.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(testPropagator{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previousPropagator) })

	definition, err := pgtask.DefineTask[reportPayload, reportResult](
		"go.render",
		pgtask.DefinitionOptions{QueueName: fmt.Sprintf("go-%d", time.Now().UnixNano()), HandlerVersion: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	idempotencyKey := fmt.Sprintf("go-%d", time.Now().UnixNano())
	handle, err := definition.Enqueue(
		ctx,
		client,
		reportPayload{ReportID: "report-123"},
		pgtask.EnqueueOptions{
			IdempotencyKey: &idempotencyKey,
			Priority:       4,
			Headers:        map[string]any{"source": "go"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if pgtask.Task[reportResult](client, handle.ID).ID != handle.ID {
		t.Fatal("task handle did not preserve the ID")
	}
	result, err := handle.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.State != "pending" || result.Result != nil || result.CompletedAt != nil {
		t.Fatalf("unexpected pending result: %#v", result)
	}
	var headers map[string]any
	if err := pool.QueryRow(ctx, "SELECT headers FROM pgtask.task_view WHERE id = $1::uuid", handle.ID).Scan(&headers); err != nil {
		t.Fatal(err)
	}
	if headers["source"] != "go" || headers["traceparent"] == nil {
		t.Fatalf("trace headers were not merged: %#v", headers)
	}

	firstSignal, err := handle.Signal(ctx, "approval", 0, map[string]bool{"approved": true})
	if err != nil {
		t.Fatal(err)
	}
	secondSignal, err := handle.Signal(ctx, "approval", 0, map[string]bool{"approved": false})
	if err != nil {
		t.Fatal(err)
	}
	if string(firstSignal) != `{"approved": true}` || string(secondSignal) != `{"approved": true}` {
		t.Fatalf("unexpected signals: %s %s", firstSignal, secondSignal)
	}

	waitContext, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelWait()
	if _, err := handle.Result(waitContext); err == nil {
		t.Fatal("expected result timeout")
	}
	waitContext, cancelWait = context.WithTimeout(ctx, 2*time.Second)
	defer cancelWait()
	resultChannel := make(chan *pgtask.TaskResult[reportResult], 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, err := handle.Result(waitContext)
		resultChannel <- result
		errorChannel <- err
	}()
	for range 5 {
		if _, err := pool.Exec(ctx, "SELECT pg_notify('pgtask_result', '00000000-0000-0000-0000-000000000000')"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := pool.Exec(
		ctx,
		"UPDATE pgtask.tasks SET state = 'succeeded', result = $2::jsonb, completed_at = statement_timestamp() WHERE id = $1::uuid",
		handle.ID,
		`{"rendered":"yes"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-errorChannel; err != nil {
		t.Fatal(err)
	}
	result = <-resultChannel
	if result == nil || result.Result == nil || result.Result.Rendered != "yes" || result.CompletedAt == nil {
		t.Fatalf("unexpected completed result: %#v", result)
	}
	result, err = handle.Result(ctx)
	if err != nil || result == nil || result.State != "succeeded" {
		t.Fatalf("terminal result was not immediate: %#v %v", result, err)
	}
	if cancelled, err := handle.Cancel(ctx); err != nil || cancelled {
		t.Fatalf("completed task cancellation = %v, %v", cancelled, err)
	}

	transaction, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := definition.EnqueueOn(ctx, transaction, reportPayload{ReportID: "rollback"}, pgtask.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	rolledBackResult, err := pgtask.Task[reportResult](client, rolledBack.TaskID).Inspect(ctx)
	if err != nil || rolledBackResult != nil {
		t.Fatalf("rolled-back task exists: %#v %v", rolledBackResult, err)
	}

	first, err := definition.EnqueueOn(
		ctx,
		pool,
		reportPayload{ReportID: "first"},
		pgtask.EnqueueOptions{IdempotencyKey: &idempotencyKey},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Created || first.TaskID != handle.ID {
		t.Fatalf("idempotency result = %#v", first)
	}

	cancellable, err := definition.Enqueue(ctx, client, reportPayload{ReportID: "cancel"}, pgtask.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan *pgtask.TaskResult[reportResult], 1)
	waitingErrors := make(chan error, 1)
	go func() {
		result, err := cancellable.Result(ctx)
		waiting <- result
		waitingErrors <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancelled, err := cancellable.Cancel(ctx)
	if err != nil || !cancelled {
		t.Fatalf("cancel task = %v, %v", cancelled, err)
	}
	if err := <-waitingErrors; err != nil {
		t.Fatal(err)
	}
	if result := <-waiting; result == nil || result.State != "cancelled" || len(result.Error) == 0 {
		t.Fatalf("unexpected cancelled result: %#v", result)
	}
}

func TestClientErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := pgtask.Connect(ctx, "://"); err == nil {
		t.Fatal("expected configuration error")
	}
	connectContext, cancelConnect := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelConnect()
	if _, err := pgtask.Connect(connectContext, "postgres://127.0.0.1:1/pgtask"); err == nil {
		t.Fatal("expected connection error")
	}
	databaseURL := os.Getenv("PGTASK_DATABASE_URL")
	client, err := pgtask.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	client.Close()
	closed := pgtask.Task[reportResult](client, "00000000-0000-0000-0000-000000000000")
	definition, err := pgtask.DefineTask[reportPayload, reportResult]("go.closed", pgtask.DefinitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definition.Enqueue(ctx, client, reportPayload{}, pgtask.EnqueueOptions{}); err == nil {
		t.Fatal("expected enqueue error")
	}
	if _, err := closed.Inspect(ctx); err == nil {
		t.Fatal("expected inspect error")
	}
	if _, err := closed.Result(ctx); err == nil {
		t.Fatal("expected listener error")
	}
	if _, err := closed.Signal(ctx, "signal", 0, nil); err == nil {
		t.Fatal("expected signal error")
	}
	if _, err := closed.Signal(ctx, "signal", 0, badJSON{}); err == nil {
		t.Fatal("expected signal encoding error")
	}
	if _, err := closed.Cancel(ctx); err == nil {
		t.Fatal("expected cancel error")
	}

	listenContext, cancelListen := context.WithCancel(context.Background())
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.PrepareConn = func(context.Context, *pgx.Conn) (bool, error) {
		cancelListen()
		return true, nil
	}
	listenPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(listenPool.Close)
	listenClient := pgtask.NewClient(listenPool)
	if _, err := pgtask.Task[reportResult](listenClient, "00000000-0000-0000-0000-000000000000").Result(listenContext); err == nil {
		t.Fatal("expected LISTEN error")
	}
}

func TestResultDecodeError(t *testing.T) {
	databaseURL := os.Getenv("PGTASK_DATABASE_URL")
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	client := pgtask.NewClient(pool)
	definition, err := pgtask.DefineTask[reportPayload, int]("go.invalid-result", pgtask.DefinitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := definition.Enqueue(ctx, client, reportPayload{}, pgtask.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(
		ctx,
		"UPDATE pgtask.tasks SET state = 'succeeded', result = '{}'::jsonb, completed_at = statement_timestamp() WHERE id = $1::uuid",
		handle.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Inspect(ctx); err == nil {
		t.Fatal("expected result decoding error")
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("PGTASK_DATABASE_URL") == "" {
		fmt.Fprintln(os.Stderr, "PGTASK_DATABASE_URL is required")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

var _ json.Marshaler = badJSON{}
