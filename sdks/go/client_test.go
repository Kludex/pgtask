package pgtask_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Kludex/pgtask/sdks/go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

type batchExecutor struct {
	rows pgx.Rows
	err  error
}

func (executor batchExecutor) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return executor.rows, executor.err
}

type batchRow struct {
	index   int
	taskID  string
	created bool
}

type batchRows struct {
	rows    []batchRow
	index   int
	scanErr error
	err     error
}

func (rows *batchRows) Close() {}

func (rows *batchRows) Err() error {
	return rows.err
}

func (rows *batchRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (rows *batchRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (rows *batchRows) Next() bool {
	if rows.index == len(rows.rows) {
		return false
	}
	rows.index++
	return true
}

func (rows *batchRows) Scan(destinations ...any) error {
	if rows.scanErr != nil {
		return rows.scanErr
	}
	row := rows.rows[rows.index-1]
	*destinations[0].(*int) = row.index
	*destinations[1].(*string) = row.taskID
	*destinations[2].(*bool) = row.created
	return nil
}

func (rows *batchRows) Values() ([]any, error) {
	return nil, nil
}

func (rows *batchRows) RawValues() [][]byte {
	return nil
}

func (rows *batchRows) Conn() *pgx.Conn {
	return nil
}

type protocolExecutor struct {
	minimum int
	maximum int
}

func (executor protocolExecutor) QueryRow(context.Context, string, ...any) pgx.Row {
	return protocolRow(executor)
}

type protocolRow struct {
	minimum int
	maximum int
}

func (row protocolRow) Scan(destinations ...any) error {
	*destinations[0].(*int) = row.minimum
	*destinations[1].(*int) = row.maximum
	return nil
}

type cancelBeforeListen struct {
	cancel context.CancelFunc
}

func (tracer cancelBeforeListen) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if strings.HasPrefix(data.SQL, "LISTEN ") {
		tracer.cancel()
	}
	return ctx
}

func (cancelBeforeListen) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

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
	invalidTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	batchErrors := []struct {
		name     string
		executor pgtask.QueryExecutor
		options  pgtask.EnqueueOptions
	}{
		{name: "encode", executor: batchExecutor{}, options: pgtask.EnqueueOptions{RunAt: &invalidTime}},
		{name: "query", executor: batchExecutor{err: errors.New("database unavailable")}},
		{name: "scan", executor: batchExecutor{rows: &batchRows{rows: []batchRow{{}}, scanErr: errors.New("bad row")}}},
		{name: "order", executor: batchExecutor{rows: &batchRows{rows: []batchRow{{index: 1}}}}},
		{name: "rows", executor: batchExecutor{rows: &batchRows{err: errors.New("connection lost")}}},
		{name: "incomplete", executor: batchExecutor{rows: &batchRows{}}},
	}
	for _, test := range batchErrors {
		t.Run("batch "+test.name, func(t *testing.T) {
			_, err := definition.EnqueueManyOn(
				context.Background(),
				test.executor,
				[]pgtask.EnqueueRequest[reportPayload]{{Options: test.options}},
			)
			if err == nil {
				t.Fatal("expected batch enqueue error")
			}
		})
	}
}

func TestStorageProtocolCompatibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if err := pgtask.CheckStorageProtocol(ctx, protocolExecutor{minimum: 1, maximum: 2}); err != nil {
		t.Fatal(err)
	}
	if err := pgtask.CheckStorageProtocol(ctx, protocolExecutor{minimum: 2, maximum: 3}); err == nil {
		t.Fatal("expected incompatible storage protocol")
	}
	if err := pgtask.CheckStorageProtocol(ctx, errorExecutor{}); err == nil {
		t.Fatal("expected storage protocol query error")
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
	separateClient := pgtask.NewClientWithListener(pool, pool)
	separateClient.Close()
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
	firstBatchKey := fmt.Sprintf("go-batch-first-%d", time.Now().UnixNano())
	secondBatchKey := fmt.Sprintf("go-batch-second-%d", time.Now().UnixNano())
	batchRequests := []pgtask.EnqueueRequest[reportPayload]{
		{Payload: reportPayload{ReportID: "batch-one"}, Options: pgtask.EnqueueOptions{IdempotencyKey: &firstBatchKey}},
		{Payload: reportPayload{ReportID: "batch-two"}, Options: pgtask.EnqueueOptions{IdempotencyKey: &secondBatchKey}},
	}
	firstBatchHandle, err := definition.Enqueue(ctx, client, batchRequests[0].Payload, batchRequests[0].Options)
	if err != nil {
		t.Fatal(err)
	}
	secondBatchHandle, err := definition.Enqueue(ctx, client, batchRequests[1].Payload, batchRequests[1].Options)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := definition.EnqueueMany(
		ctx,
		client,
		batchRequests,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].ID != firstBatchHandle.ID || batch[1].ID != secondBatchHandle.ID {
		t.Fatalf("unexpected batch handles: %#v", batch)
	}
	if _, err := definition.EnqueueMany(
		ctx,
		client,
		[]pgtask.EnqueueRequest[reportPayload]{{Options: pgtask.EnqueueOptions{MaxAttempts: -1}}},
	); err == nil {
		t.Fatal("expected batch validation error")
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
	var unrelatedChannel string
	if err := pool.QueryRow(
		ctx,
		"SELECT pgtask.result_channel('00000000-0000-0000-0000-000000000000'::uuid)",
	).Scan(&unrelatedChannel); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := pool.Exec(ctx, "SELECT pg_notify($1, $2)", unrelatedChannel, "00000000-0000-0000-0000-000000000000"); err != nil {
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
	rolledBackBatch, err := definition.EnqueueManyOn(
		ctx,
		transaction,
		[]pgtask.EnqueueRequest[reportPayload]{
			{Payload: reportPayload{ReportID: "batch-rollback-one"}},
			{Payload: reportPayload{ReportID: "batch-rollback-two"}},
		},
	)
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
	for _, result := range rolledBackBatch {
		rolledBackBatchResult, inspectErr := pgtask.Task[reportResult](client, result.TaskID).Inspect(ctx)
		if inspectErr != nil || rolledBackBatchResult != nil {
			t.Fatalf("rolled-back batch task exists: %#v %v", rolledBackBatchResult, inspectErr)
		}
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
	if _, err := pgtask.ConnectWithConfig(ctx, pgtask.ConnectConfig{DatabaseURL: "://", MaxQueryConnections: -1}); err == nil {
		t.Fatal("expected connection limit error")
	}
	if _, err := pgtask.Connect(ctx, "://"); err == nil {
		t.Fatal("expected configuration error")
	}
	connectContext, cancelConnect := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelConnect()
	if _, err := pgtask.Connect(connectContext, "postgres://127.0.0.1:1/pgtask"); err == nil {
		t.Fatal("expected connection error")
	}
	databaseURL := os.Getenv("PGTASK_DATABASE_URL")
	ownerPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ownerPool.Close)
	restrictedRole := fmt.Sprintf("pgtask_go_no_protocol_%d", time.Now().UnixNano())
	restrictedIdentifier := pgx.Identifier{restrictedRole}.Sanitize()
	if _, err := ownerPool.Exec(ctx, "CREATE ROLE "+restrictedIdentifier); err != nil {
		t.Fatal(err)
	}
	if _, err := ownerPool.Exec(ctx, "GRANT "+restrictedIdentifier+" TO CURRENT_USER"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := ownerPool.Exec(ctx, "REVOKE "+restrictedIdentifier+" FROM CURRENT_USER"); err != nil {
			t.Error(err)
		}
		if _, err := ownerPool.Exec(ctx, "DROP ROLE "+restrictedIdentifier); err != nil {
			t.Error(err)
		}
	})
	restrictedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	restrictedQuery := restrictedURL.Query()
	restrictedQuery.Set("options", "-c role="+restrictedRole)
	restrictedURL.RawQuery = restrictedQuery.Encode()
	if _, err := pgtask.Connect(ctx, restrictedURL.String()); err == nil {
		t.Fatal("expected storage protocol connection error")
	}
	restrictedPool, err := pgxpool.New(ctx, restrictedURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restrictedPool.Close)
	restrictedClient := pgtask.NewClient(restrictedPool)
	restrictedHandle := pgtask.Task[reportResult](
		restrictedClient,
		"00000000-0000-0000-0000-000000000000",
	)
	if _, err := restrictedHandle.Inspect(ctx); err == nil {
		t.Fatal("expected inspect protocol error")
	}
	if _, err := restrictedHandle.Result(ctx); err == nil {
		t.Fatal("expected result protocol error")
	}
	if _, err := restrictedHandle.Signal(ctx, "signal", 0, nil); err == nil {
		t.Fatal("expected signal protocol error")
	}
	if _, err := restrictedHandle.Cancel(ctx); err == nil {
		t.Fatal("expected cancel protocol error")
	}
	if _, err := pgtask.ConnectWithConfig(ctx, pgtask.ConnectConfig{
		DatabaseURL: databaseURL,
		ListenerURL: "://",
	}); err == nil {
		t.Fatal("expected listener configuration error")
	}
	listenerContext, cancelListener := context.WithTimeout(ctx, 2*time.Second)
	defer cancelListener()
	if _, err := pgtask.ConnectWithConfig(listenerContext, pgtask.ConnectConfig{
		DatabaseURL: databaseURL,
		ListenerURL: "postgres://127.0.0.1:1/pgtask",
	}); err == nil {
		t.Fatal("expected listener connection error")
	}
	client, err := pgtask.ConnectWithConfig(ctx, pgtask.ConnectConfig{
		DatabaseURL:            databaseURL,
		ListenerURL:            databaseURL,
		MaxQueryConnections:    2,
		MaxListenerConnections: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgtask.Task[reportResult](client, "not-a-uuid").Result(ctx); err == nil {
		t.Fatal("expected result channel error")
	}
	client.Close()
	client.Close()
	closed := pgtask.Task[reportResult](client, "00000000-0000-0000-0000-000000000000")
	definition, err := pgtask.DefineTask[reportPayload, reportResult]("go.closed", pgtask.DefinitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definition.Enqueue(ctx, restrictedClient, reportPayload{}, pgtask.EnqueueOptions{}); err == nil {
		t.Fatal("expected enqueue protocol error")
	}
	if _, err := definition.EnqueueMany(ctx, restrictedClient, nil); err == nil {
		t.Fatal("expected batch enqueue protocol error")
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
	config.ConnConfig.Tracer = cancelBeforeListen{cancel: cancelListen}
	listenPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(listenPool.Close)
	listenClient := pgtask.NewClient(listenPool)
	listenHandle := pgtask.Task[reportResult](listenClient, "00000000-0000-0000-0000-000000000000")
	if result, err := listenHandle.Inspect(context.Background()); err != nil || result != nil {
		t.Fatalf("prime listener client protocol: %#v %v", result, err)
	}
	if _, err := listenHandle.Result(listenContext); err == nil {
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
