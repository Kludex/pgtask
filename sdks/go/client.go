package pgtask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Client connects producer APIs to pgtask's PostgreSQL protocol.
type Client struct {
	pool  *pgxpool.Pool
	owned bool
}

// Connect creates and verifies a client-owned connection pool.
func Connect(ctx context.Context, databaseURL string) (*Client, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return &Client{pool: pool, owned: true}, nil
}

// NewClient uses an existing pool without taking ownership of it.
func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// Close closes a pool created by Connect.
func (client *Client) Close() {
	if client.owned {
		client.pool.Close()
	}
}

// Task returns a typed handle for an existing task ID.
func Task[Result any](client *Client, id string) TaskHandle[Result] {
	return TaskHandle[Result]{ID: id, client: client}
}

// TaskResult is the durable terminal or non-terminal state of a task.
type TaskResult[Result any] struct {
	State       string
	Result      *Result
	Error       json.RawMessage
	CompletedAt *time.Time
}

// TaskHandle exposes operations for one typed task.
type TaskHandle[Result any] struct {
	ID     string
	client *Client
}

// Inspect reads the task's current durable state.
func (handle TaskHandle[Result]) Inspect(ctx context.Context) (*TaskResult[Result], error) {
	return readTaskResult[Result](ctx, handle.client.pool, handle.ID)
}

// Result waits for completion using PostgreSQL LISTEN notifications.
func (handle TaskHandle[Result]) Result(ctx context.Context) (*TaskResult[Result], error) {
	connection, err := handle.client.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire result listener: %w", err)
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, "LISTEN pgtask_result"); err != nil {
		return nil, fmt.Errorf("listen for task result: %w", err)
	}
	defer func() {
		_, _ = connection.Exec(context.Background(), "UNLISTEN pgtask_result")
	}()
	for {
		result, err := readTaskResult[Result](ctx, connection, handle.ID)
		if err != nil || result == nil || isTerminal(result.State) {
			return result, err
		}
		for {
			notification, err := connection.Conn().WaitForNotification(ctx)
			if err != nil {
				return nil, fmt.Errorf("wait for task result: %w", err)
			}
			if notification.Channel == "pgtask_result" && notification.Payload == handle.ID {
				break
			}
		}
	}
}

// Signal emits one durable signal value. Repeating an occurrence returns its first value.
func (handle TaskHandle[Result]) Signal(
	ctx context.Context,
	name string,
	occurrence int,
	value any,
) (json.RawMessage, error) {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode signal: %w", err)
	}
	var stored json.RawMessage
	err = handle.client.pool.QueryRow(
		ctx,
		"SELECT value FROM pgtask.emit_signal($1::uuid, $2, $3, $4::jsonb)",
		handle.ID,
		name,
		occurrence,
		valueJSON,
	).Scan(&stored)
	if err != nil {
		return nil, fmt.Errorf("emit signal: %w", err)
	}
	return stored, nil
}

// Cancel requests cancellation. The database role must be a pgtask administrator.
func (handle TaskHandle[Result]) Cancel(ctx context.Context) (bool, error) {
	var cancelled bool
	err := handle.client.pool.QueryRow(
		ctx,
		"SELECT EXISTS(SELECT 1 FROM pgtask.cancel_task($1::uuid))",
		handle.ID,
	).Scan(&cancelled)
	if err != nil {
		return false, fmt.Errorf("cancel task: %w", err)
	}
	return cancelled, nil
}

func readTaskResult[Result any](
	ctx context.Context,
	executor QueryRowExecutor,
	id string,
) (*TaskResult[Result], error) {
	var state string
	var resultJSON []byte
	var errorJSON []byte
	var completedAt *time.Time
	err := executor.QueryRow(
		ctx,
		"SELECT state, result, error, completed_at FROM pgtask.task_result($1::uuid)",
		id,
	).Scan(&state, &resultJSON, &errorJSON, &completedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("read task result: %w", err)
	}
	result := TaskResult[Result]{State: state, CompletedAt: completedAt}
	if len(resultJSON) != 0 && string(resultJSON) != "null" {
		var value Result
		if err := json.Unmarshal(resultJSON, &value); err != nil {
			return nil, fmt.Errorf("decode task result: %w", err)
		}
		result.Result = &value
	}
	if len(errorJSON) != 0 && string(errorJSON) != "null" {
		result.Error = errorJSON
	}
	return &result, nil
}

func injectTraceContext(ctx context.Context, headers map[string]any) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for key, value := range carrier {
		headers[key] = value
	}
}

func isTerminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}
