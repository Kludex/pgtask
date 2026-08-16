package pgtask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	// StorageProtocolMinVersion is the oldest storage protocol understood by this SDK.
	StorageProtocolMinVersion = 1
	// StorageProtocolMaxVersion is the newest storage protocol understood by this SDK.
	StorageProtocolMaxVersion = 1
)

// Client connects producer APIs to pgtask's PostgreSQL protocol.
type Client struct {
	pool         *pgxpool.Pool
	listenerPool *pgxpool.Pool
	owned        bool
	compatible   bool
	protocolMu   sync.Mutex
}

// ConnectConfig configures separate query and session-listener connection pools.
type ConnectConfig struct {
	DatabaseURL            string
	ListenerURL            string
	MaxQueryConnections    int32
	MaxListenerConnections int32
}

// Connect creates and verifies a client-owned connection pool.
func Connect(ctx context.Context, databaseURL string) (*Client, error) {
	return ConnectWithConfig(ctx, ConnectConfig{DatabaseURL: databaseURL})
}

// ConnectWithConfig creates query and listener pools with explicit connection budgets.
func ConnectWithConfig(ctx context.Context, config ConnectConfig) (*Client, error) {
	if config.ListenerURL == "" {
		config.ListenerURL = config.DatabaseURL
	}
	if config.MaxQueryConnections == 0 {
		config.MaxQueryConnections = 10
	}
	if config.MaxListenerConnections == 0 {
		config.MaxListenerConnections = 4
	}
	if config.MaxQueryConnections < 1 || config.MaxListenerConnections < 1 {
		return nil, errors.New("PostgreSQL connection limits must be positive")
	}
	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL query pool: %w", err)
	}
	poolConfig.MaxConns = config.MaxQueryConnections
	// A positive MaxConns is the constructor's only error invariant.
	pool, _ := pgxpool.NewWithConfig(ctx, poolConfig)
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect PostgreSQL query pool: %w", err)
	}
	listenerConfig, err := pgxpool.ParseConfig(config.ListenerURL)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("parse PostgreSQL listener pool: %w", err)
	}
	listenerConfig.MaxConns = config.MaxListenerConnections
	// A positive MaxConns is the constructor's only error invariant.
	listenerPool, _ := pgxpool.NewWithConfig(ctx, listenerConfig)
	if err := listenerPool.Ping(ctx); err != nil {
		listenerPool.Close()
		pool.Close()
		return nil, fmt.Errorf("connect PostgreSQL listener pool: %w", err)
	}
	client := &Client{pool: pool, listenerPool: listenerPool, owned: true}
	if err := client.ensureStorageProtocol(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

// NewClient uses an existing pool without taking ownership of it.
func NewClient(pool *pgxpool.Pool) *Client {
	return NewClientWithListener(pool, pool)
}

// NewClientWithListener uses separate existing query and listener pools without taking ownership.
func NewClientWithListener(pool *pgxpool.Pool, listenerPool *pgxpool.Pool) *Client {
	return &Client{pool: pool, listenerPool: listenerPool}
}

// Close closes a pool created by Connect.
func (client *Client) Close() {
	if client.owned {
		client.pool.Close()
		client.listenerPool.Close()
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
	if err := handle.client.ensureStorageProtocol(ctx); err != nil {
		return nil, err
	}
	return readTaskResult[Result](ctx, handle.client.pool, handle.ID)
}

// Result waits for completion using PostgreSQL LISTEN notifications.
func (handle TaskHandle[Result]) Result(ctx context.Context) (*TaskResult[Result], error) {
	if err := handle.client.ensureStorageProtocol(ctx); err != nil {
		return nil, err
	}
	connection, err := handle.client.listenerPool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire result listener: %w", err)
	}
	defer connection.Release()
	var channel string
	if err := connection.QueryRow(ctx, "SELECT pgtask.result_channel($1::uuid)", handle.ID).Scan(&channel); err != nil {
		return nil, fmt.Errorf("resolve task result channel: %w", err)
	}
	if _, err := connection.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return nil, fmt.Errorf("listen for task result: %w", err)
	}
	defer func() {
		_, _ = connection.Exec(context.Background(), "UNLISTEN *")
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
			if notification.Channel == channel && notification.Payload == handle.ID {
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
	if err := handle.client.ensureStorageProtocol(ctx); err != nil {
		return nil, err
	}
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
	if err := handle.client.ensureStorageProtocol(ctx); err != nil {
		return false, err
	}
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

func (client *Client) ensureStorageProtocol(ctx context.Context) error {
	client.protocolMu.Lock()
	defer client.protocolMu.Unlock()
	if client.compatible {
		return nil
	}
	if err := CheckStorageProtocol(ctx, client.pool); err != nil {
		return err
	}
	client.compatible = true
	return nil
}

// CheckStorageProtocol verifies that an executor can use this SDK's SQL protocol.
func CheckStorageProtocol(ctx context.Context, executor QueryRowExecutor) error {
	var minimum int
	var maximum int
	if err := executor.QueryRow(
		ctx,
		"SELECT minimum, maximum FROM pgtask.storage_protocol_range()",
	).Scan(&minimum, &maximum); err != nil {
		return fmt.Errorf("read storage protocol range: %w", err)
	}
	if minimum > StorageProtocolMaxVersion || maximum < StorageProtocolMinVersion {
		return fmt.Errorf(
			"database storage protocols %d..=%d are incompatible with client protocols %d..=%d",
			minimum,
			maximum,
			StorageProtocolMinVersion,
			StorageProtocolMaxVersion,
		)
	}
	return nil
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
