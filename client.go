package sqlg

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
)

type (
	clientCtxKey string

	// Client defines the interface for executing SQL queries against a database.
	// It provides methods for executing queries, fetching single rows, and scanning results.
	Client interface {
		// Qb returns a new QueryBuilder instance for constructing SQL queries.
		Qb() *QueryBuilder
		// Close closes the database connection.
		Close() error
		// Get executes a query that is expected to return at most one row and scans it into dest.
		Get(ctx context.Context, dest any, query string, args ...any) error
		// Exec executes a query without returning any rows (e.g., INSERT, UPDATE, DELETE).
		Exec(ctx context.Context, query string, args ...any) (Result, error)
		// Query executes a query that returns rows, returning a Rows object for iteration.
		Query(ctx context.Context, query string, args ...any) (*Rows, error)
		// Select executes a query and scans all returned rows into dest (typically a slice).
		Select(ctx context.Context, dest any, query string, args ...any) error
	}

	// Result represents the result of an Exec operation (rows affected, last insert ID).
	Result = sql.Result
	// Rows represents the result of a Query operation for row-by-row iteration.
	Rows = sqlx.Rows
)

var _ Client = (*client)(nil)

type client struct {
	logger   log.Logger
	db       *sqlx.DB
	executor exec.Executor
}

// ProvideClient provides a client from context.
// Applies the options on creation of a new client if none is registered in the context yet for the name.
// When requesting a client with the same name but different options the options will not be applied but
// the already registered client will be returned.
func ProvideClient(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*client, error) {
	var err error
	var settings *Settings

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return appctx.Provide(ctx, clientCtxKey(fmt.Sprint(settings)), func() (*client, error) {
		return NewClientWithSettings(ctx, config, logger, name, settings)
	})
}

// NewClient creates a new SQL client with the given name.
// It reads the configuration for the named connection and establishes a database connection.
func NewClient(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*client, error) {
	var err error
	var settings *Settings

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return NewClientWithSettings(ctx, config, logger, name, settings)
}

// NewClientWithSettings creates a new SQL client with the provided settings.
// It establishes a database connection and optionally configures retry behavior.
func NewClientWithSettings(ctx context.Context, config cfg.Config, logger log.Logger, name string, settings *Settings) (*client, error) {
	var (
		err        error
		connection *sqlx.DB
		executor   exec.Executor = exec.NewDefaultExecutor()
	)

	if connection, err = ProvideConnectionFromSettings(ctx, logger, name, settings); err != nil {
		return nil, fmt.Errorf("can not connect to sql database: %w", err)
	}

	if settings.Retry.Enabled {
		if executor, err = NewExecutor(config, logger, name, ExecutorBackoffType(name)); err != nil {
			return nil, fmt.Errorf("can not create executor for sql client %s: %w", name, err)
		}
	}

	return NewClientWithInterfaces(logger, connection, executor), nil
}

// NewClientWithInterfaces creates a new SQL client with provided interfaces.
// This is useful for testing or when you want to provide custom implementations.
func NewClientWithInterfaces(logger log.Logger, connection *sqlx.DB, executor exec.Executor) *client {
	return &client{
		logger:   logger,
		db:       connection,
		executor: executor,
	}
}

// Qb returns a new QueryBuilder instance for this client.
// QueryBuilder provides a fluent interface for constructing SQL queries.
func (c *client) Qb() *QueryBuilder {
	return &QueryBuilder{client: c}
}

// Close closes the database connection and releases any associated resources.
func (c *client) Close() error {
	return c.db.Close()
}

// Get executes a query that is expected to return at most one row and scans it into dest.
// If the query returns no rows, it returns sql.ErrNoRows.
// The query is logged and executed through the configured executor (which may include retry logic).
func (c *client) Get(ctx context.Context, dest any, query string, args ...any) error {
	c.logger.Debug(ctx, "> %s %q", query, args)

	_, err := c.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, c.db.GetContext(ctx, dest, query, args...)
	})

	return err
}

// Exec executes a query without returning any rows (e.g., INSERT, UPDATE, DELETE).
// It returns a Result containing the number of rows affected and the last insert ID (if applicable).
// The query is logged and executed through the configured executor (which may include retry logic).
func (c *client) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	c.logger.Debug(ctx, "> %s %q", query, args)

	res, err := c.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return c.db.ExecContext(ctx, query, args...)
	})
	if err != nil {
		return nil, err
	}

	return res.(Result), err
}

// Select executes a query and scans all returned rows into dest (typically a slice).
// The dest parameter should be a pointer to a slice of structs.
// The query is logged and executed through the configured executor (which may include retry logic).
func (c *client) Select(ctx context.Context, dest any, query string, args ...any) error {
	c.logger.Debug(ctx, "> %s %q", query, args)

	_, err := c.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, c.db.SelectContext(ctx, dest, query, args...)
	})

	return err
}

// Query executes a query that returns rows, returning a Rows object for iteration.
// The caller is responsible for calling Close on the returned Rows.
// The query is logged and executed through the configured executor (which may include retry logic).
func (c *client) Query(ctx context.Context, query string, args ...any) (*Rows, error) {
	c.logger.Debug(ctx, "> %s %q", query, args)

	res, err := c.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return c.db.QueryxContext(ctx, query, args...)
	})
	if err != nil {
		return nil, err
	}

	return res.(*Rows), err
}
