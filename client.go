package sqlc

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

	// Querier defines the interface for executing SQL queries.
	// It provides methods for executing queries, fetching single rows, and scanning results.
	Querier interface {
		// Get executes a query that is expected to return at most one row and scans it into dest.
		Get(ctx context.Context, dest any, query string, args ...any) error
		// Exec executes a query without returning any rows (e.g., INSERT, UPDATE, DELETE).
		Exec(ctx context.Context, query string, args ...any) (Result, error)
		// NamedExec executes a named query without returning rows using named parameters from a struct or map.
		// Named parameters in the query are specified with :name syntax and are bound to struct fields or map keys.
		NamedExec(ctx context.Context, query string, arg any) (Result, error)
		// Prepare creates a prepared statement for later queries or executions.
		Prepare(ctx context.Context, query string) (*Stmt, error)
		// Query executes a query that returns rows, returning a Rows object for iteration.
		Query(ctx context.Context, query string, args ...any) (*Rows, error)
		// QueryRow executes a query that is expected to return at most one row.
		QueryRow(ctx context.Context, query string, args ...any) *sql.Row
		// Select executes a query and scans all returned rows into dest (typically a slice).
		Select(ctx context.Context, dest any, query string, args ...any) error
	}

	// Client defines the interface for executing SQL queries against a database.
	// It provides methods for executing queries, fetching single rows, and scanning results.
	Client interface {
		Querier

		// BeginTx starts a new transaction with the given options.
		BeginTx(ctx context.Context, ops ...*sql.TxOptions) (Tx, error)
		// Close closes the database connection.
		Close() error
		// Qb returns a new QueryBuilder instance for constructing SQL queries.
		Q() *QueryBuilder
	}

	// Result represents the result of an Exec operation (rows affected, last insert ID).
	Result = sql.Result
	// Row represents a single row returned from a query.
	Row = sqlx.Row
	// Rows represents the result of a Query operation for row-by-row iteration.
	Rows = sqlx.Rows
	// Stmt represents a prepared statement.
	Stmt = sqlx.Stmt
)

var _ Client = (*client)(nil)

type client struct {
	*baseQuerier
	db       *sqlx.DB
	qbConfig *QueryBuilderConfig
}

// ProvideClient provides a client from context.
// Applies the options on creation of a new client if none is registered in the context yet for the name.
// When requesting a client with the same name but different options the options will not be applied but
// the already registered client will be returned.
func ProvideClient(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*client, error) {
	var (
		err      error
		settings *Settings
	)

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
	var (
		err      error
		settings *Settings
	)

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return NewClientWithSettings(ctx, config, logger, name, settings)
}

// NewClientWithSettings creates a new SQL client with the provided settings.
// It establishes a database connection and optionally configures retry behavior.
func NewClientWithSettings(ctx context.Context, config cfg.Config, logger log.Logger, name string, settings *Settings) (*client, error) {
	var err error
	var connection *sqlx.DB
	var driver Driver

	executor := exec.NewDefaultExecutor()

	if connection, err = ProvideConnectionFromSettings(ctx, logger, name, settings); err != nil {
		return nil, fmt.Errorf("can not connect to sql database: %w", err)
	}

	if driver, err = GetDriver(logger, settings.Driver); err != nil {
		return nil, fmt.Errorf("can not get driver for sql client %s: %w", name, err)
	}

	qbConfig := &QueryBuilderConfig{
		StructTag:       dbStructTag,
		Placeholder:     driver.GetPlaceholder(),
		IdentifierQuote: driver.GetQuote(),
	}

	if !settings.Retry.Enabled {
		return NewClientWithInterfaces(logger, connection, executor, qbConfig), nil
	}

	if executor, err = NewExecutor(config, logger, name, ExecutorBackoffType(name)); err != nil {
		return nil, fmt.Errorf("can not create executor for sql client %s: %w", name, err)
	}

	return NewClientWithInterfaces(logger, connection, executor, qbConfig), nil
}

// NewClientWithInterfaces creates a new SQL client with provided interfaces.
// This is useful for testing or when you want to provide custom implementations.
func NewClientWithInterfaces(logger log.Logger, connection *sqlx.DB, executor exec.Executor, qbConfig *QueryBuilderConfig) *client {
	return &client{
		baseQuerier: newBaseQuerier(logger, executor, connection),
		db:          connection,
		qbConfig:    qbConfig,
	}
}

// Qb returns a new QueryBuilder instance for this client.
// QueryBuilder provides a fluent interface for constructing SQL queries.
func (c *client) Q() *QueryBuilder {
	return NewQueryBuilder(c, c.qbConfig)
}

func (c *client) BeginTx(ctx context.Context, ops ...*sql.TxOptions) (Tx, error) {
	c.logger.Debug(ctx, "start tx")

	if len(ops) == 0 {
		ops = append(ops, &sql.TxOptions{})
	}

	res, err := c.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return c.db.BeginTxx(ctx, ops[0])
	})
	if err != nil {
		return nil, err
	}

	return newTx(ctx, c.logger, c.executor, res.(*sqlx.Tx)), err
}

// Close closes the database connection and releases any associated resources.
func (c *client) Close() error {
	return c.db.Close()
}
