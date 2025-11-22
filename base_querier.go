package sqlc

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
)

// sqlxQuerier is an internal interface that both *sqlx.DB and *sqlx.Tx satisfy.
// This allows us to write code that works with either a direct database connection
// or a transaction without duplication.
type sqlxQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
}

// baseQuerier implements the common Querier interface methods using an underlying sqlxQuerier.
// This eliminates code duplication between client and tx implementations.
type baseQuerier struct {
	logger   log.Logger
	executor exec.Executor
	db       sqlxQuerier
}

// newBaseQuerier creates a new baseQuerier with the given dependencies.
func newBaseQuerier(logger log.Logger, executor exec.Executor, db sqlxQuerier) *baseQuerier {
	return &baseQuerier{
		logger:   logger,
		executor: executor,
		db:       db,
	}
}

// Get executes a query that is expected to return at most one row and scans it into dest.
// If the query returns no rows, it returns sql.ErrNoRows.
// The query is logged and executed through the configured executor (which may include retry logic).
func (b *baseQuerier) Get(ctx context.Context, dest any, query string, args ...any) error {
	b.logger.Debug(ctx, "> %s %q", query, args)

	_, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, b.db.GetContext(ctx, dest, query, args...)
	})

	return err
}

// Exec executes a query without returning any rows (e.g., INSERT, UPDATE, DELETE).
// It returns a Result containing the number of rows affected and the last insert ID (if applicable).
// The query is logged and executed through the configured executor (which may include retry logic).
func (b *baseQuerier) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	b.logger.Debug(ctx, "> %s %q", query, args)

	res, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return b.db.ExecContext(ctx, query, args...)
	})
	if err != nil {
		return nil, err
	}

	return res.(Result), err
}

// NamedExec executes a named query without returning rows using named parameters from a struct or map.
// Named parameters in the query are specified with :name syntax (e.g., :id, :name).
// The arg parameter should be a struct with field tags matching the named parameters, or a map[string]any.
// It returns a Result containing the number of rows affected and the last insert ID (if applicable).
// The query is logged and executed through the configured executor (which may include retry logic).
//
// Example with struct:
//
//	type User struct {
//	    ID   int    `db:"id"`
//	    Name string `db:"name"`
//	}
//	user := User{ID: 1, Name: "John"}
//	result, err := client.NamedExec(ctx, "INSERT INTO users (id, name) VALUES (:id, :name)", user)
//
// Example with map:
//
//	params := map[string]any{"id": 1, "name": "John"}
//	result, err := client.NamedExec(ctx, "INSERT INTO users (id, name) VALUES (:id, :name)", params)
func (b *baseQuerier) NamedExec(ctx context.Context, query string, arg any) (Result, error) {
	b.logger.Debug(ctx, "> %s %q", query, arg)

	res, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return b.db.NamedExecContext(ctx, query, arg)
	})
	if err != nil {
		return nil, err
	}

	return res.(Result), err
}

// Prepare creates a prepared statement for later queries or executions.
// The query is executed through the configured executor (which may include retry logic).
func (b *baseQuerier) Prepare(ctx context.Context, query string) (*Stmt, error) {
	res, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return b.db.PreparexContext(ctx, query)
	})
	if err != nil {
		return nil, err
	}

	return res.(*Stmt), nil
}

// Query executes a query that returns rows, returning a Rows object for iteration.
// The caller is responsible for calling Close on the returned Rows.
// The query is logged and executed through the configured executor (which may include retry logic).
func (b *baseQuerier) Query(ctx context.Context, query string, args ...any) (*Rows, error) {
	b.logger.Debug(ctx, "> %s %q", query, args)

	res, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return b.db.QueryxContext(ctx, query, args...)
	})
	if err != nil {
		return nil, err
	}

	return res.(*Rows), err
}

// QueryRow executes a query that is expected to return at most one row.
// The query is logged and executed through the configured executor (which may include retry logic).
func (b *baseQuerier) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	b.logger.Debug(ctx, "> %s %q", query, args)

	res, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return b.db.QueryRowContext(ctx, query, args...), nil
	})
	if err != nil {
		return nil
	}

	return res.(*sql.Row)
}

// Select executes a query and scans all returned rows into dest (typically a slice).
// The dest parameter should be a pointer to a slice of structs.
// The query is logged and executed through the configured executor (which may include retry logic).
func (b *baseQuerier) Select(ctx context.Context, dest any, query string, args ...any) error {
	b.logger.Debug(ctx, "> %s %q", query, args)

	_, err := b.executor.Execute(ctx, func(ctx context.Context) (any, error) {
		return nil, b.db.SelectContext(ctx, dest, query, args...)
	})

	return err
}
