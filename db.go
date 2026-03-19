package sqlc

import (
	"context"
	"database/sql"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
)

// DB is a sqlc-owned database handle that preserves the package's query,
// scan, and named parameter behavior without exposing third-party types publicly.
type DB struct {
	handle dbHandle
}

func newDB(handle dbHandle) *DB {
	return &DB{handle: handle}
}

// ProvideDB provides a database handle from context.
func ProvideDB(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*DB, error) {
	var (
		err      error
		settings *Settings
	)

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return ProvideDBFromSettings(ctx, logger, name, settings)
}

// NewDB creates a new database handle.
func NewDB(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*DB, error) {
	var (
		err      error
		settings *Settings
	)

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return NewDBFromSettings(ctx, logger, name, settings)
}

// ProvideDBFromSettings provides a database handle from settings.
func ProvideDBFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (*DB, error) {
	connection, err := provideDBFromSettings(ctx, logger, name, settings)
	if err != nil {
		return nil, err
	}

	return newDB(connection), nil
}

// NewDBFromSettings creates a new database handle from settings.
func NewDBFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (*DB, error) {
	connection, err := newDBFromSettings(ctx, logger, name, settings)
	if err != nil {
		return nil, err
	}

	return newDB(connection), nil
}

// NewDBWithSettings creates a new database handle from settings.
func NewDBWithSettings(logger log.Logger, settings *Settings) (*DB, error) {
	connection, err := newDBWithInterfaces(logger, settings)
	if err != nil {
		return nil, err
	}

	return newDB(connection), nil
}

// WrapDB wraps an existing database/sql handle for use with sqlc.
//
// The driverName must match the registered SQL driver so named parameter
// binding and placeholder handling behave correctly.
func WrapDB(db *sql.DB, driverName string) *DB {
	return newDB(newStdlibDBAdapter(db, driverName))
}

// Close closes the underlying database handle.
func (d *DB) Close() error {
	return d.handle.Close()
}

// ExecContext executes a query without returning rows.
func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (Result, error) {
	return d.handle.ExecContext(ctx, query, args...)
}

// GetContext executes a query that is expected to return at most one row.
func (d *DB) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return d.handle.GetContext(ctx, dest, query, args...)
}

// NamedExecContext executes a named query without returning rows.
func (d *DB) NamedExecContext(ctx context.Context, query string, arg any) (Result, error) {
	return d.handle.NamedExecContext(ctx, query, arg)
}

// PrepareContext prepares a statement for later use.
func (d *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	return d.handle.PrepareContext(ctx, query)
}

// QueryContext executes a query that returns rows.
func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	return d.handle.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.handle.QueryRowContext(ctx, query, args...)
}

// SelectContext executes a query and scans all returned rows into dest.
func (d *DB) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return d.handle.SelectContext(ctx, dest, query, args...)
}

// SQLDB returns the underlying database/sql handle.
func (d *DB) SQLDB() *sql.DB {
	return d.handle.SQLDB()
}
