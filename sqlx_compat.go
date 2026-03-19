package sqlc

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
)

// Remaining direct sqlx touchpoints after this refactor are intentionally kept
// in this compatibility layer while the public API migration is handled in
// sqlc-b7a:
// - NewClientWithInterfaces accepts *sqlx.DB for tests and downstream callers.
// - ProvideConnection/NewConnection helpers still return *sqlx.DB.
// - Tx.SqlTx() exposes the underlying *sqlx.Tx escape hatch.
// - sqlx_adapter.go contains the temporary sqlx-backed runtime adapter.

// Row represents a single row returned from a query.
type Row = sqlx.Row

// Tx represents a transaction-scoped query client.
type Tx interface {
	context.Context
	Querier
	Q() *QueryBuilder
	Commit() error
	Rollback() error
	SqlTx() *sqlx.Tx
	WithContext(ctx context.Context) Tx
}

type sqlxDBAccessor interface {
	SQLXDB() *sqlx.DB
}

type sqlxTxAccessor interface {
	SQLXTx() *sqlx.Tx
}

// ProvideConnection provides a sqlx-backed connection from context.
func ProvideConnection(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*sqlx.DB, error) {
	var (
		err      error
		settings *Settings
	)

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return ProvideConnectionFromSettings(ctx, logger, name, settings)
}

// NewConnection creates a new sqlx-backed connection.
func NewConnection(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*sqlx.DB, error) {
	var (
		err      error
		settings *Settings
	)

	if settings, err = ReadSettings(config, name); err != nil {
		return nil, err
	}

	return NewConnectionFromSettings(ctx, logger, name, settings)
}

// ProvideConnectionFromSettings provides a sqlx-backed connection from settings.
func ProvideConnectionFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (*sqlx.DB, error) {
	connection, err := provideDBFromSettings(ctx, logger, name, settings)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(connection)
}

// NewConnectionFromSettings creates a new sqlx-backed connection from settings.
func NewConnectionFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (*sqlx.DB, error) {
	connection, err := newDBFromSettings(ctx, logger, name, settings)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(connection)
}

// NewConnectionWithInterfaces creates a new sqlx-backed connection from settings.
func NewConnectionWithInterfaces(logger log.Logger, settings *Settings) (*sqlx.DB, error) {
	connection, err := newDBWithInterfaces(logger, settings)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(connection)
}

// NewClientWithInterfaces creates a new SQL client with provided interfaces.
// This is useful for testing or when you want to provide custom implementations.
func NewClientWithInterfaces(logger log.Logger, connection *sqlx.DB, executor exec.Executor, qbConfig *QueryBuilderConfig) *client {
	return newClientWithDB(logger, newSQLXDBAdapter(connection), executor, qbConfig)
}

func (t *tx) SqlTx() *sqlx.Tx {
	sqlxBacked, ok := t.tx.(sqlxTxAccessor)
	if !ok {
		return nil
	}

	return sqlxBacked.SQLXTx()
}

func unwrapSQLXDB(connection dbHandle) (*sqlx.DB, error) {
	sqlxBacked, ok := connection.(sqlxDBAccessor)
	if !ok {
		return nil, fmt.Errorf("connection %T is not backed by sqlx", connection)
	}

	return sqlxBacked.SQLXDB(), nil
}
