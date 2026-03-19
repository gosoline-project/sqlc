package sqlc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
)

// Remaining direct sqlx touchpoints are intentionally isolated in this
// compatibility layer while downstream users migrate to sqlc-owned APIs.

// Row represents a single row returned from a query.
// Deprecated: use the *sql.Row values returned by QueryRow or QueryRowContext.
type Row = sql.Row

// Tx represents a transaction-scoped query client.
type Tx interface {
	context.Context
	Querier
	Q() *QueryBuilder
	Commit() error
	Rollback() error
	SQLTx() *sql.Tx
	// Deprecated: use SQLTx instead.
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
// Deprecated: use ProvideDB.
func ProvideConnection(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*sqlx.DB, error) {
	db, err := ProvideDB(ctx, config, logger, name)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(db.handle)
}

// NewConnection creates a new sqlx-backed connection.
// Deprecated: use NewDB.
func NewConnection(ctx context.Context, config cfg.Config, logger log.Logger, name string) (*sqlx.DB, error) {
	db, err := NewDB(ctx, config, logger, name)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(db.handle)
}

// ProvideConnectionFromSettings provides a sqlx-backed connection from settings.
// Deprecated: use ProvideDBFromSettings.
func ProvideConnectionFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (*sqlx.DB, error) {
	db, err := ProvideDBFromSettings(ctx, logger, name, settings)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(db.handle)
}

// NewConnectionFromSettings creates a new sqlx-backed connection from settings.
// Deprecated: use NewDBFromSettings.
func NewConnectionFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (*sqlx.DB, error) {
	db, err := NewDBFromSettings(ctx, logger, name, settings)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(db.handle)
}

// NewConnectionWithInterfaces creates a new sqlx-backed connection from settings.
// Deprecated: use NewDBWithSettings.
func NewConnectionWithInterfaces(logger log.Logger, settings *Settings) (*sqlx.DB, error) {
	db, err := NewDBWithSettings(logger, settings)
	if err != nil {
		return nil, err
	}

	return unwrapSQLXDB(db.handle)
}

// NewClientWithInterfaces creates a new SQL client with provided interfaces.
// This is useful for testing or when you want to provide custom implementations.
// Deprecated: use NewClientWithDB together with WrapDB.
func NewClientWithInterfaces(logger log.Logger, connection *sqlx.DB, executor exec.Executor, qbConfig *QueryBuilderConfig) *client {
	return NewClientWithDB(logger, newDB(newStdlibDBAdapter(connection.DB, connection.DriverName())), executor, qbConfig)
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
