package sqlc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/log"
)

type sqlxDBAdapter struct {
	db *sqlx.DB
}

type sqlxTxAdapter struct {
	tx *sqlx.Tx
}

type sqlxStmtAdapter struct {
	stmt *sqlx.Stmt
}

type sqlxRowsAdapter struct {
	rows *sqlx.Rows
}

func newSQLXDBAdapter(db *sqlx.DB) *sqlxDBAdapter {
	return &sqlxDBAdapter{db: db}
}

func newSQLXDBAdapterWithSettings(logger log.Logger, settings *Settings) (*sqlxDBAdapter, error) {
	drv, err := GetDriver(logger, settings.Driver)
	if err != nil {
		return nil, fmt.Errorf("could not get dsn provider for driver %s", settings.Driver)
	}

	dsn := drv.GetDSN(settings)

	genDriver, err := getGenericDriver(settings.Driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("could not get driver from %s connection factory: %w", settings.Driver, err)
	}

	metricDriverID := newMetricDriver(genDriver)

	db, err := sqlx.Connect(metricDriverID, dsn)
	if err != nil {
		return nil, fmt.Errorf("can not connect: %w", err)
	}

	db.SetConnMaxIdleTime(settings.ConnectionMaxIdleTime)
	db.SetConnMaxLifetime(settings.ConnectionMaxLifetime)
	db.SetMaxIdleConns(settings.MaxIdleConnections)
	db.SetMaxOpenConns(settings.MaxOpenConnections)

	return newSQLXDBAdapter(db), nil
}

func (d *sqlxDBAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (dbTx, error) {
	tx, err := d.db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &sqlxTxAdapter{tx: tx}, nil
}

func (d *sqlxDBAdapter) Close() error {
	return d.db.Close()
}

func (d *sqlxDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

func (d *sqlxDBAdapter) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return d.db.GetContext(ctx, dest, query, args...)
}

func (d *sqlxDBAdapter) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	return d.db.NamedExecContext(ctx, query, arg)
}

func (d *sqlxDBAdapter) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	stmt, err := d.db.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return newStmt(&sqlxStmtAdapter{stmt: stmt}), nil
}

func (d *sqlxDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	rows, err := d.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return newRows(&sqlxRowsAdapter{rows: rows}), nil
}

func (d *sqlxDBAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, query, args...)
}

func (d *sqlxDBAdapter) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return d.db.SelectContext(ctx, dest, query, args...)
}

func (d *sqlxDBAdapter) SQLDB() *sql.DB {
	return d.db.DB
}

func (d *sqlxDBAdapter) SQLXDB() *sqlx.DB {
	return d.db
}

func (t *sqlxTxAdapter) Commit() error {
	return t.tx.Commit()
}

func (t *sqlxTxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *sqlxTxAdapter) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return t.tx.GetContext(ctx, dest, query, args...)
}

func (t *sqlxTxAdapter) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	return t.tx.NamedExecContext(ctx, query, arg)
}

func (t *sqlxTxAdapter) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	stmt, err := t.tx.PreparexContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return newStmt(&sqlxStmtAdapter{stmt: stmt}), nil
}

func (t *sqlxTxAdapter) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	rows, err := t.tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return newRows(&sqlxRowsAdapter{rows: rows}), nil
}

func (t *sqlxTxAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

func (t *sqlxTxAdapter) Rollback() error {
	return t.tx.Rollback()
}

func (t *sqlxTxAdapter) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return t.tx.SelectContext(ctx, dest, query, args...)
}

func (t *sqlxTxAdapter) SQLXTx() *sqlx.Tx {
	return t.tx
}

func (s *sqlxStmtAdapter) Close() error {
	return s.stmt.Close()
}

func (s *sqlxStmtAdapter) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return s.stmt.ExecContext(ctx, args...)
}

func (s *sqlxStmtAdapter) GetContext(ctx context.Context, dest any, args ...any) error {
	return s.stmt.GetContext(ctx, dest, args...)
}

func (s *sqlxStmtAdapter) QueryContext(ctx context.Context, args ...any) (*Rows, error) {
	rows, err := s.stmt.QueryxContext(ctx, args...)
	if err != nil {
		return nil, err
	}

	return newRows(&sqlxRowsAdapter{rows: rows}), nil
}

func (s *sqlxStmtAdapter) SelectContext(ctx context.Context, dest any, args ...any) error {
	return s.stmt.SelectContext(ctx, dest, args...)
}

func (r *sqlxRowsAdapter) Close() error {
	return r.rows.Close()
}

func (r *sqlxRowsAdapter) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *sqlxRowsAdapter) Err() error {
	return r.rows.Err()
}

func (r *sqlxRowsAdapter) Next() bool {
	return r.rows.Next()
}

func (r *sqlxRowsAdapter) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *sqlxRowsAdapter) StructScan(dest any) error {
	return r.rows.StructScan(dest)
}
