package sqlc

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/justtrackio/gosoline/pkg/log"
)

type stdlibDBAdapter struct {
	db         *sql.DB
	driverName string
	mapper     *structMapper
}

type stdlibTxAdapter struct {
	tx         *sql.Tx
	driverName string
	mapper     *structMapper
}

type stdlibStmtAdapter struct {
	stmt   *sql.Stmt
	mapper *structMapper
}

type stdlibRowsAdapter struct {
	rows    *sql.Rows
	mapper  *structMapper
	started bool
	fields  [][]int
	values  []any
}

func newStdlibDBAdapter(db *sql.DB, driverName string) *stdlibDBAdapter {
	return &stdlibDBAdapter{
		db:         db,
		driverName: driverName,
		mapper:     defaultMapper,
	}
}

func newStdlibDBAdapterWithSettings(logger log.Logger, settings *Settings) (*stdlibDBAdapter, error) {
	driver, err := GetDriver(logger, settings.Driver)
	if err != nil {
		return nil, fmt.Errorf("could not get dsn provider for driver %s", settings.Driver)
	}

	dsn := driver.GetDSN(settings)
	genDriver, err := getGenericDriver(settings.Driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("could not get driver from %s connection factory: %w", settings.Driver, err)
	}

	metricDriverID := newMetricDriver(genDriver)
	db, err := sql.Open(metricDriverID, dsn)
	if err != nil {
		return nil, fmt.Errorf("can not connect: %w", err)
	}

	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("can not connect: %w", err)
	}

	db.SetConnMaxIdleTime(settings.ConnectionMaxIdleTime)
	db.SetConnMaxLifetime(settings.ConnectionMaxLifetime)
	db.SetMaxIdleConns(settings.MaxIdleConnections)
	db.SetMaxOpenConns(settings.MaxOpenConnections)

	return newStdlibDBAdapter(db, settings.Driver), nil
}

func (d *stdlibDBAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (dbTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &stdlibTxAdapter{tx: tx, driverName: d.driverName, mapper: d.mapper}, nil
}

func (d *stdlibDBAdapter) Close() error {
	return d.db.Close()
}

func (d *stdlibDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

func (d *stdlibDBAdapter) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return getContext(rows, dest, d.mapper)
}

func (d *stdlibDBAdapter) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	boundQuery, args, err := bindNamedMapper(d.driverName, query, arg, d.mapper)
	if err != nil {
		return nil, err
	}

	return d.db.ExecContext(ctx, boundQuery, args...)
}

func (d *stdlibDBAdapter) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	stmt, err := d.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return newStmt(&stdlibStmtAdapter{stmt: stmt, mapper: d.mapper}), nil
}

func (d *stdlibDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return newRows(&stdlibRowsAdapter{rows: rows, mapper: d.mapper}), nil
}

func (d *stdlibDBAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.db.QueryRowContext(ctx, query, args...)
}

func (d *stdlibDBAdapter) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return selectContext(rows, dest, d.mapper)
}

func (d *stdlibDBAdapter) SQLDB() *sql.DB {
	return d.db
}

func (d *stdlibDBAdapter) DriverName() string {
	return d.driverName
}

func (t *stdlibTxAdapter) Commit() error {
	return t.tx.Commit()
}

func (t *stdlibTxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *stdlibTxAdapter) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return getContext(rows, dest, t.mapper)
}

func (t *stdlibTxAdapter) NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error) {
	boundQuery, args, err := bindNamedMapper(t.driverName, query, arg, t.mapper)
	if err != nil {
		return nil, err
	}

	return t.tx.ExecContext(ctx, boundQuery, args...)
}

func (t *stdlibTxAdapter) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return newStmt(&stdlibStmtAdapter{stmt: stmt, mapper: t.mapper}), nil
}

func (t *stdlibTxAdapter) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return newRows(&stdlibRowsAdapter{rows: rows, mapper: t.mapper}), nil
}

func (t *stdlibTxAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

func (t *stdlibTxAdapter) Rollback() error {
	return t.tx.Rollback()
}

func (t *stdlibTxAdapter) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return selectContext(rows, dest, t.mapper)
}

func (t *stdlibTxAdapter) SQLTx() *sql.Tx {
	return t.tx
}

func (s *stdlibStmtAdapter) Close() error {
	return s.stmt.Close()
}

func (s *stdlibStmtAdapter) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return s.stmt.ExecContext(ctx, args...)
}

func (s *stdlibStmtAdapter) GetContext(ctx context.Context, dest any, args ...any) error {
	rows, err := s.stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}

	return getContext(rows, dest, s.mapper)
}

func (s *stdlibStmtAdapter) QueryContext(ctx context.Context, args ...any) (*Rows, error) {
	rows, err := s.stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}

	return newRows(&stdlibRowsAdapter{rows: rows, mapper: s.mapper}), nil
}

func (s *stdlibStmtAdapter) SelectContext(ctx context.Context, dest any, args ...any) error {
	rows, err := s.stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}

	return selectContext(rows, dest, s.mapper)
}

func (s *stdlibStmtAdapter) WithTx(_ context.Context, tx *sql.Tx) preparedStatement {
	if tx == nil {
		return s
	}

	return &stdlibStmtAdapter{
		stmt:   tx.Stmt(s.stmt),
		mapper: s.mapper,
	}
}

func (r *stdlibRowsAdapter) Close() error {
	return r.rows.Close()
}

func (r *stdlibRowsAdapter) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *stdlibRowsAdapter) Err() error {
	return r.rows.Err()
}

func (r *stdlibRowsAdapter) Next() bool {
	return r.rows.Next()
}

func (r *stdlibRowsAdapter) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *stdlibRowsAdapter) StructScan(dest any) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return fmt.Errorf("nil pointer passed to StructScan destination")
	}

	value = value.Elem()
	if !r.started {
		columns, err := r.rows.Columns()
		if err != nil {
			return err
		}

		r.fields = effectiveMapper(r.mapper).TraversalsByName(value.Type(), columns)
		if index, err := missingFields(r.fields); err != nil {
			return fmt.Errorf("missing destination name %s in %T", columns[index], dest)
		}

		r.values = make([]any, len(columns))
		r.started = true
	}

	if err := fieldsByTraversal(value, r.fields, r.values, true); err != nil {
		return err
	}

	if err := r.rows.Scan(r.values...); err != nil {
		return err
	}

	return r.rows.Err()
}
