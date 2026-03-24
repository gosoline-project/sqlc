package sqlc

import (
	"context"
	"database/sql"
)

type dbQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
}

type dbHandle interface {
	dbQuerier
	BeginTx(ctx context.Context, opts *sql.TxOptions) (dbTx, error)
	Close() error
	SQLDB() *sql.DB
}

type dbTx interface {
	dbQuerier
	Commit() error
	Rollback() error
}

type rowsScanner interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...any) error
	StructScan(dest any) error
}

type preparedStatement interface {
	Close() error
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest any, args ...any) error
	QueryContext(ctx context.Context, args ...any) (*Rows, error)
	SelectContext(ctx context.Context, dest any, args ...any) error
	WithTx(ctx context.Context, tx *sql.Tx) preparedStatement
}

// Rows represents the result of a Query operation for row-by-row iteration.
type Rows struct {
	rows rowsScanner
}

func newRows(rows rowsScanner) *Rows {
	return &Rows{rows: rows}
}

// Rows.Close releases the underlying row iterator resources.
func (r *Rows) Close() error {
	return r.rows.Close()
}

// Rows.Columns reports the column names for the current result set.
func (r *Rows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Rows.Err returns the terminal iteration error, if any.
func (r *Rows) Err() error {
	return r.rows.Err()
}

// Rows.Next advances the iterator to the next row.
func (r *Rows) Next() bool {
	return r.rows.Next()
}

// Rows.Scan copies the current row into primitive destinations.
func (r *Rows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Rows.StructScan copies the current row into a struct destination.
func (r *Rows) StructScan(dest any) error {
	return r.rows.StructScan(dest)
}

// Stmt represents a prepared statement.
type Stmt struct {
	stmt preparedStatement
}

func newStmt(stmt preparedStatement) *Stmt {
	return &Stmt{stmt: stmt}
}

// Stmt.Close releases the prepared statement resources.
func (s *Stmt) Close() error {
	return s.stmt.Close()
}

// Stmt.ExecContext executes the prepared statement without returning rows.
func (s *Stmt) ExecContext(ctx context.Context, args ...any) (Result, error) {
	return s.stmt.ExecContext(ctx, args...)
}

// Stmt.GetContext executes the prepared statement and scans a single row.
func (s *Stmt) GetContext(ctx context.Context, dest any, args ...any) error {
	return s.stmt.GetContext(ctx, dest, args...)
}

// QueryContext executes the prepared statement and returns rows.
func (s *Stmt) QueryContext(ctx context.Context, args ...any) (*Rows, error) {
	return s.stmt.QueryContext(ctx, args...)
}

// QueryxContext executes the prepared statement and returns rows.
// Deprecated: use QueryContext.
func (s *Stmt) QueryxContext(ctx context.Context, args ...any) (*Rows, error) {
	return s.QueryContext(ctx, args...)
}

// Stmt.SelectContext executes the prepared statement and scans all rows into dest.
func (s *Stmt) SelectContext(ctx context.Context, dest any, args ...any) error {
	return s.stmt.SelectContext(ctx, dest, args...)
}

// Stmt.WithTx binds the prepared statement to the provided transaction.
// Statements prepared on the shared client can be reused safely inside a
// transaction by creating a transaction-scoped wrapper around the same SQL.
func (s *Stmt) WithTx(ctx context.Context, tx *sql.Tx) *Stmt {
	if tx == nil {
		return s
	}

	return newStmt(s.stmt.WithTx(ctx, tx))
}
