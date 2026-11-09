package sqlc

import (
	"context"
	"database/sql"
)

//go:generate env MOCKERY_INCLUDE_INTERFACE_REGEX=^Tx$DOLLAR mockery --config .mockery-select.yml

// Tx represents a transaction-scoped query client.
type Tx interface {
	context.Context
	Querier
	Q() *QueryBuilder
	Commit() error
	Rollback() error
	SQLTx() *sql.Tx
	WithContext(ctx context.Context) Tx
}
