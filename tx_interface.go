package sqlc

import (
	"context"
	"database/sql"
)

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
