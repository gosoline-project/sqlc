package sqlc

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
)

type (
	Tx interface {
		context.Context
		Querier
		Q() *QueryBuilder
		Commit() error
		Rollback() error
		SqlTx() *sqlx.Tx
	}
)

type tx struct {
	*baseQuerier
	ctx context.Context
	tx  *sqlx.Tx
}

func newTx(ctx context.Context, logger log.Logger, executor exec.Executor, txx *sqlx.Tx) Tx {
	return &tx{
		baseQuerier: newBaseQuerier(logger, executor, txx),
		ctx:         ctx,
		tx:          txx,
	}
}

func (t *tx) Deadline() (deadline time.Time, ok bool) {
	return t.ctx.Deadline()
}

func (t *tx) Done() <-chan struct{} {
	return t.ctx.Done()
}

func (t *tx) Err() error {
	return t.ctx.Err()
}

func (t *tx) Value(key any) any {
	return t.ctx.Value(key)
}

func (t *tx) Q() *QueryBuilder {
	return &QueryBuilder{client: t}
}

func (t *tx) Commit() error {
	return t.tx.Commit()
}

func (t *tx) Rollback() error {
	return t.tx.Rollback()
}

func (t *tx) SqlTx() *sqlx.Tx {
	return t.tx
}
