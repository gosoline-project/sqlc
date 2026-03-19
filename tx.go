package sqlc

import (
	"context"
	"time"

	"github.com/justtrackio/gosoline/pkg/exec"
	"github.com/justtrackio/gosoline/pkg/log"
)

type tx struct {
	*baseQuerier
	ctx context.Context
	tx  dbTx
}

func newTx(ctx context.Context, logger log.Logger, executor exec.Executor, txx dbTx) Tx {
	return &tx{
		baseQuerier: newBaseQuerier(logger, executor, txx),
		ctx:         ctx,
		tx:          txx,
	}
}

func (t *tx) WithContext(ctx context.Context) Tx {
	return newTx(ctx, t.logger, t.executor, t.tx)
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
