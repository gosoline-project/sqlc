package sqlc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/reslife"
)

type connectionCtxKey string

func provideDBFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (dbHandle, error) {
	return appctx.Provide(ctx, connectionCtxKey(fmt.Sprint(settings)), func() (dbHandle, error) {
		return newDBFromSettings(ctx, logger, name, settings)
	})
}

func newDBFromSettings(ctx context.Context, logger log.Logger, name string, settings *Settings) (dbHandle, error) {
	var (
		err        error
		connection dbHandle
	)

	if connection, err = newDBWithInterfaces(logger, settings); err != nil {
		return nil, fmt.Errorf("can not create connection: %w", err)
	}

	if err := reslife.AddLifeCycleer(ctx, NewLifecycleManager(name, settings)); err != nil {
		return nil, err
	}

	if err = runMigrations(ctx, logger, settings, connection.SQLDB()); err != nil {
		return nil, fmt.Errorf("can not run migrations: %w", err)
	}

	publishConnectionMetrics(connection.SQLDB())

	return connection, nil
}

func newDBWithInterfaces(logger log.Logger, settings *Settings) (dbHandle, error) {
	return newSQLXDBAdapterWithSettings(logger, settings)
}

func getGenericDriver(driverName, dsn string) (driver.Driver, error) {
	initDb, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	err = initDb.Close()
	if err != nil {
		return nil, err
	}

	genDriver := initDb.Driver()

	return genDriver, nil
}
