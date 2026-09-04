package sqlc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/funk"
	"github.com/justtrackio/gosoline/pkg/log"
)

var tableExcludes = []string{
	"goose_db_version",
}

type LifeCyclePurger struct {
	logger   log.Logger
	db       *sql.DB
	settings *Settings
}

func NewLifeCyclePurger(config cfg.Config, logger log.Logger, connectionName string) (*LifeCyclePurger, error) {
	var err error
	var settings *Settings

	if settings, err = ReadSettings(config, connectionName); err != nil {
		return nil, fmt.Errorf("error reading db settings for connection %q: %w", connectionName, err)
	}

	return NewLifeCyclePurgerWithSettings(logger, settings)
}

func NewLifeCyclePurgerWithSettings(logger log.Logger, settings *Settings) (*LifeCyclePurger, error) {
	connection, err := newDBWithInterfaces(logger, settings)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %w", err)
	}

	return &LifeCyclePurger{
		logger:   logger,
		db:       connection.SQLDB(),
		settings: settings,
	}, nil
}

func (p LifeCyclePurger) Purge(ctx context.Context) (err error) {
	var tables []string

	defer func() {
		if closeErr := p.db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("could not close database: %w", closeErr))
		}
	}()

	rows, err := p.db.QueryContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?;", p.settings.Uri.Database)
	if err != nil {
		return fmt.Errorf("failed to check tables of database: %w", err)
	}

	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			// on error, we will end the iteration and read the error afterwards with rows.Err()
			break
		}
		tables = append(tables, table)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("could not close rows: %w", err)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("could not iterate over rows: %w", err)
	}

	tables = funk.Filter(tables, func(s string) bool {
		return !funk.Contains(tableExcludes, s)
	})

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not start purge transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("could not roll back purge transaction: %w", rollbackErr))
		}
	}()

	statements := make([]string, 0, len(tables)+2)
	statements = append(statements, "SET FOREIGN_KEY_CHECKS = 0;")
	for _, table := range tables {
		quotedTable := strings.ReplaceAll(table, "`", "``")
		statements = append(statements, fmt.Sprintf("TRUNCATE TABLE `%s`;", quotedTable))
	}
	statements = append(statements, "SET FOREIGN_KEY_CHECKS = 1;")

	if _, err = tx.ExecContext(ctx, strings.Join(statements, " ")); err != nil {
		return fmt.Errorf("could not truncate tables: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("could not commit purge transaction: %w", err)
	}

	return nil
}
