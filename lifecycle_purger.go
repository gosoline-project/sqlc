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
	var err error
	var db *sql.DB

	fkSettings := *settings
	fkSettings.Parameters = map[string]string{
		"FOREIGN_KEY_CHECKS": "0",
	}
	for k, v := range settings.Parameters {
		fkSettings.Parameters[k] = v
	}

	connection, err := newDBWithInterfaces(logger, &fkSettings)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %w", err)
	}

	db = connection.SQLDB()

	return &LifeCyclePurger{
		logger:   logger,
		db:       db,
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

	if len(tables) == 0 {
		return nil
	}

	var sqls []string
	for _, table := range tables {
		sqls = append(sqls, fmt.Sprintf("TRUNCATE TABLE %s;", table))
	}

	if _, execErr := p.db.ExecContext(ctx, strings.Join(sqls, " ")); execErr != nil {
		return fmt.Errorf("could not truncate tables: %w", execErr)
	}

	return nil
}
