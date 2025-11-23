package sqlc

import (
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

// Settings contains all configuration options for a database connection.
// It can be populated from a configuration file using ReadSettings() or
// constructed manually for programmatic configuration.
//
// Example configuration (YAML):
//
//	sqlc:
//	  main:
//	    driver: mysql
//	    uri:
//	      host: localhost
//	      port: 3306
//	      user: myapp
//	      password: secret
//	      database: myapp_db
//	    charset: utf8mb4
//	    max_open_connections: 25
//
// For PostgreSQL, use additional connection parameters:
//
//	sqlc:
//	  pg_main:
//	    driver: postgres
//	    uri:
//	      host: localhost
//	      port: 5432
//	      user: myapp
//	      password: secret
//	      database: myapp_db
//	    parameters:
//	      sslmode: disable
//	      connect_timeout: "10"
type Settings struct {
	Charset               string            `cfg:"charset" default:"utf8mb4"`
	Collation             string            `cfg:"collation" default:"utf8mb4_general_ci"`
	ConnectionMaxIdleTime time.Duration     `cfg:"connection_max_idletime" default:"120s"`
	ConnectionMaxLifetime time.Duration     `cfg:"connection_max_lifetime" default:"120s"`
	Driver                string            `cfg:"driver"`
	MaxIdleConnections    int               `cfg:"max_idle_connections" default:"2"` // 0 or negative number=no idle connections, sql driver default=2
	MaxOpenConnections    int               `cfg:"max_open_connections" default:"0"` // 0 or negative number=unlimited, sql driver default=0
	Migrations            MigrationSettings `cfg:"migrations"`
	MultiStatements       bool              `cfg:"multi_statements" default:"true"`
	Parameters            map[string]string `cfg:"parameters"`
	ParseTime             bool              `cfg:"parse_time" default:"true"`
	Retry                 SettingsRetry     `cfg:"retry"`
	Timeouts              SettingsTimeout   `cfg:"timeouts"`
	Uri                   SettingsUri       `cfg:"uri"`
}

// SettingsUri contains the database connection URI components.
// These values are combined to form the database connection string.
type SettingsUri struct {
	Host     string `cfg:"host" default:"localhost" validation:"required"`
	Port     int    `cfg:"port" default:"3306" validation:"required"`
	User     string `cfg:"user" validation:"required"`
	Password string `cfg:"password" validation:"required"`
	Database string `cfg:"database" validation:"required"`
}

// SettingsRetry controls automatic retry behavior for database operations.
// When enabled, failed operations will be retried according to the retry policy.
type SettingsRetry struct {
	Enabled bool `cfg:"enabled" default:"false"`
}

// SettingsTimeout contains various timeout settings for database operations.
// All timeout values must be decimal numbers with unit suffixes:
// "ms" (milliseconds), "s" (seconds), "m" (minutes), "h" (hours).
// A value of "0" means no timeout.
//
// Examples: "30s", "500ms", "1m30s", "0.5m"
type SettingsTimeout struct {
	ReadTimeout  time.Duration `cfg:"readTimeout" default:"0"`  // I/O read timeout. The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	WriteTimeout time.Duration `cfg:"writeTimeout" default:"0"` // I/O write timeout. The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	Timeout      time.Duration `cfg:"timeout" default:"0"`      // Timeout for establishing connections, aka dial timeout. The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
}

// ReadSettings reads database connection settings from the application configuration.
// It looks for settings under the key "sqlg.<name>" in the configuration.
// Returns an error if the configuration key doesn't exist or if unmarshalling fails.
//
// Example:
//
//	settings, err := ReadSettings(config, "main")
//	// Reads from config key: sqlg.main
//
//	client, err := NewClientWithSettings(ctx, logger, settings)
func ReadSettings(config cfg.Config, name string) (*Settings, error) {
	key := fmt.Sprintf("sqlc.%s", name)

	if !config.IsSet(key) {
		return nil, fmt.Errorf("there is no sqlc connection with name %q configured - key should be sqlc.%s", name, name)
	}

	settings := &Settings{}
	if err := config.UnmarshalKey(key, settings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal db settings for %s: %w", name, err)
	}

	return settings, nil
}
