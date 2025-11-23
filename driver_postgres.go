package sqlc

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/log"
	_ "github.com/lib/pq"
)

const DriverPostgres = "postgres"

func init() {
	AddDriverFactory(DriverPostgres, NewPostgresDriver)
}

func NewPostgresDriver(logger log.Logger) (Driver, error) {
	return &postgresDriver{}, nil
}

type postgresDriver struct{}

func (m *postgresDriver) GetDSN(settings *Settings) string {
	host := settings.Uri.Host
	port := settings.Uri.Port
	user := settings.Uri.User
	dbname := settings.Uri.Database
	pass := settings.Uri.Password

	// set up postgres sql to open it.
	psqlSetup := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s", host, port, user, dbname, pass)

	// Add additional connection parameters
	for k, v := range settings.Parameters {
		psqlSetup = fmt.Sprintf("%s %s=%s", psqlSetup, k, v)
	}

	return psqlSetup
}

func (m *postgresDriver) GetPlaceholder() string {
	return "$"
}

func (m *postgresDriver) GetQuote() string {
	return `"`
}
