package sqlc_test

import (
	"testing"

	sqlc "github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	logMocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/stretchr/testify/suite"
)

func TestPostgresDriver(t *testing.T) {
	suite.Run(t, new(PostgresDriverTestSuite))
}

type PostgresDriverTestSuite struct {
	suite.Suite

	config   cfg.GosoConf
	logger   log.Logger
	settings *sqlc.Settings
}

func (s *PostgresDriverTestSuite) SetupTest() {
	s.config = cfg.New()
	err := s.config.Option(cfg.WithConfigMap(map[string]any{
		"app_name": "test",
	}))
	s.NoError(err)

	s.settings = &sqlc.Settings{}
	err = s.config.UnmarshalDefaults(s.settings)
	s.NoError(err)

	s.logger = logMocks.NewLoggerMock(logMocks.WithMockAll, logMocks.WithTestingT(s.T()))
}

func (s *PostgresDriverTestSuite) TestDsn() {
	driver, err := sqlc.NewPostgresDriver(s.logger)
	s.NoError(err)

	dsn := driver.GetDSN(s.settings)
	s.Equal("host=localhost port=3306 user= dbname= password=", dsn)
}

func (s *PostgresDriverTestSuite) TestDsnWithParameters() {
	driver, err := sqlc.NewPostgresDriver(s.logger)
	s.NoError(err)

	s.settings.Parameters = map[string]string{
		"sslmode":         "disable",
		"connect_timeout": "10",
	}

	dsn := driver.GetDSN(s.settings)
	s.Contains(dsn, "host=localhost")
	s.Contains(dsn, "port=3306")
	s.Contains(dsn, "sslmode=disable")
	s.Contains(dsn, "connect_timeout=10")
}
