package sqlc_test

import (
	"testing"
	"time"

	"github.com/gosoline-project/sqlc"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	logMocks "github.com/justtrackio/gosoline/pkg/log/mocks"
	"github.com/stretchr/testify/suite"
)

func TestMysqlDriver(t *testing.T) {
	suite.Run(t, new(MysqlDriverTestSuite))
}

type MysqlDriverTestSuite struct {
	suite.Suite

	config   cfg.GosoConf
	logger   log.Logger
	settings *sqlc.Settings
}

func (s *MysqlDriverTestSuite) SetupTest() {
	s.config = cfg.New()
	err := s.config.Option(cfg.WithConfigMap(map[string]any{
		"app.name": "test",
	}))
	s.NoError(err)

	s.settings = &sqlc.Settings{}
	err = s.config.UnmarshalDefaults(s.settings)
	s.NoError(err)

	s.logger = logMocks.NewLoggerMock(logMocks.WithMockAll, logMocks.WithTestingT(s.T()))
}

func (s *MysqlDriverTestSuite) TestDsn() {
	driver, err := sqlc.NewMysqlDriver(s.logger)
	s.NoError(err)

	dsn := driver.GetDSN(s.settings)
	s.Equal("tcp(localhost:3306)/?collation=utf8mb4_general_ci&multiStatements=true&parseTime=true&charset=utf8mb4&readTimeout=0s&writeTimeout=0s", dsn)

	s.settings.Timeouts.ReadTimeout = time.Millisecond * 50
	s.settings.Timeouts.WriteTimeout = time.Millisecond * 50
	dsn = driver.GetDSN(s.settings)
	s.Equal("tcp(localhost:3306)/?collation=utf8mb4_general_ci&multiStatements=true&parseTime=true&charset=utf8mb4&readTimeout=50ms&writeTimeout=50ms", dsn)

	s.settings.Parameters["param1"] = "value1"
	dsn = driver.GetDSN(s.settings)
	s.Equal("tcp(localhost:3306)/?collation=utf8mb4_general_ci&multiStatements=true&parseTime=true&charset=utf8mb4&param1=value1&readTimeout=50ms&writeTimeout=50ms", dsn)
}
