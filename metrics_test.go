package sqlc_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gosoline-project/sqlc"
	sqlcMocks "github.com/gosoline-project/sqlc/mocks"
	driverMocks "github.com/gosoline-project/sqlc/mocks/driver"
	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/clock"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/metric"
	metricMocks "github.com/justtrackio/gosoline/pkg/metric/mocks"
	"github.com/justtrackio/gosoline/pkg/test/matcher"
	"github.com/justtrackio/gosoline/pkg/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const sqlcMetricsTestWriter = "sqlc-metrics-contract-test"

type metricDBConstructorResult struct {
	db  *sqlc.DB
	err error
}

func TestConnectionMetricsUseCanonicalCounterAndPoolGauges(t *testing.T) {
	metricWrites := make(chan struct{}, 6)
	metricWriter := metricMocks.NewWriter(t)
	notifyMetricWrite := metricWriteNotifier(metricWrites)

	metricWriter.EXPECT().
		GetPriority().
		Return(metric.PriorityHigh).
		Maybe()
	metricWriter.EXPECT().
		Write(matcher.Context, mock.MatchedBy(matchesNonDbClientBatch)).
		Maybe()
	metricWriter.EXPECT().
		Write(matcher.Context, mock.MatchedBy(matchesConnectionCounterBatch)).
		Run(notifyMetricWrite).
		Once()
	metricWriter.EXPECT().
		Write(matcher.Context, mock.MatchedBy(matchesConnectionCounterBatch)).
		Run(notifyMetricWrite).
		Once()
	for _, sample := range []struct {
		used float64
		idle float64
	}{
		{used: 0, idle: 1},
		{used: 1, idle: 0},
		{used: 0, idle: 1},
		{used: 0, idle: 0},
	} {
		metricWriter.EXPECT().
			Write(matcher.Context, mock.MatchedBy(func(batch metric.Data) bool {
				return matchesConnectionPoolBatch(batch, sample.used, sample.idle)
			})).
			Run(notifyMetricWrite).
			Once()
	}

	metric.RegisterWriterFactory(sqlcMetricsTestWriter, func(context.Context, cfg.Config, log.Logger) (metric.Writer, error) {
		return metricWriter, nil
	})

	ctx, cancel := context.WithCancel(appctx.WithContainer(t.Context()))
	config := cfg.New(map[string]any{
		"metric": map[string]any{
			"enabled": true,
			"writers": []string{sqlcMetricsTestWriter},
		},
	})
	module, err := metric.NewDaemonModule(ctx, config, log.NewCliLogger())
	require.NoError(t, err)
	require.NotNil(t, module)

	originalClockProvider := clock.Provider
	t.Cleanup(func() {
		clock.WithProvider(originalClockProvider)
	})
	testClock := clock.NewFakeClock()
	clock.WithProvider(testClock)

	daemonDone := make(chan error, 1)
	go func() {
		defer close(daemonDone)
		daemonDone <- module.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err, ok := <-daemonDone:
			if !ok {
				t.Error("metric daemon stopped without returning")

				return
			}

			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for metric daemon shutdown")
		}
	})

	driverName := "sqlc-metrics-contract-" + uuid.New().NewV4()
	dsn := "sqlc-metrics-contract-dsn"
	settings := &sqlc.Settings{
		Driver:             driverName,
		MaxOpenConnections: 1,
		MaxIdleConnections: 1,
	}

	settingsDriver := sqlcMocks.NewDriver(t)
	settingsDriver.EXPECT().GetDSN(settings).Return(dsn).Once()
	sqlc.AddDriverFactory(driverName, func(log.Logger) (sqlc.Driver, error) {
		return settingsDriver, nil
	})

	sqlDriver := driverMocks.NewDriver(t)
	driverConnection := driverMocks.NewConn(t)
	driverConnection.EXPECT().Close().Return(nil).Once()

	var openAttempts atomic.Int32
	firstOpen := make(chan struct{})
	allowFirstOpen := make(chan struct{})
	var firstOpenRelease sync.Once
	releaseFirstOpen := func() {
		firstOpenRelease.Do(func() {
			close(allowFirstOpen)
		})
	}
	openCall := sqlDriver.EXPECT().Open(dsn).RunAndReturn(func(string) (driver.Conn, error) {
		if openAttempts.Add(1) == 1 {
			close(firstOpen)
			<-allowFirstOpen

			return nil, driver.ErrBadConn
		}

		return driverConnection, nil
	})
	openCall.Times(2)
	sql.Register(driverName, sqlDriver)

	constructorResults := make(chan metricDBConstructorResult, 1)
	var (
		db                        *sqlc.DB
		constructorStarted        bool
		constructorResultReceived bool
		dbClosed                  bool
	)
	t.Cleanup(func() {
		releaseFirstOpen()
		db, constructorResultReceived = cleanupMetricDB(
			t,
			constructorStarted,
			constructorResultReceived,
			constructorResults,
			db,
			dbClosed,
		)
	})

	constructorStarted = true
	go func() {
		defer close(constructorResults)
		db, err := sqlc.NewDBFromSettings(ctx, log.NewCliLogger(), driverName, settings)
		constructorResults <- metricDBConstructorResult{db: db, err: err}
	}()

	select {
	case <-firstOpen:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the public constructor to open the driver")
	}
	waitForMetricWrite(t, metricWrites)
	releaseFirstOpen()

	result, hasResult, completed := waitForDBConstructorResult(constructorResults)
	constructorResultReceived = completed
	if !completed {
		t.Fatal("timed out waiting for public database constructor")
	}
	if !hasResult {
		t.Fatal("public database constructor stopped without returning")
	}
	db = result.db
	require.NoError(t, result.err)
	require.NotNil(t, db)
	require.Equal(t, int32(2), openAttempts.Load())
	waitForMetricWrite(t, metricWrites)

	waitForMetricWrite(t, metricWrites)

	sqlConnection, err := db.SQLDB().Conn(ctx)
	require.NoError(t, err)
	testClock.Advance(time.Minute)
	waitForMetricWrite(t, metricWrites)

	require.NoError(t, sqlConnection.Close())
	testClock.Advance(time.Minute)
	waitForMetricWrite(t, metricWrites)

	require.NoError(t, db.Close())
	dbClosed = true
	testClock.Advance(time.Minute)
	waitForMetricWrite(t, metricWrites)
}

func matchesNonDbClientBatch(batch metric.Data) bool {
	for _, datum := range batch {
		if datum == nil || datum.Namespace == "db.client" {
			return false
		}
	}

	return true
}

func matchesConnectionCounterBatch(batch metric.Data) bool {
	counterFound := false

	for _, datum := range batch {
		if datum == nil {
			return false
		}
		if datum.Namespace != "db.client" {
			continue
		}
		if counterFound ||
			datum.MetricName != "connections" ||
			!reflect.DeepEqual(datum.Kind, metric.KindCounter.Build()) ||
			datum.Unit != metric.UnitCount ||
			datum.Priority != metric.PriorityHigh ||
			datum.Value != 1 ||
			len(datum.Dimensions) != 0 {
			return false
		}

		counterFound = true
	}

	return counterFound
}

func matchesConnectionPoolBatch(batch metric.Data, used, idle float64) bool {
	values := make(map[string]float64, 2)
	dbClientMetrics := 0

	for _, datum := range batch {
		if datum == nil {
			return false
		}
		if datum.Namespace != "db.client" {
			continue
		}
		dbClientMetrics++

		state, ok := connectionPoolMetricState(datum)
		if !ok {
			return false
		}
		if _, found := values[state]; found {
			return false
		}

		values[state] = datum.Value
	}

	return dbClientMetrics == 2 && values["used"] == used && values["idle"] == idle
}

func connectionPoolMetricState(datum *metric.Datum) (string, bool) {
	if datum == nil ||
		datum.Namespace != "db.client" ||
		datum.MetricName != "connection.count" ||
		!reflect.DeepEqual(datum.Kind, metric.KindGauge.Build()) ||
		datum.Unit != metric.UnitCountAverage ||
		datum.Priority != metric.PriorityHigh ||
		len(datum.Dimensions) != 1 {
		return "", false
	}

	state, ok := datum.Dimensions["connection.state"]

	return state, ok && (state == "used" || state == "idle")
}

func metricWriteNotifier(metricWrites chan<- struct{}) func(context.Context, metric.Data) {
	return func(_ context.Context, _ metric.Data) {
		metricWrites <- struct{}{}
	}
}

func waitForMetricWrite(t *testing.T, metricWrites <-chan struct{}) {
	t.Helper()

	select {
	case <-metricWrites:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for db.client metric delivery")
	}
}

func waitForDBConstructorResult(results <-chan metricDBConstructorResult) (metricDBConstructorResult, bool, bool) {
	select {
	case result, ok := <-results:
		return result, ok, true
	case <-time.After(5 * time.Second):
		return metricDBConstructorResult{}, false, false
	}
}

func cleanupMetricDB(
	t *testing.T,
	constructorStarted bool,
	constructorResultReceived bool,
	constructorResults <-chan metricDBConstructorResult,
	db *sqlc.DB,
	dbClosed bool,
) (*sqlc.DB, bool) {
	if constructorStarted && !constructorResultReceived {
		result, hasResult, completed := waitForDBConstructorResult(constructorResults)
		constructorResultReceived = completed
		switch {
		case !completed:
			t.Error("timed out waiting for public database constructor during cleanup")
		case !hasResult:
			t.Error("public database constructor stopped without returning")
		default:
			db = result.db
		}
	}

	if db != nil && !dbClosed {
		require.NoError(t, db.Close())
	}

	return db, constructorResultReceived
}
