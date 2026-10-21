package sqlc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/clock"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sqlcMetricsTestWriter = "sqlc-metrics-contract-test"

type sqlcMetricRecorder struct {
	batches chan metric.Data
}

func (r *sqlcMetricRecorder) GetPriority() int {
	return metric.PriorityHigh
}

func (r *sqlcMetricRecorder) Write(_ context.Context, batch metric.Data) {
	copied := make(metric.Data, len(batch))
	for i, datum := range batch {
		copyDatum := *datum
		if datum.Dimensions != nil {
			copyDatum.Dimensions = make(metric.Dimensions, len(datum.Dimensions))
			for key, value := range datum.Dimensions {
				copyDatum.Dimensions[key] = value
			}
		}
		copied[i] = &copyDatum
	}

	r.batches <- copied
}

func (r *sqlcMetricRecorder) WriteOne(ctx context.Context, datum *metric.Datum) {
	r.Write(ctx, metric.Data{datum})
}

type sqlcMetricsTestDriver struct {
	openAttempts     atomic.Int32
	firstOpen        chan struct{}
	allowFirstOpen   chan struct{}
	firstOpenRelease sync.Once
}

func (d *sqlcMetricsTestDriver) Open(string) (driver.Conn, error) {
	if d.openAttempts.Add(1) == 1 {
		close(d.firstOpen)
		<-d.allowFirstOpen

		return nil, errors.New("synthetic driver open failure")
	}

	return &sqlcMetricsTestConn{}, nil
}

type sqlcMetricsTestConn struct{}

func (c *sqlcMetricsTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *sqlcMetricsTestConn) Close() error {
	return nil
}

func (c *sqlcMetricsTestConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func TestConnectionMetricsUseCanonicalCounterAndPoolGauges(t *testing.T) {
	recorder := &sqlcMetricRecorder{batches: make(chan metric.Data, 32)}
	metric.RegisterWriterFactory(sqlcMetricsTestWriter, func(context.Context, cfg.Config, log.Logger) (metric.Writer, error) {
		return recorder, nil
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

	daemonDone := make(chan error, 1)
	go func() {
		daemonDone <- module.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-daemonDone)
	})

	testDriver := &sqlcMetricsTestDriver{
		firstOpen:      make(chan struct{}),
		allowFirstOpen: make(chan struct{}),
	}
	driverName := newMetricDriver(testDriver)
	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	releaseFirstOpen := func() {
		testDriver.firstOpenRelease.Do(func() {
			close(testDriver.allowFirstOpen)
		})
	}
	dbClosed := false
	t.Cleanup(func() {
		releaseFirstOpen()
		if !dbClosed {
			require.NoError(t, db.Close())
		}
	})

	pingResult := make(chan error, 1)
	go func() {
		pingResult <- db.Ping()
	}()
	select {
	case <-testDriver.firstOpen:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the wrapped driver open")
	}

	firstCounter := receiveDbClientBatch(t, recorder)
	require.Len(t, firstCounter, 1)
	assertConnectionCounter(t, firstCounter[0])
	releaseFirstOpen()
	require.Error(t, <-pingResult)

	require.NoError(t, db.Ping())
	secondCounter := receiveDbClientBatch(t, recorder)
	require.Len(t, secondCounter, 1)
	assertConnectionCounter(t, secondCounter[0])

	connection, err := db.Conn(ctx)
	require.NoError(t, err)
	poolWriter := metric.NewWriter("db.client")
	testClock := clock.NewFakeClock()
	publishConnectionMetricsWithInterfaces(ctx, db, testClock, poolWriter)
	assertPoolGaugeSample(t, receiveDbClientBatch(t, recorder), 1, 0)

	testClock.BlockUntilTickers(1)
	require.NoError(t, connection.Close())
	testClock.Advance(time.Minute)
	assertPoolGaugeSample(t, receiveDbClientBatch(t, recorder), 0, 1)

	require.NoError(t, db.Close())
	dbClosed = true
	testClock.Advance(time.Minute)
	assertPoolGaugeSample(t, receiveDbClientBatch(t, recorder), 0, 0)
}

func receiveDbClientBatch(t *testing.T, recorder *sqlcMetricRecorder) metric.Data {
	t.Helper()

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case batch := <-recorder.batches:
			var dbClientMetrics metric.Data
			for _, datum := range batch {
				if datum.Namespace == "db.client" {
					dbClientMetrics = append(dbClientMetrics, datum)
				}
			}
			if len(dbClientMetrics) > 0 {
				return dbClientMetrics
			}
		case <-timeout.C:
			t.Fatal("timed out waiting for db.client metrics")
		}
	}
}

func assertConnectionCounter(t *testing.T, datum *metric.Datum) {
	t.Helper()

	assert.Equal(t, "db.client", datum.Namespace)
	assert.Equal(t, "connections", datum.MetricName)
	assert.Equal(t, metric.PriorityHigh, datum.Priority)
	assert.Equal(t, metric.UnitCount, datum.Unit)
	assert.Equal(t, metric.KindCounter.Build(), datum.Kind)
	assert.Empty(t, datum.Dimensions)
	assert.Equal(t, float64(1), datum.Value)
}

func assertPoolGaugeSample(t *testing.T, batch metric.Data, used, idle float64) {
	t.Helper()
	require.Len(t, batch, 2)

	values := make(map[string]float64, 2)
	for _, datum := range batch {
		assert.Equal(t, "db.client", datum.Namespace)
		assert.Equal(t, "connection.count", datum.MetricName)
		assert.Equal(t, metric.PriorityHigh, datum.Priority)
		assert.Equal(t, metric.UnitCountAverage, datum.Unit)
		assert.Equal(t, metric.KindGauge.Build(), datum.Kind)
		require.Len(t, datum.Dimensions, 1)
		values[datum.Dimensions["connection.state"]] = datum.Value
	}

	assert.Equal(t, map[string]float64{"used": used, "idle": idle}, values)
}
