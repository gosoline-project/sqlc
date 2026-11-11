package sqlc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/justtrackio/gosoline/pkg/clock"
	"github.com/justtrackio/gosoline/pkg/metric"
	"github.com/justtrackio/gosoline/pkg/uuid"
)

const (
	metricNamespace           = "db.client"
	metricNameConnectionCount = "connection.count"
	metricNameDbConnections   = "connections"

	dimensionConnectionState = "connection.state"

	connectionStateIdle = "idle"
	connectionStateUsed = "used"
)

func init() {
	metric.RegisterHelp(metricNamespace, metricNameConnectionCount, "database connections currently open")
	metric.RegisterHelp(metricNamespace, metricNameDbConnections, "database connections opened since the process started")
}

// DBStatsProvider provides database connection pool statistics.
type DBStatsProvider interface {
	Stats() sql.DBStats
}

type metricDriver struct {
	driver.Driver

	metricWriter metric.Writer
}

func newMetricDriver(driver driver.Driver) string {
	mw := metric.NewWriter(metricNamespace)

	id := uuid.New().NewV4()
	md := NewMetricDriverWithInterfaces(driver, mw)

	sql.Register(id, md)

	return id
}

// NewMetricDriverWithInterfaces creates an instrumented driver with explicit dependencies.
// It is exposed for isolated tests; database constructors add instrumentation automatically.
func NewMetricDriverWithInterfaces(wrapped driver.Driver, writer metric.Writer) driver.Driver {
	return &metricDriver{
		Driver:       wrapped,
		metricWriter: writer,
	}
}

func (m *metricDriver) Open(dsn string) (driver.Conn, error) {
	m.metricWriter.WriteOne(context.Background(), &metric.Datum{
		Priority:   metric.PriorityHigh,
		MetricName: metricNameDbConnections,
		Unit:       metric.UnitCount,
		Value:      1.0,
		Kind:       metric.KindCounter.Build(),
	})

	return m.Driver.Open(dsn)
}

func publishConnectionMetrics(ctx context.Context, conn *sql.DB) {
	PublishConnectionMetricsWithInterfaces(ctx, conn, clock.Provider, metric.NewWriter(metricNamespace))
}

// PublishConnectionMetricsWithInterfaces exposes the pool publisher for isolated tests.
// Database constructors start it automatically; applications should not call it directly.
// It samples immediately and once per minute until ctx is canceled.
// The writer must use the db.client namespace.
func PublishConnectionMetricsWithInterfaces(ctx context.Context, conn DBStatsProvider, clk clock.Clock, writer metric.Writer) {
	go func() {
		ticker := clk.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			stats := conn.Stats()

			writer.Write(ctx, metric.Data{
				connectionCountDatum(connectionStateUsed, stats.InUse),
				connectionCountDatum(connectionStateIdle, stats.Idle),
			})

			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
			}
		}
	}()
}

func connectionCountDatum(state string, count int) *metric.Datum {
	return &metric.Datum{
		Priority:   metric.PriorityHigh,
		MetricName: metricNameConnectionCount,
		Dimensions: map[string]string{
			dimensionConnectionState: state,
		},
		Unit:  metric.UnitCountAverage,
		Value: float64(count),
		Kind:  metric.KindGauge.Build(),
	}
}
