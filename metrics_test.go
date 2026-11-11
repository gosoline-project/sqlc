package sqlc_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/gosoline-project/sqlc"
	sqlcMocks "github.com/gosoline-project/sqlc/mocks"
	"github.com/justtrackio/gosoline/pkg/clock"
	"github.com/justtrackio/gosoline/pkg/metric"
	metricMocks "github.com/justtrackio/gosoline/pkg/metric/mocks"
	"github.com/justtrackio/gosoline/pkg/test/matcher"
	"github.com/stretchr/testify/mock"
)

func TestPublishConnectionMetricsSamplesPoolStates(t *testing.T) {
	t.Parallel()

	stats := sqlcMocks.NewDBStatsProvider(t)
	writer := metricMocks.NewWriter(t)
	clk := clock.NewFakeClock()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	writes := make(chan struct{}, 1)

	samples := []struct {
		stats sql.DBStats
		used  float64
		idle  float64
	}{
		{
			stats: sql.DBStats{
				OpenConnections: 3,
				InUse:           2,
				Idle:            1,
			},
			used: 2,
			idle: 1,
		},
		{
			stats: sql.DBStats{
				OpenConnections: 2,
				Idle:            2,
			},
			used: 0,
			idle: 2,
		},
		{
			stats: sql.DBStats{},
			used:  0,
			idle:  0,
		},
	}
	for _, sample := range samples {
		stats.EXPECT().Stats().Return(sample.stats).Once()
		writer.EXPECT().
			Write(matcher.Context, mock.MatchedBy(func(batch metric.Data) bool {
				return matchesConnectionPoolBatch(batch, sample.used, sample.idle)
			})).
			Run(func(context.Context, metric.Data) {
				writes <- struct{}{}
			}).
			Once()
	}

	sqlc.PublishConnectionMetricsWithInterfaces(ctx, stats, clk, writer)
	waitForMetricWrite(t, writes)
	for range samples[1:] {
		clk.Advance(time.Minute)
		waitForMetricWrite(t, writes)
	}
}

func matchesConnectionPoolBatch(batch metric.Data, used, idle float64) bool {
	if len(batch) != 2 {
		return false
	}

	expected := map[string]float64{
		"used": used,
		"idle": idle,
	}
	for _, datum := range batch {
		if !isConnectionPoolGauge(datum) {
			return false
		}

		state := datum.Dimensions["connection.state"]
		value, ok := expected[state]
		if !ok || datum.Value != value {
			return false
		}
		delete(expected, state)
	}

	return len(expected) == 0
}

func isConnectionPoolGauge(datum *metric.Datum) bool {
	return datum != nil &&
		datum.Namespace == "" &&
		datum.MetricName == "connection.count" &&
		reflect.DeepEqual(datum.Kind, metric.KindGauge.Build()) &&
		datum.Unit == metric.UnitCountAverage &&
		datum.Priority == metric.PriorityHigh &&
		len(datum.Dimensions) == 1
}

func waitForMetricWrite(t *testing.T, writes <-chan struct{}) {
	t.Helper()

	select {
	case <-writes:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a pool metric sample")
	}
}
