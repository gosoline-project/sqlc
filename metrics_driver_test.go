package sqlc

import (
	"errors"
	"reflect"
	"testing"

	driverMocks "github.com/gosoline-project/sqlc/mocks/driver"
	"github.com/justtrackio/gosoline/pkg/metric"
	metricMocks "github.com/justtrackio/gosoline/pkg/metric/mocks"
	"github.com/justtrackio/gosoline/pkg/test/matcher"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMetricDriverCountsOpenAttemptsBeforeDelegating(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		err  error
	}{
		{
			name: "success",
		},
		{
			name: "failure",
			err:  errors.New("driver open failed"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			writer := metricMocks.NewWriter(t)
			driver := driverMocks.NewDriver(t)
			connection := driverMocks.NewConn(t)
			counted := writer.EXPECT().
				WriteOne(matcher.Context, mock.MatchedBy(func(datum *metric.Datum) bool {
					return reflect.DeepEqual(datum, &metric.Datum{
						Priority:   metric.PriorityHigh,
						MetricName: "connections",
						Unit:       metric.UnitCount,
						Value:      1,
						Kind:       metric.KindCounter.Build(),
					})
				})).
				Once()
			driver.EXPECT().
				Open("metric-test-dsn").
				Return(connection, testCase.err).
				Once().
				NotBefore(counted)

			wrapped := metricDriver{
				Driver:       driver,
				metricWriter: writer,
			}
			_, err := wrapped.Open("metric-test-dsn")
			require.ErrorIs(t, err, testCase.err)
		})
	}
}
