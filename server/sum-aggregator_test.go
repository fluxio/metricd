package server

import (
	"testing"

	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"
)

func assertMetricsContain(
	t *testing.T,
	metrics []*pb.Metric,
	name string,
	value float64,
	ts int64,
) {
	for _, m := range metrics {
		if m.Name == name {
			if m.GetDoubleValue() != value {
				t.Errorf("Value of %s expected to be %f, got %f",
					name, value, m.GetDoubleValue())
			}
			if m.Ts != ts {
				t.Errorf("Timestamp of %s expected to be %d, got %d",
					name, ts, m.Ts)
			}
			return
		}
	}

	t.Errorf("Could not find metric with name %s", name)
}

func assertMetricsLen(t *testing.T, metrics []*pb.Metric, length int) {
	if len(metrics) != length {
		t.Errorf("Expected to receive %d metrics, received %d instead",
			length, len(metrics))
	}
}

func TestSumAggregator(t *testing.T) {
	const metricName = "test_metric"

	sa := NewSumAggregator(metricName, metricd.LabelSet{})
	if sa == nil {
		t.Fatal("NewSumAggregator returned nil")
	}

	assertMetricsLen(t, sa.GetAggregations(), 0)

	// Add some data points. Arbitrarily start with a timestamp of 20.
	sa.AddValue(1, 20)
	sa.AddValue(2, 21)
	sa.AddValue(4, 22)

	vals := sa.GetAggregations()
	assertMetricsLen(t, vals, 2)
	assertMetricsContain(t, vals, metricName, 7, 22)
	assertMetricsContain(t, vals, metricName+"_sum", 7, 22)

	// No data points were added, so no aggregations should occur
	assertMetricsLen(t, sa.GetAggregations(), 0)

	sa.AddValue(10, 30)
	sa.AddValue(11, 31)
	sa.AddValue(104, 32)

	vals = sa.GetAggregations()
	assertMetricsLen(t, vals, 2)
	assertMetricsContain(t, vals, metricName, 125, 32)
	assertMetricsContain(t, vals, metricName+"_sum", 132, 32)

	// No data points were added, so no aggregations should occur
	assertMetricsLen(t, sa.GetAggregations(), 0)
}
