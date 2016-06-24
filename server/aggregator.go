package server

import (
	"time"

	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"
	"github.com/streamrail/concurrent-map"

	"math"
)

// Each metric may optionally specify one or more aggregations to be applied to
// it via the Aggregations field in the Metric object.  These aggregations
// imply some pre-processing on the metrics.  For example, currently there is
// only the "Sum" aggregation that indicates that this metric should be
// accumulated each time it is reported (rather than reporting only the most
// recent value).  The aggregator object processes a stream of metrics and
// applies the aggregations specified on each metric, the pushes the results
// out to the output metric stream.  In general, the processing required by an
// aggregation could completely replace the metric.  For example, the Sum
// aggregation might completely replace the raw most-recent-value Metric
// originally reported with only the cumulative value.

// Creates an instance of the aggregator, with in and out channels.
func NewAggregator(
	in chan *pb.Metric,
	out chan *pb.Metric,
	submissionCadence time.Duration,
) *aggregator {
	return &aggregator{
		in:      in,
		out:     out,
		state:   cmap.New(),
		cadence: submissionCadence,
	}
}

// The aggregator reads metrics from the `in` channel and runs necessary
// aggregations on it. The aggregation are requested via the `Aggregations()`
// field in the `Metric` object. As a result of aggregations, several
// additional metrics may  be emitted to the `out` channel. Also the original
// metric might be duplicated to `out`.
func (a *aggregator) Run() {
	go a.submitter()

	for m := range a.in {
		for _, out_m := range a.process(m) {
			a.out <- out_m
		}
	}
}

type aggregator struct {
	in  chan *pb.Metric
	out chan *pb.Metric

	state   cmap.ConcurrentMap
	cadence time.Duration
}

// Interface that all aggregators must implement. Note that AddValue() and
// GetAggregations() are called from different goroutines, and thus must be
// thread-safe with regard to each other.
type aggregatorUnit interface {
	AddValue(value float64, ts int64)
	GetAggregations() []*pb.Metric
}

func stateKey(n string, l metricd.LabelSet) string {
	res := n
	for k, v := range l {
		res += "!" + k + "!" + v
	}
	return res
}

func (a *aggregator) submitter() {
	tick := time.NewTicker(a.cadence)

	for range tick.C {
		for t := range a.state.Iter() {
			for _, m := range t.Val.(aggregatorUnit).GetAggregations() {
				a.out <- m
			}
		}
	}
}

func (a *aggregator) process(m *pb.Metric) []*pb.Metric {
	emitOriginal := true
	var res []*pb.Metric
	for _, agg := range m.Aggregations {
		// Don't emit original metric if there is an aggregation.
		emitOriginal = false

		val := m.ValueAsFloat()
		if !math.IsNaN(val) {
			p := stateKey(m.Name, m.Labels)
			u, exists := a.state.Get(p)

			if agg == pb.Agg_HIST {
				if !exists {
					u = NewHistAggregator(m.Name, m.Labels)
					a.state.Set(p, u)
				}
				u.(aggregatorUnit).AddValue(val, m.Ts)
			}

			if agg == pb.Agg_SUM {
				if !exists {
					u = NewSumAggregator(m.Name, m.Labels)
					a.state.Set(p, u)
				}
				u.(aggregatorUnit).AddValue(val, m.Ts)
			}
		}
	}

	if emitOriginal {
		res = append(res, m)
	}
	return res
}
