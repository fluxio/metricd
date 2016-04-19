package server

import (
	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"

	"math"
	"time"
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
func NewAggregator(in chan *pb.Metric, out chan *pb.Metric) *aggregator {
	return &aggregator{
		in:    in,
		out:   out,
		arena: make(map[string]float64),
	}
}

// The aggregator reads metrics from the `in` channel and runs necessary
// aggregations on it. The aggregation are requested via the `Aggregations()`
// field in the `Metric` object. As a result of aggregations, several
// additional metrics may  be emitted to the `out` channel. Also the original
// metric might be duplicated to `out`.
func (a *aggregator) Run() {
	for m := range a.in {
		for _, out_m := range a.process(m) {
			a.out <- out_m
		}
	}
}

type aggregator struct {
	in  chan *pb.Metric
	out chan *pb.Metric

	arena map[string]float64
}

func arenaKey(n string, l metricd.LabelSet) string {
	res := n
	for k, v := range l {
		res += "!" + k + "!" + v
	}
	return res
}

func (a *aggregator) process(m *pb.Metric) []*pb.Metric {
	var res []*pb.Metric
	for _, agg := range m.Aggregations {
		if agg == pb.Agg_SUM {
			val := m.ValueAsFloat()
			if !math.IsNaN(val) {
				p := arenaKey(m.Name, m.Labels)
				a.arena[p] += val
				new_m := &pb.Metric{
					Name:   m.Name + "_sum",
					Labels: m.Labels,
					Value:  &pb.Metric_DoubleValue{a.arena[p]},
					Ts:     time.Now().UTC().Unix(),
				}
				res = append(res, new_m)
			}
		}
	}

	// Return the original metric by default.
	return append(res, m)
}
