package server

import (
	"strconv"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/fluxio/metricd/pb"
)

const LOWEST_VALUE = 0
const HIGHEST_VALUE = 1e7

// HdrHistogram doesn't store the values precisely, instead it maintains
// counts with a number of digits of precision requested.
const DIGITS_OF_PRECISION = 2

type histAggregator struct {
	name            string
	indexedLabels   map[string]string
	unindexedLabels map[string]string
	// usec.
	// Initialized lazily in AddValue().
	h *hdrhistogram.Histogram
	m sync.Mutex
}

func (a *histAggregator) AddValue(value float64, ts int64) {
	a.m.Lock()
	defer a.m.Unlock()

	if a.h == nil {
		a.h = hdrhistogram.New(LOWEST_VALUE, HIGHEST_VALUE, DIGITS_OF_PRECISION)
	}
	a.h.RecordValue(int64(value))
}

// Outputs:
//    [metric_name]_0 : Minimum value in the aggregation interval
//    [metric_name]_50 : Median value in the aggregation interval
//    [metric_name]_90 : 90th percentile value in the aggregation interval
//    [metric_name]_95 : 95th percentile value in the aggregation interval
//    [metric_name]_99 : 99th percentile value in the aggregation interval
//    [metric_name]_100 : Maximum value in the aggregation interval
func (a *histAggregator) GetAggregations() []*pb.Metric {
	a.m.Lock()
	defer a.m.Unlock()

	var res []*pb.Metric
	if a.h != nil && a.h.TotalCount() != 0 {
		for _, q := range []float64{0, 50, 90, 95, 99, 100} {
			res = append(res, &pb.Metric{
				Name:            a.name + "_p" + strconv.Itoa(int(q)),
				IndexedLabels:   a.indexedLabels,
				UnindexedLabels: a.unindexedLabels,
				Value:           &pb.Metric_DoubleValue{float64(a.h.ValueAtQuantile(q))},
				Ts:              time.Now().UnixNano(),
			})
		}
		// No point of keeping an empty histogram around, it can be large.
		a.h = nil
	}
	return res
}

func NewHistAggregator(name string, indexedLabels, unindexedLabels map[string]string) aggregatorUnit {
	return &histAggregator{
		name:            name,
		indexedLabels:   indexedLabels,
		unindexedLabels: unindexedLabels,
	}
}
