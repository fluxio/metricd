package server

import (
	"strconv"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"
)

const LOWEST_VALUE = 0
const HIGHEST_VALUE = 1e7

// HdrHistogram doesn't store the values precisely, instead it maintains
// counts with a number of digits of precision requested.
const DIGITS_OF_PRECISION = 2

type histAggregator struct {
	name   string
	labels metricd.LabelSet
	// usec.
	// Initialized lazily in AddValue().
	h *hdrhistogram.Histogram
}

func (a *histAggregator) AddValue(value float64, ts int64) {
	if a.h == nil {
		a.h = hdrhistogram.New(LOWEST_VALUE, HIGHEST_VALUE, DIGITS_OF_PRECISION)
	}
	a.h.RecordValue(int64(value))
}

func (a *histAggregator) GetAggregations() []*pb.Metric {
	var res []*pb.Metric
	if a.h != nil && a.h.TotalCount() != 0 {
		for _, q := range []float64{0, 50, 90, 95, 99, 100} {
			res = append(res, &pb.Metric{
				Name:   a.name + "_p" + strconv.Itoa(int(q)),
				Labels: a.labels,
				Value:  &pb.Metric_DoubleValue{float64(a.h.ValueAtQuantile(q))},
				Ts:     time.Now().Unix(),
			})
		}
		// No point of keeping an empty histogram around, it can be large.
		a.h = nil
	}
	return res
}

func NewHistAggregator(name string, labels metricd.LabelSet) aggregatorUnit {
	return &histAggregator{
		name:   name,
		labels: labels,
	}
}
