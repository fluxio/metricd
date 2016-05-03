package server

import (
	"strconv"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"
)

type histAggregator struct {
	name   string
	labels metricd.LabelSet
	h      *hdrhistogram.Histogram
}

func (a *histAggregator) AddValue(value float64, ts int64) {
	a.h.RecordValue(int64(value))
}

func (a *histAggregator) GetAggregations() []*pb.Metric {
	if a.h.TotalCount() != 0 {
		var res []*pb.Metric
		for _, q := range []float64{0, 50, 90, 95, 99, 100} {
			res = append(res, &pb.Metric{
				Name:   a.name + "_p" + strconv.Itoa(int(q)),
				Labels: a.labels,
				Value:  &pb.Metric_DoubleValue{float64(a.h.ValueAtQuantile(q))},
				Ts:     time.Now().Unix(),
			})
		}
		a.h = hdrhistogram.New(0, 1e8, 3)
	}
	return []*pb.Metric{}
}

func NewHistAggregator(name string, labels metricd.LabelSet) aggregatorUnit {
	return &histAggregator{
		name:   name,
		labels: labels,
		h:      hdrhistogram.New(0, 1e8, 3), // usec
	}
}
