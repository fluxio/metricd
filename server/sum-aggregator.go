package server

import (
	"sync"

	"github.com/fluxio/metricd/pb"
)

type sumAggregator struct {
	name            string
	indexedLabels   map[string]string
	unindexedLabels map[string]string
	sum             float64
	bucketSum       float64
	lastTs          int64
	updated         bool
	m               sync.Mutex
}

func (a *sumAggregator) AddValue(value float64, ts int64) {
	a.m.Lock()
	defer a.m.Unlock()

	a.sum += value
	a.bucketSum += value
	a.lastTs = ts
	a.updated = true
}

// Outputs:
//    [metric_name] : The sum of all metric values over the current aggregation interval
//    [metric_name]_sum : The sum of all metric values since this aggregator was created
func (a *sumAggregator) GetAggregations() []*pb.Metric {
	a.m.Lock()
	defer a.m.Unlock()

	if a.updated {
		a.updated = false
		aggs := []*pb.Metric{
			{
				Name:            a.name + "_sum",
				IndexedLabels:   a.indexedLabels,
				UnindexedLabels: a.unindexedLabels,
				Value:           &pb.Metric_DoubleValue{a.sum},
				Ts:              a.lastTs,
			},
			{
				Name:            a.name,
				IndexedLabels:   a.indexedLabels,
				UnindexedLabels: a.unindexedLabels,
				Value:           &pb.Metric_DoubleValue{a.bucketSum},
				Ts:              a.lastTs,
			},
		}
		a.bucketSum = 0
		return aggs
	}
	return nil
}

func NewSumAggregator(name string, indexedLabels, unindexedLabels map[string]string) aggregatorUnit {
	return &sumAggregator{
		name:            name,
		indexedLabels:   indexedLabels,
		unindexedLabels: unindexedLabels,
		updated:         false,
	}
}
