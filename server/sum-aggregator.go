package server

import (
	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"
)

type sumAggregator struct {
	name    string
	labels  metricd.LabelSet
	sum     float64
	lastTs  int64
	updated bool
}

func (a *sumAggregator) AddValue(value float64, ts int64) {
	a.sum += value
	a.lastTs = ts
	a.updated = true
}

func (a *sumAggregator) GetAggregations() []*pb.Metric {
	if a.updated {
		a.updated = false
		return []*pb.Metric{{
			Name:   a.name + "_sum",
			Labels: a.labels,
			Value:  &pb.Metric_DoubleValue{a.sum},
			Ts:     a.lastTs,
		}}
	}
	return []*pb.Metric{}
}

func NewSumAggregator(name string, labels metricd.LabelSet) aggregatorUnit {
	return &sumAggregator{
		name:    name,
		labels:  labels,
		updated: false,
	}
}
