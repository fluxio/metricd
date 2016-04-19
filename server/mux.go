package server

import (
	"github.com/fluxio/metricd/pb"
	"github.com/fluxio/logging"
)

var MUX_CHANNEL_BUFFER_SIZE = MAIN_CHANNEL_BUFFER_SIZE

type Mux interface {
	NewReceiver(receiverName string) chan *pb.Metric
	Run()
}

func NewMux(in chan *pb.Metric) Mux {
	return &mux{make(map[string]chan *pb.Metric, 0), in}
}

func (mux *mux) NewReceiver(receiverName string) chan *pb.Metric {
	ch := make(chan *pb.Metric, MUX_CHANNEL_BUFFER_SIZE)
	mux.chs[receiverName] = ch
	return ch
}

func (mux *mux) Run() {
	failedBefore := make(map[string]bool)
	for m := range mux.in {
		for name, ch := range mux.chs {
			select {
			case ch <- m:
				if failedBefore[name] {
					failedBefore[name] = false
					logging.Errorf("Receiver %s recovered.", name)
				}
			default:
				if !failedBefore[name] { // Only report failure once.
					failedBefore[name] = true
					logging.Errorf("Receiver %s is not consuming fast enough. "+
						"Dropping metrics.", name)
				}
			}
		}
	}
}

type mux struct {
	chs map[string]chan *pb.Metric
	in  chan *pb.Metric
}
