package server

import (
	"fmt"
	"io"
	"net"

	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"
	"github.com/fluxio/logging"

	"google.golang.org/grpc"
)

// A handler for the Thrift metricd service.
type metricHandler struct {
	ch chan *pb.Metric
}

func (m *metricHandler) Report(stream pb.Server_ReportServer) error {
	for {
		v, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			logging.Errorf("Error reading the metric stream: %#v", err)
			return err
		}

		if v.Name == "" || v.GetValue() == nil {
			logging.Errorf("Invalid metric: %s", v)
			continue
		}

		if v.Labels == nil {
			v.Labels = metricd.LabelSet{}
		}

		select {
		case m.ch <- v:
		default:
			msg := "Metric buffer is full, dropping a metric: " + v.String()
			logging.Error(msg)
		}
	}
}

// RunEndpoint runs a metricd grpc endpoint on a given port,
// with all the reported Metrics submitted to the supplied channel.
func RunEndpoint(port int, ch chan *pb.Metric) error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logging.Fatalf("Failed binding to port %d: %v", port, err)
	}

	server := grpc.NewServer()
	pb.RegisterServerServer(server, &metricHandler{ch})
	return server.Serve(l)
}
