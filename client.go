// Metricd is a system used to collect app-level metrics.
// The package includes a daemon, an accompanying client, which provide a
// mechanism for collection of the metrics. The aggregation and storage logic
// is encapsulated in a set of metricd plugins, found in the `plugins` package.
package metricd

import (
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/fluxio/logging"
	"github.com/fluxio/metricd/pb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const metricsBufferSize = 10000
const shutdownTimeout = 10 * time.Second

const connectionTimeout = 5 * time.Second

// Metric queue length considered "normal".
// Past that size a warning message is logged.
const normalQueueSize = 100

// How often to inspect the queue length.
const reportPeriod = int64(1000)

// Labelset represents a set of metric labels.
type LabelSet map[string]string

// Client provides a mechanism to submit metrics to metricd.
// A metric is identified by a name and by a set of labels (string pairs),
// which are used as dimensions to aggregate across. Values can be ints,
// floats, doubles, or strings.
// Close has to be called every time the Client is about to be discarded.
type Client interface {
	// Terminates the connection to metricd. The client is unusable after
	// this call.
	Close() error

	// Reports a "gauge" metric. The timeseries will keep the provided value
	// until the next call with the same name and the set of labels provides
	// a new one.
	Report(name string, labels LabelSet, value interface{}) error

	// The provided value is going to be added to the current integral
	// of all values reported for this timeseries. The result is
	// aggregated into a float variable with the same name and  a "_sum"
	// suffix. Negative values and zeroes are allowed.
	ReportSum(name string, labels LabelSet, value interface{}) error

	// The reported values are recorded in a histogram over a set period.
	ReportHistogram(name string, labels LabelSet, value interface{}) error

	// Returns a timer instance, which can be used for reporting time
	// length metrics.
	Timer(name string, labels LabelSet) Timer
}

type realClient struct {
	addr             *url.URL
	conn             *grpc.ClientConn
	client           pb.ServerClient
	stream           pb.Server_ReportClient
	metrics          chan pb.Metric
	shutdownComplete chan bool
	bufferFull       int32
}

func (c *realClient) tryConnect() error {
	// Close possibly existing connection.
	if c.conn != nil && c.conn.State() != grpc.Shutdown {
		c.conn.Close()
	}
	var err error
	c.conn, err = grpc.Dial(c.addr.Host, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("Error creating grpc connection: %s.", err)
	}

	c.client = pb.NewServerClient(c.conn)
	c.stream, err = c.client.Report(context.Background())

	logging.Infof("Connection to metricd succeeded.")
	return nil
}

func (c *realClient) init(addr *url.URL) {
	c.metrics = make(chan pb.Metric, metricsBufferSize)
	c.shutdownComplete = make(chan bool)

	c.addr = addr
}

// Has to be called before the client is to be destroyed.
func (c realClient) Close() error {
	close(c.metrics) // Notifies the goroutine, that it needs to flush and shut down.

	select {
	case <-c.shutdownComplete:
		logging.Info("Metricd client shutdown complete.")
		return nil
	case <-time.After(shutdownTimeout):
		return fmt.Errorf("Timed out shutting down the metricd client. " +
			"Some metrics could have been lost.")
	}
}

func (c *realClient) Report(
	name string,
	labels LabelSet,
	value interface{},
) error {
	return c.submit(name, labels, value, nil)
}

func (c *realClient) ReportSum(
	name string,
	labels LabelSet,
	value interface{},
) error {
	return c.submit(name, labels, value, []pb.Agg{pb.Agg(pb.Agg_value["SUM"])})
}

func (c *realClient) ReportHistogram(
	name string,
	labels LabelSet,
	value interface{},
) error {
	return c.submit(name, labels, value, []pb.Agg{pb.Agg(pb.Agg_value["HIST"])})
}

func (c *realClient) submit(
	name string,
	labels LabelSet,
	value interface{},
	agg []pb.Agg,
) error {
	m := pb.Metric{
		Name:         name,
		Labels:       labels,
		Ts:           time.Now().UTC().UnixNano(),
		Aggregations: agg}

	switch v := value.(type) {
	case int:
		m.Value = &pb.Metric_IntValue{int64(v)}
	case int32:
		m.Value = &pb.Metric_IntValue{int64(v)}
	case int64:
		m.Value = &pb.Metric_IntValue{v}
	case float32:
		m.Value = &pb.Metric_DoubleValue{float64(v)}
	case float64:
		m.Value = &pb.Metric_DoubleValue{v}
	case string:
		m.Value = &pb.Metric_StringValue{v}
	default:
		return fmt.Errorf("Unsupported type: %T", v)
	}

	// Drop a metric on the floor if the buffer is full.
	// FIXME(fwd): Drop older metrics instead of the newer ones.
	select {
	case c.metrics <- m:
		// Wait until the queue drops to normal levels before reporting that
		// reporting is back online. Then unset the "buffer full" flag if it's set.
		if len(c.metrics) < normalQueueSize && atomic.CompareAndSwapInt32(&c.bufferFull, 1, 0) {
			logging.Info("Metricd buffer has returned to a normal level.")
		}
	default:
		msg := "Metric buffer is full, dropping metrics."
		// Set the "buffer full" flag.
		if atomic.CompareAndSwapInt32(&c.bufferFull, 0, 1) {
			logging.Error(msg)
		}
		return fmt.Errorf(msg)
	}

	return nil
}

func (c *realClient) Timer(name string, labels LabelSet) Timer {
	return &timer{
		name:   name,
		labels: labels,
		client: c,
	}
}

func (c *realClient) reconnectWithExpBackoff() {
	wait := 20 * time.Millisecond
	for err := c.tryConnect(); err != nil; err = c.tryConnect() {
		logging.Errorf("Error connecting to metricd: %v", err)
		time.Sleep(wait)
		wait *= 2
		if wait > time.Minute {
			wait = time.Minute
		}
	}
}

func (c *realClient) run() {
	cnt := int64(0)
	latency := time.Duration(0)
	for m := range c.metrics {
		cnt++
		for {
			if c.client == nil {
				c.reconnectWithExpBackoff()
			}

			if cnt%reportPeriod == 0 {
				l := len(c.metrics)
				if l > normalQueueSize {
					logging.Infof("Metricd falling behind: %d in queue."+
						" Avg latency: %dµs", l, latency.Nanoseconds()/(reportPeriod*1000))
				}
				latency = 0
			}

			s := time.Now()
			err := c.stream.Send(&m)
			latency += time.Since(s)

			if err != nil {
				logging.Errorf("Failed reporting to metricd: %v", err)
				c.client = nil // Will trigger re-connection.
			} else {
				break // Successful submission.
			}
		}
	}

	logging.Info("Metricd client started shutdown.")
	c.stream.CloseSend()
	c.shutdownComplete <- true
}

type nopClient struct{}

func (nopClient) Close() error { return nil }
func (nopClient) Report(
	name string,
	labels LabelSet,
	value interface{},
) error {
	return nil
}
func (nopClient) ReportSum(
	name string,
	labels LabelSet,
	value interface{},
) error {
	return nil
}
func (nopClient) ReportHistogram(
	name string,
	labels LabelSet,
	value interface{},
) error {
	return nil
}
func (*nopClient) Timer(string, LabelSet) Timer { return NopTimer{} }

func newNopClient() Client {
	return &nopClient{}
}

func newRealClient(addr *url.URL) (Client, error) {
	c := &realClient{}
	c.init(addr)
	go c.run()
	return c, nil
}

// NewClient makes a real client that sends metrics to the specified
// metricd instance if a valid url is provided. If the addr == nil, this
// returns a client that does nothing and just drops values on the floor.
func NewClient(addr *url.URL) (Client, error) {
	if addr == nil || addr.Host == "" {
		logging.Info("Using nop metricd client.")
		return newNopClient(), nil
	}
	logging.Infof("Using metricd at: %s", addr.Host)
	return newRealClient(addr)
}

func NopClientOnError(c Client, err error) Client {
	if err != nil {
		logging.Errorf("Failed to create real client: %v", err)
		logging.Error("Metrics will be discarded.")
		return newNopClient()
	}
	return c
}
