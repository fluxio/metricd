// The influxdb metricd plugin.
package influxdb

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/fluxio/logging"
	"github.com/fluxio/metricd/pb"
	"github.com/influxdb/influxdb/client"
)

var (
	// Batching configuration
	// TODO(avaskys): Make this configurable
	numWorkers = 8
	batchSize  = 5000
	batchTime  = 5 * time.Second
)

// InfluxDb object represents a connection to a InfluxDb server,
// located at "host:port" supplied in Loc.
type InfluxDb struct {
	// A channel, which will receive metrics from the users.
	Metrics chan *pb.Metric
	// Location of the InfluxDb server.
	Loc string
}

type influxdbWorker struct {
	// A channel which will receive metric batches
	batches chan []client.Point
	// Location of the InfluxDb server
	loc string
	// Handler to the library client.
	client *client.Client
}

func (w *influxdbWorker) connect() error {
	logging.Infof("Initializing InfluxDb Client. Connecting to %s", w.loc)

	u, err := url.Parse(fmt.Sprintf("http://%s", w.loc))
	if err != nil {
		logging.Errorf("Illegal influxdb location: %s", w.loc)
		logging.Info("Using Nop influxdb client.")
		w.client = nil
		return nil
	}

	conf := client.Config{
		URL:      *u,
		Username: "data",
		Password: "data",
	}

	w.client, err = client.NewClient(conf)

	return err
}

func (w *influxdbWorker) ensureConnected() {
	reconnect_interval := 1 * time.Second
	for w.client == nil {
		if err := w.connect(); err != nil {
			logging.Errorf("Error connecting to InfluxDb: %s", err)
		} else { // Check that the connection actually succeeded.
			if _, _, err = w.client.Ping(); err == nil {
				logging.Errorf("Connection succeeded.")
				break
			}
			logging.Errorf("Ping failed: %s", err)
			w.client = nil // Reconnect.
		}
		time.Sleep(reconnect_interval)

		// Exp backoff with a ceiling.
		reconnect_interval *= 2
		if reconnect_interval > 60*time.Second {
			reconnect_interval = 60 * time.Second
		}
	}
}

func (w *influxdbWorker) sendToInfluxDb(p []client.Point) error {
	w.ensureConnected()
	bps := client.BatchPoints{
		Points:          p,
		Database:        "data",
		RetentionPolicy: "default",
	}
	_, err := w.client.Write(bps)
	if err != nil {
		if strings.Contains(err.Error(), "field type conflict") {
			logging.Errorf("Field Type Conflict writing to Influxdb. %s", err.Error())
		} else {
			logging.Errorf(
				"Error sending value to InfluxDb: %s. Forcing Reconnection.",
				err.Error(),
			)
			w.client = nil // Force reconnect.
		}
	}
	return err
}

func (w *influxdbWorker) run() {
	for batch := range w.batches {
		w.sendToInfluxDb(batch)
	}
}

func buildPoint(m *pb.Metric) client.Point {
	validLabels := make(map[string]string)
	for k, v := range m.Labels {
		if v != "" && k != "" {
			validLabels[k] = v
		}
	}
	return client.Point{
		Measurement: m.Name,
		Tags:        validLabels,
		Fields: map[string]interface{}{
			"value": m.ValueAsFloat(),
		},
		Time: time.Unix(0, m.Ts),
	}
}

func (i *InfluxDb) Run() {
	batchChan := make(chan []client.Point, numWorkers)

	// Start our workers
	for worker := 0; worker < numWorkers; worker++ {
		worker := influxdbWorker{
			batches: batchChan,
			loc:     i.Loc,
		}
		go worker.run()
	}

	// Start batching metrics
	var curBatch []client.Point
	ticker := time.NewTicker(batchTime)
	for {
		if curBatch == nil {
			// Optimize for the case where we're receiving lots of metrics. So
			// pre-allocate the full batch size.
			curBatch = make([]client.Point, 0, batchSize)
		}

		// Send a batch when we reach batchSize or when our timer expires,
		// whichever comes first.
		select {
		case m := <-i.Metrics:
			curBatch = append(curBatch, buildPoint(m))
			if len(curBatch) == batchSize {
				batchChan <- curBatch
				curBatch = nil
			}
		case <-ticker.C:
			if len(curBatch) > 0 {
				batchChan <- curBatch
				curBatch = nil
			}
		}
	}
}

func NewInfluxDb(metricChannel chan *pb.Metric, influxDbUrl string) *InfluxDb {
	return &InfluxDb{
		Metrics: metricChannel,
		Loc:     influxDbUrl,
	}
}
