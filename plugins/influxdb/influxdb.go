// The influxdb metricd plugin.
package influxdb

import (
	"fmt"
	"net/url"
	"time"

	"github.com/fluxio/metricd/pb"
	"github.com/fluxio/logging"

	"github.com/influxdb/influxdb/client"
)

// InfluxDb object represents a connection to a InfluxDb server,
// located at "host:port" supplied in Loc.
type InfluxDb struct {
	// A channel, which will receive metrics from the users.
	Metrics chan *pb.Metric
	// Location of the InfluxDb server.
	Loc string
	// Handler to the library client.
	client *client.Client
}

func (i *InfluxDb) connect() error {
	logging.Infof("Initializing InfluxDb Client. Connecting to %s", i.Loc)

	u, err := url.Parse(fmt.Sprintf("http://%s", i.Loc))
	if err != nil {
		logging.Errorf("Illegal influxdb location: %s", i.Loc)
		logging.Info("Using Nop influxdb client.")
		i.client = nil
		return nil
	}

	conf := client.Config{
		URL:      *u,
		Username: "data",
		Password: "data",
	}

	i.client, err = client.NewClient(conf)

	return err
}

func (i *InfluxDb) ensureConnected() {
	reconnect_interval := 1 * time.Second
	for i.client == nil {
		if err := i.connect(); err != nil {
			logging.Errorf("Error connecting to InfluxDb: %s", err)
		} else { // Check that the connection actually succeeded.
			if _, _, err = i.client.Ping(); err == nil {
				logging.Errorf("Connection succeeded.")
				break
			}
			logging.Errorf("Ping failed: %s", err)
			i.client = nil // Reconnect.
		}
		time.Sleep(reconnect_interval)

		// Exp backoff with a ceiling.
		reconnect_interval *= 2
		if reconnect_interval > 60*time.Second {
			reconnect_interval = 60 * time.Second
		}
	}
}

func (i *InfluxDb) sendToInfluxDb(p client.Point) error {
	i.ensureConnected()
	bps := client.BatchPoints{
		Points:          []client.Point{p},
		Database:        "data",
		RetentionPolicy: "default",
	}
	_, err := i.client.Write(bps)
	if err != nil {
		logging.Errorf("Error sending value to InfluxDb: %s", err)
		i.client = nil // Force reconnect.
	}
	return err
}

func buildPoint(m *pb.Metric) client.Point {
	return client.Point{
		Measurement: m.Name,
		Tags:        m.Labels,
		Fields: map[string]interface{}{
			"value": m.ValueAsFloat(),
		},
		Time: time.Unix(m.Ts, 0),
	}
}

func (i *InfluxDb) Run() {
	for m := range i.Metrics {
		i.sendToInfluxDb(buildPoint(m))
	}
}

func NewInfluxDb(metricChannel chan *pb.Metric, influxDbUrl string) *InfluxDb {
	return &InfluxDb{
		Metrics: metricChannel,
		Loc:     influxDbUrl,
	}
}
