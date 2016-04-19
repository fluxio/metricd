// Metricd is a server, that receives application level metrics
// and routes those to the appropriate plugins for further processing.
package main

import (
	"flag"

	"github.com/fluxio/metricd/pb"
	"github.com/fluxio/metricd/plugins"
	"github.com/fluxio/metricd/plugins/influxdb"
	"github.com/fluxio/metricd/server"

	"github.com/fluxio/logging"
)

var port = flag.Int("port", 6699, "Port to serve on.")
var graphiteUrl = flag.String(
	"graphite", "", "The <host>:<port> for the graphite server.")
var influxDbUrl = flag.String(
	"influxDb", "", "The <host>:<port> for the influxdb server.")
var logLevel = flag.String("loglevel", "info",
	"Log level to display. Valid values are [trace,debug,info,error]")

func main() {
	flag.Parse()
	logging.System =
		logging.NewTextLogger(nil, "metricd", logging.ParseLevelOrDie(*logLevel))

	metricChannel := make(chan *pb.Metric, server.MAIN_CHANNEL_BUFFER_SIZE)
	pluginChannel := make(chan *pb.Metric, server.MAIN_CHANNEL_BUFFER_SIZE)

	aggregatorOutput := pluginChannel

	aggregator := server.NewAggregator(metricChannel, aggregatorOutput)
	go aggregator.Run()

	mux := server.NewMux(pluginChannel)
	go mux.Run()

	var graphite server.Plugin
	if *graphiteUrl != "" {
		graphite = plugins.NewGraphite(mux.NewReceiver("graphite"), *graphiteUrl)
		go graphite.Run()
	}

	var influx server.Plugin
	if *influxDbUrl != "" {
		influx = influxdb.NewInfluxDb(mux.NewReceiver("influxdb"), *influxDbUrl)
		go influx.Run()
	}

	server.RunEndpoint(*port, metricChannel)
}
