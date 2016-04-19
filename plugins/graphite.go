// The plugins package contains metricd plugins.
package plugins

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fluxio/metricd/pb"
	"github.com/fluxio/logging"

	"github.com/marpaia/graphite-golang"
)

// Unless a value is written to graphite every storage interval,
// Graphite will consider value for that interval null. We store
// the last value of every metric and flush it to graphite at
// the interval, equal to Graphite storage interval.
const flushInterval = 10 * time.Second

// Needed for a storage of the current metric values for flushing.
type value struct {
	val string
	ts  int64
}

// Graphite object represents a connection to a Graphite server,
// located at "host:port" supplied in Loc.
type Graphite struct {
	// A channel, which will receive metrics from the users.
	Metrics chan *pb.Metric
	// Location of the Graphite server.
	Loc string
	// Handler to the library client.
	graph *graphite.Graphite
	// Map of current values.
	currentValues map[string]value
	// Protects the currentValues
	currentValuesMu sync.Mutex
}

func parseHostPort(v string) (host string, port int, err error) {
	r := strings.Split(v, ":")
	if len(r) < 2 {
		return "", 0, fmt.Errorf("Failed to parse the <host>:<port> from %q", v)
	}
	port, err = strconv.Atoi(r[1])
	return r[0], port, err
}

func (g *Graphite) connect() error {
	logging.Infof("Initializing Graphite Client. Connecting to %s", g.Loc)
	host, port, err := parseHostPort(g.Loc)

	if host == "" || port <= 0 || err != nil {
		logging.Errorf("Illegal graphite location: %s", g.Loc)
		logging.Info("Using Nop graphite client.")
		g.graph = graphite.NewGraphiteNop(host, port)
		return nil
	}
	g.graph, err = graphite.NewGraphite(host, port)

	return err
}

func (g *Graphite) sendAllValues() error {
	g.currentValuesMu.Lock()
	defer g.currentValuesMu.Unlock()
	for name, val := range g.currentValues {
		// HACK: Send the metric with the current time.
		// Has to be done this way in order to fall into the latest
		// Graphite storage interval.
		err := g.graph.SendMetric(
			graphite.NewMetric(name, val.val, time.Now().UTC().Unix()))
		if err != nil {
			logging.Errorf("Error sending to Graphite: %s\n", err)
			return err
		}
	}
	return nil
}

func (g *Graphite) flusher() {
	if g.Loc == "" {
		return
	}

	for {
		disconnected := false
		reconnect_interval := 1 * time.Second
		for g.graph == nil {
			err := g.connect()
			if err != nil {
				if !disconnected {
					disconnected = true
					logging.Errorf("Error connecting to graphite: %s\n", err)
				}
				time.Sleep(reconnect_interval)

				// Exp backoff with a ceiling.
				reconnect_interval *= 2
				if reconnect_interval > 60*time.Second {
					reconnect_interval = 60 * time.Second
				}
			} else {
				if disconnected {
					disconnected = false
					logging.Error("Graphite Connection Error Resolved.")
				}
			}
		}

		flushStart := time.Now()
		err := g.sendAllValues()
		logging.DebugTrackTimef(flushStart, "graphite flush.")

		if err != nil {
			g.graph = nil
			continue
		}
		// Make sure we obey the flushInterval.
		time.Sleep(flushStart.Add(flushInterval).Sub(time.Now()))
	}
}

func (g *Graphite) sendToGraphite(var_name string, val string, ts int64) {
	g.currentValuesMu.Lock()
	defer g.currentValuesMu.Unlock()
	g.currentValues[var_name] = value{val, ts}
}

// Runs the submission of the metrics, received on Graphite.Metrics to the
// Grapite server, located at host:port, supplied in Graphite.Loc.
func (g *Graphite) Run() {
	go g.flusher()

	for m := range g.Metrics {
		g.sendToGraphite(buildGraphiteName(m), m.ValueAsString(), m.Ts)
	}
}

var illegal = regexp.MustCompile("[/ !+?.,;]")

func replaceDisallowedChars(s string) string {
	return illegal.ReplaceAllString(s, "_")
}

func buildGraphiteName(m *pb.Metric) string {
	res := m.Name

	var keys []string
	for k := range m.Labels {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	for _, k := range keys {
		res += "." + k + "." + replaceDisallowedChars(m.Labels[k])
	}

	return res
}

func NewGraphite(metricChannel chan *pb.Metric, graphiteUrl string) *Graphite {
	return &Graphite{
		Metrics:       metricChannel,
		Loc:           graphiteUrl,
		currentValues: make(map[string]value)}
}
