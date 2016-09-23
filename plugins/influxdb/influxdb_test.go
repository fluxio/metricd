package influxdb

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fluxio/metricd/pb"
)

func TestInfluxdbPlugin(t *testing.T) {
	// Smaller batching parameters for testing
	batchSize = 20
	batchTime = 500 * time.Millisecond
	numWorkers = 2

	// Ensure that the number of data points is not evenly divisible by our batch
	// size. We should receive all data points anyway.
	numPoints := 100*batchSize + 3

	// Accessed using the atomic package
	var countedPoints int32 = 0

	// Create a fake influxdb server that counts the number of data points received.
	// It signals on doneChan when countedPoints == numPoints.
	doneChan := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scanner := bufio.NewScanner(r.Body)
		numLines := 0
		for scanner.Scan() {
			numLines++
		}
		// InfluxDB line protocol has one data point per line
		atomic.AddInt32(&countedPoints, int32(numLines))
		if atomic.CompareAndSwapInt32(&countedPoints, int32(numPoints), 0) {
			doneChan <- struct{}{}
		}
	}))
	defer srv.Close()

	// Create a channel and fill it with data points
	metrics := make(chan *pb.Metric, numPoints)
	for i := 0; i < numPoints; i++ {
		m := &pb.Metric{
			Name: "testing123",
			Ts:   time.Now().UTC().UnixNano(),
			Value: &pb.Metric_DoubleValue{
				DoubleValue: 1234,
			},
		}
		metrics <- m
	}

	// Start and run our influxdb plugin.
	// TODO(avaskys): The plugin runs forever here. Come up with a way to stop it.
	plugin := NewInfluxDb(metrics, strings.TrimPrefix(srv.URL, "http://"))
	go plugin.Run()

	// We should get a signal on doneChan within a reasonable amount of time.
	// Theoretically it's impossible to put an upper bound on how long this will take
	// but realistically this is a reasonable timeout.
	timeout := batchTime * 5
	timer := time.NewTimer(timeout)
	select {
	case <-doneChan:
		t.Logf("Received %d data points as expected.", numPoints)
	case <-timer.C:
		t.Errorf("Did not receive %d data points within %s. Received %d.",
			numPoints, timeout.String(), atomic.LoadInt32(&countedPoints))
	}
}

func TestEmptyLabelFiltering(t *testing.T) {
	labels := make(map[string]string)
	labels["empty"] = ""
	labels["nonempty"] = "hello"
	labels[""] = "empty"
	labels["spaces"] = "with spaces"
	labels["check"] = ""
	m := &pb.Metric{
		Name: "testing123",
		Ts:   time.Now().UTC().UnixNano(),
		Value: &pb.Metric_DoubleValue{
			DoubleValue: 1234,
		},
		IndexedLabels: labels,
	}
	point := buildPoint(m)
	_, ok := point.Tags["empty"]
	if ok {
		t.Error("Empty tag should not be here")
	}
	_, ok = point.Tags["check"]
	if ok {
		t.Error("Check tag should not be here")
	}
	_, ok = point.Tags[""]
	if ok {
		t.Error("Blank tag should not be here")
	}
	_, ok = point.Tags["nonempty"]
	if !ok {
		t.Error("Non-empty tag should remain in point")
	}
	_, ok = point.Tags["spaces"]
	if !ok {
		t.Error("Tags with spaces should remain in point")
	}

}
