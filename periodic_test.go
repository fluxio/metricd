package metricd

import (
	"reflect"
	"testing"
	"time"
)

type mockClient struct {
	nopClient
	lastName   string
	lastLabels LabelSet
	lastValue  interface{}
	reportChan chan struct{}
}

func (mc *mockClient) Report(name string, labels LabelSet, value interface{}) error {
	mc.lastName = name
	mc.lastLabels = labels
	mc.lastValue = value
	mc.reportChan <- struct{}{}
	return nil
}

func TestPeriodicReporter(t *testing.T) {
	const metricName = "testMetric"
	labels := LabelSet{
		"label1": "one",
		"label2": "two",
	}

	// Override our ticker
	tick := make(chan time.Time)
	newTicker = func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: tick}
	}
	defer func() {
		newTicker = time.NewTicker
	}()

	// Simple value-getting function
	val := 0
	getVal := func() interface{} {
		val++
		return val
	}

	// Create a periodic reporter
	reportChan := make(chan struct{})
	cli := mockClient{reportChan: reportChan}
	newPeriodicReporter(&cli, metricName, time.Second, /* Doesn't matter, we control the clock */
		labels, getVal)

	// Let's tick through a few times
	for i := 0; i < 10; i++ {
		tick <- time.Now()

		// Wait for the data point to be reported
		timeout := time.NewTimer(5 * time.Second)
		select {
		case <-timeout.C:
			t.Fatal("Timed out waiting for metric to be reported")
		case <-reportChan:
			// Yay, we got a data point
		}

		if !reflect.DeepEqual(cli.lastValue, i+1) {
			t.Errorf("Expected value of %d, got %v", i+1, cli.lastValue)
		}
		if !reflect.DeepEqual(cli.lastLabels, labels) {
			t.Errorf("Expected labels of %v, got %v", labels, cli.lastLabels)
		}
		if metricName != cli.lastName {
			t.Errorf("Expected metric name of %s, got %s", metricName, cli.lastName)
		}
	}
}
