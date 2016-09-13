package metricd

import "time"

// Mock out the ticker for testing
var newTicker = time.NewTicker

type PeriodicReporter interface {
	// Permanently stops the periodic reporter
	Stop()
}

type periodicReporter struct {
	ticker *time.Ticker
	done   chan struct{}
}

func newPeriodicReporter(
	cli Client,
	name string,
	interval time.Duration,
	labels LabelSet,
	valueGetter func() interface{},
) (PeriodicReporter, error) {
	ticker := newTicker(interval)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <- ticker.C:
				val := valueGetter()
				cli.Report(name, labels, val)
			case <- done:
				return
			}
		}
	}()

	pr := &periodicReporter{
		ticker: ticker,
		done: done,
	}
	return pr, nil
}

func (pr *periodicReporter) Stop() {
	pr.ticker.Stop()
	close(pr.done)
}

type nopPeriodicReporter struct{}

func (npr nopPeriodicReporter) Stop() {}
