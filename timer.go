package metricd

import "time"

// Describes a timer metric, which allows measurement between two time points.
type Timer interface {
	// Starts a timer. Subsequent calls without calling `End()` or `Pause()`
	// in between are ignored. If the timer was `Paused()` before, the time
	// accumulation resumes from the point of that call.
	Start() Timer
	// Pauses the timer. Does nothing, unless the previous call was `Start()`.
	// Otherwises temporarily pauses the time accumulation, until `Start()` is
	// called again.
	Pause()
	// Stops the timer and submits to metricd the time difference between
	// the current moment and the moment `Start()` was called for the first time.
	// If `Start()` wasn't called after the previous `End()`, the invocation
	// is ignored.
	End() error

	// Adds the label in incoming map to the LabelSet.
	AddLabels(LabelSet)
}

type timer struct {
	startedTs   time.Time
	accumulated time.Duration
	name        string
	labels      LabelSet
	client      Client
}

func (t *timer) Start() Timer {
	if t.startedTs.IsZero() {
		t.startedTs = time.Now()
	}
	return t
}

func (t *timer) Pause() {
	if !t.startedTs.IsZero() {
		t.accumulated += time.Now().Sub(t.startedTs)
		t.startedTs = time.Time{}
	}
}

func (t *timer) End() error {
	if !t.startedTs.IsZero() || t.accumulated != 0 {
		t.Pause()
		runLenUs := t.accumulated.Nanoseconds() / 1e3
		t.accumulated = 0
		return t.client.ReportHistogram(t.name, t.labels, runLenUs)
	}
	return nil
}

func (t *timer) AddLabels(labs LabelSet) {
	for k, v := range labs {
		t.labels[k] = v
	}
}

// A mock that does nothing, for use in tests.
type NopTimer struct{}

func (t NopTimer) Start() Timer     { return &t }
func (NopTimer) Pause()             {}
func (NopTimer) End() error         { return nil }
func (NopTimer) AddLabels(LabelSet) {}
