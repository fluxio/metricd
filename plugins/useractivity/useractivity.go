package useractivity

import (
	"encoding/base64"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fluxio/logging"
	"github.com/fluxio/metricd/pb"
	"github.com/segmentio/analytics-go"
	"github.com/streamrail/concurrent-map"
	"github.com/xtgo/uuid"
)

var freq = 10 * time.Second
var activityDefinitionVer = 2

const analyticsMetricPrefix = "head_proxy.analytics."

// Hide the analytics.Client behind an interface to
// facilitate dependency injection.
type segmentClient interface {
	Track(*analytics.Track) error
}

type reporter struct {
	c        segmentClient
	ch       chan *pb.Metric
	outCh    chan *pb.Metric
	window   time.Duration
	sessions cmap.ConcurrentMap
	build    string
}

type session struct {
	start         time.Time
	lastAct       time.Time
	id            string
	activityCount int64
	activities    map[string]bool
	clients       map[string]bool
	projects      map[string]bool

	// This mutex synchronizes access to all fields.
	mut sync.Mutex
}

var analyticsContext = map[string]interface{}{
	"integrations": map[string]interface{}{
		"All": true,
	},
	"active": true,
	"ip":     0,
}

func (r *reporter) injectSegmentClient(c segmentClient) {
	r.c = c
}

func (r *reporter) submit(uid string, s session) {
	length := math.Floor(s.lastAct.Sub(s.start).Seconds())

	t := &analytics.Track{
		Event:   "Activity Session",
		UserId:  uid,
		Context: analyticsContext,
		Properties: map[string]interface{}{
			"Start":         s.start,
			"Duration":      length,
			"Ver":           activityDefinitionVer,
			"Build":         r.build,
			"SessionId":     s.id,
			"ActivityCount": s.activityCount,
			"Activities":    convertMapKeysToArray(s.activities),
			"Clients":       convertMapKeysToArray(s.clients),
			"Projects":      convertMapKeysToArray(s.projects),
		},
	}
	t.Message.Timestamp = s.lastAct.String()
	err := r.c.Track(t)

	if err != nil {
		logging.Errorf("Error submitting the session info to SegmentIO: %s", err)
	}
}

func (r *reporter) submitter() {
	// Set up the ticker.
	tick := time.NewTicker(freq)

	// Will execute every tick.
	for range tick.C {
		// When sessions come out of the window, we want to remove them from
		// the concurrent map with a call to sessions.Remove(). However, this
		// can lead to a deadlock if done while iterating over the map with
		// Iter(). So, we accumulate the keys to delete, and then remove them
		// after we've ranged over Iter().
		//
		// The deadlock depends on the internal implementation of
		// concurrent-map. Looking inside Iter(), cmap grabs the RLock and
		// then writes each item to the channel. As the channel is unbuffered,
		// each item has to be read from the channel before the shard's mutex
		// is unlocked.
		//
		// When cmap has > 2 keys in a shard badness happens: as the
		// the loop body must execute before the second item is read from the
		// channel, and the loop body contains a Remove call (which acquires the
		// shard's mutex), we deadlock.
		//
		tickTime := time.Now()
		toRemove := []string{}
		for t := range r.sessions.Iter() {
			func() {
				s := t.Val.(session)
				// Session ran out of the window?
				s.mut.Lock()
				defer s.mut.Unlock()
				if s.lastAct.Add(r.window).Before(tickTime) {
					toRemove = append(toRemove, t.Key)
				}
			}()
		}

		for _, key := range toRemove {
			func() {
				val, exists := r.sessions.Get(key)
				if !exists {
					logging.Errorf("Atempting to delete a nonexisting session for %s", key)
					return
				}
				s := val.(session)
				s.mut.Lock()
				defer s.mut.Unlock()
				if s.lastAct.Add(r.window).Before(tickTime) {
					r.sessions.Remove(key)
					r.submit(key, s)
				}
			}()
		}
	}
}

func getBase64UniqueId() string {
	uuid := uuid.NewRandom()
	out := base64.StdEncoding.EncodeToString(uuid[:])
	return out[:len(out)-2] // Removes the '=='.
}

func (r *reporter) record(m *pb.Metric) *pb.Metric {
	uid := m.Labels["id"]
	if uid == "" {
		logging.Info("Gotten a useractivity metric with no user id.")
		return m
	}
	// FIXME(mag): there is a low probability race condition here:
	// 1. The sesssion is just come out of the window.
	// 2. There is a new event for the same user, and we retrieve the session.
	// 3. Right after that, but before we do the `Set()`, submitter gets to it
	//    and removes it from the map.
	// 4. We reach the last line of this function, and set the function with
	//    the old startTime, thus creating a new session with a wrong startTime.
	// Unfortunately, to fix this, we need a capability to hold a lock on a given
	// key, and concurrent-map doesn't currently provide that. mag will try to
	// get that functionality in and will update the code.
	val, exists := r.sessions.Get(uid)
	now := time.Now()
	var s session
	if !exists {
		s = session{
			start:         now,
			lastAct:       now,
			id:            getBase64UniqueId(),
			activityCount: 0,
			activities:    map[string]bool{},
			clients:       map[string]bool{},
			projects:      map[string]bool{},
		}
	} else {
		s = val.(session)
	}

	// Note: At this point, it may have been removed from the map already, but we
	//       are going to ignore that possibility to avoid having to keep track of
	//       sent vs unsent.
	sid := func() string {
		s.mut.Lock()
		defer s.mut.Unlock()

		s.lastAct = now

		s.activityCount++
		s.activities[m.Labels["type"]] = true
		s.clients[m.Labels["clientType"]] = true

		if prjid, ok := m.Labels["prj"]; ok {
			s.projects[prjid] = true
		}

		return s.id

	}()

	r.sessions.Set(uid, s)

	m.Labels["sessionId"] = sid

	return m
}

func (r *reporter) outputMetric(m *pb.Metric) {
	// The receiver is the Mux, which has flood control in place,
	// hence there is no need to control for the channel buffer
	// overflow here.
	r.outCh <- m
}

func (*reporter) sel(m *pb.Metric) bool {
	return strings.HasPrefix(m.Name, analyticsMetricPrefix)
}

func (r *reporter) Run() {
	go r.submitter()

	for m := range r.ch {
		if r.sel(m) {
			outM := r.record(m)
			if outM != nil {
				r.outputMetric(outM)
			}
		} else {
			r.outputMetric(m)
		}
	}
}

// Creates a new instance of the plugin.
// The plugin consumes a set of events, generated by the platform, and
// aggregates them into activity sessions. An activity session is a set of
// events, that happen closer than a given duration apart.
//
//   metricChannel:  The channel for inbound metrics.
//   segmentioToken: The segmentio write token.
//   window:         A maximum duration between two events, that would still be
//                   considered a part of one session.
//   dockerBuild:    A string identifying the platform build.
//
func NewUserActivityReporter(
	metricChannel chan *pb.Metric,
	outputChannel chan *pb.Metric,
	segmentioToken string,
	window time.Duration,
	dockerBuild string,
) *reporter {

	c := analytics.New(segmentioToken)
	c.Client = http.Client{Timeout: time.Duration(30 * time.Second)}

	return &reporter{
		c,
		metricChannel,
		outputChannel,
		window,
		cmap.New(),
		dockerBuild,
	}
}

func convertMapKeysToArray(m map[string]bool) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
