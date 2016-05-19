package useractivity

import (
	"testing"
	"time"

	"github.com/fluxio/metricd"
	"github.com/fluxio/metricd/pb"

	"github.com/segmentio/analytics-go"
	. "github.com/smartystreets/goconvey/convey"
)

const dockerBuild = "123"

type fakeSegmentClient struct {
	ch chan *analytics.Track
}

func (c *fakeSegmentClient) Track(t *analytics.Track) error {
	c.ch <- t
	return nil
}

// Using individual tests here, because I want to make sure no
// internal state of the plugin will effect the following test.

func TestSessionId(t *testing.T) {
	in := make(chan *pb.Metric)
	out := make(chan *pb.Metric)
	r := NewUserActivityReporter(in, out, "", time.Duration(1), dockerBuild)
	// Can't block on this one, as the call is inside the eval loop.
	segmentCh := make(chan *analytics.Track, 10)
	r.injectSegmentClient(&fakeSegmentClient{segmentCh})

	freq = 1 * time.Millisecond // Make internal candence very short.

	go r.Run()

	Convey("A session id present", t, func() {
		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "whateva",
			Labels: metricd.LabelSet{"id": "1"},
		}
		nm := <-out

		So(nm.Name, ShouldEqual, analyticsMetricPrefix+"whateva")
		So(nm.Labels["id"], ShouldEqual, "1")
		So(nm.Labels["sessionId"], ShouldNotBeNil)
	})

	close(in)
}

func TestActivtiesAndClients(t *testing.T) {
	in := make(chan *pb.Metric)
	out := make(chan *pb.Metric)
	r := NewUserActivityReporter(in, out, "", 10*time.Millisecond, dockerBuild)
	// Can't block on this one, as the call is inside the eval loop.
	segmentCh := make(chan *analytics.Track, 10)
	r.injectSegmentClient(&fakeSegmentClient{segmentCh})

	freq = 1 * time.Millisecond // Make internal candence very short.

	go r.Run()

	Convey("Activities are tracked properly", t, func() {
		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "test1",
			Labels: metricd.LabelSet{"id": "1", "type": "test1", "clientType": "web"},
		}

		session1metric1 := <-out
		val, exists := r.sessions.Get(session1metric1.Labels["id"])

		So(exists, ShouldBeTrue)
		So(val.(session).activities, ShouldResemble, map[string]bool{"test1": true})
		So(val.(session).clients, ShouldResemble, map[string]bool{"web": true})

		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "test2",
			Labels: metricd.LabelSet{"id": "1", "type": "test2", "clientType": "web"},
		}

		session1metric2 := <-out

		val, exists = r.sessions.Get(session1metric2.Labels["id"])

		So(exists, ShouldBeTrue)
		So(val.(session).activities, ShouldResemble, map[string]bool{"test1": true, "test2": true})
		So(val.(session).clients, ShouldResemble, map[string]bool{"web": true})

		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "test3",
			Labels: metricd.LabelSet{"id": "1", "type": "test3", "clientType": "web"},
		}

		session1metric3 := <-out
		val, exists = r.sessions.Get(session1metric3.Labels["id"])

		So(exists, ShouldBeTrue)
		So(val.(session).activities, ShouldResemble, map[string]bool{"test1": true, "test2": true, "test3": true})
		So(val.(session).clients, ShouldResemble, map[string]bool{"web": true})

		<-segmentCh
	})

	Convey("Clients are tracked properly", t, func() {
		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "test1",
			Labels: metricd.LabelSet{"id": "1", "type": "test1", "clientType": "web"},
		}
		session2metric1 := <-out
		val, exists := r.sessions.Get(session2metric1.Labels["id"])

		So(exists, ShouldBeTrue)
		So(val.(session).activities, ShouldResemble, map[string]bool{"test1": true})
		So(val.(session).clients, ShouldResemble, map[string]bool{"web": true})

		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "test1",
			Labels: metricd.LabelSet{"id": "1", "type": "test1", "clientType": "revit"},
		}
		session2metric2 := <-out
		val, exists = r.sessions.Get(session2metric2.Labels["id"])

		So(exists, ShouldBeTrue)
		So(val.(session).activities, ShouldResemble, map[string]bool{"test1": true})
		So(val.(session).clients, ShouldResemble, map[string]bool{"web": true, "revit": true})

		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "test2",
			Labels: metricd.LabelSet{"id": "1", "type": "test2", "clientType": "grasshopper"},
		}
		session2metric3 := <-out
		val, exists = r.sessions.Get(session2metric3.Labels["id"])

		So(exists, ShouldBeTrue)
		So(val.(session).activities, ShouldResemble, map[string]bool{"test1": true, "test2": true})
		So(val.(session).clients, ShouldResemble, map[string]bool{"web": true, "revit": true, "grasshopper": true})

		<-segmentCh
	})

	close(in)
}

func TestSessionCompletion(t *testing.T) {
	in := make(chan *pb.Metric)
	out := make(chan *pb.Metric)
	r := NewUserActivityReporter(in, out, "", 10*time.Millisecond, dockerBuild)
	// Can't block on this one, as the call is inside the eval loop.
	segmentCh := make(chan *analytics.Track, 10)
	r.injectSegmentClient(&fakeSegmentClient{segmentCh})

	freq = 1 * time.Millisecond // Make internal candence very short.

	go r.Run()

	Convey("Session completes", t, func() {
		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "whateva",
			Labels: metricd.LabelSet{"id": "1"},
		}
		session1metric1 := <-out

		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "whateva",
			Labels: metricd.LabelSet{"id": "1"},
		}
		session1metric2 := <-out

		So(session1metric1.Labels["sessionId"],
			ShouldEqual,
			session1metric2.Labels["sessionId"])

		// Wait for the session to elapse.
		track1 := <-segmentCh
		So(track1.Properties["SessionId"],
			ShouldEqual,
			session1metric2.Labels["sessionId"])

		in <- &pb.Metric{
			Name:   analyticsMetricPrefix + "whateva",
			Labels: metricd.LabelSet{"id": "1"},
		}
		session2metric1 := <-out

		So(session1metric1.Labels["sessionId"],
			ShouldNotEqual,
			session2metric1.Labels["sessionId"])

		// Wait for the second session to elapse.
		track2 := <-segmentCh
		So(track1.Properties["SessionId"],
			ShouldNotEqual,
			track2.Properties["SessionId"])
	})

	close(in)
}
