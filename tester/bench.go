package main

import (
	"flag"
	"fmt"
	"github.com/fluxio/metricd"
	"math"
	"net/url"
	"sync"
	"time"
)

var initialSleep = flag.Duration("initialSleep", 1*time.Millisecond, "")
var speedupRate = flag.Float64("speedup_rate", 1.01, "")
var slowdownRate = flag.Float64("slowdown_rate", 0.95, "")
var metricd_loc = flag.String("metricd", "http://127.0.0.1:6699", "")
var updateInterval = flag.Duration("update_interval", 100*time.Millisecond, "")

func write(inst int) {
	url, _ := url.Parse(*metricd_loc)
	client, err := metricd.NewClient(url)
	if err != nil {
		panic(err)
	}

	s := *initialSleep
	c := 0
	m := &sync.Mutex{}

	tick := time.NewTicker(*updateInterval)

	go func() {
		for range tick.C {
			m.Lock()
			fmt.Printf("Metrics sent: %d\n", c)
			c = 0
			m.Unlock()
		}
	}()

	for i := 0; ; i++ {
		if err :=
			client.Report(
				fmt.Sprintf("test-%d", inst), metricd.LabelSet{}, i); err != nil {
			// Slow down.
			s = time.Duration(math.Floor(float64(s.Nanoseconds()) / *slowdownRate))
		} else {
			// Speed up.
			s = time.Duration(math.Floor(float64(s.Nanoseconds()) / *speedupRate))
			m.Lock()
			c++
			m.Unlock()
		}

		time.Sleep(s)
	}

}

func main() {
	flag.Parse()
	for i := 1; i < 2; i++ {
		go write(i)
	}

	select {}
}
