package metricd

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// Should be run in a separate goroutine. Runs a grpc server at the
// given port after sleeping for 3 seconds. The listener will be passed
// to listenerChan, or listenerChan will be closed if there was an error.
func runSlowListeningServer(t *testing.T, port int, listenerChan chan net.Listener) {
	time.Sleep(3 * time.Second)
	server := grpc.NewServer()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println(err)
		close(listenerChan)
		return
	}
	// Let the main goroutine know about the listener so they can close it
	listenerChan <- l
	server.Serve(l)
}

func TestClientConnect(t *testing.T) {
	// Set up a dummy grpc server that takes a few seconds to
	// start listening
	const port = 6789
	listenerChan := make(chan net.Listener, 1)
	go runSlowListeningServer(t, port, listenerChan)
	defer func() {
		l, success := <-listenerChan
		if success {
			l.Close()
		}
	}()

	// Create a client
	url := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("localhost:%d", port),
	}
	client, err := NewClient(&url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Try connecting
	rc := client.(*realClient)
	err = rc.tryConnect()
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: Technically this is racy, as the connection could have left the
	// Ready state by the time we get here. But given that we're looking at this
	// directly after connecting and we're on the loopback interface, this should
	// be okay in practice.
	if rc.conn.State() != grpc.Ready {
		t.Errorf("Unexpected grpc state. Expected READY, got %s", rc.conn.State())
	}
}

