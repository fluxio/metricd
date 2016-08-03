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
// given port after sleeping for sleepDur seconds. The listener will be passed
// to listenerChan, or listenerChan will be closed if there was an error.
func runSlowListeningServer(
	t *testing.T,
	port int,
	listenerChan chan net.Listener,
	sleepDur time.Duration,
) {
	time.Sleep(sleepDur * time.Second)
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
	go runSlowListeningServer(t, port, listenerChan, 3)
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

func TestClientWithAddressGetter(t *testing.T) {
	const port1, port2 = 7878, 8787

	currentServer := 0
	// addrGetter is a function that will tell metricd client where the server
	// lives. We first return nil, then server with port1, then server with port2.
	addrGetter := func() *url.URL {
		var port int
		switch currentServer {
		case 0:
			return nil
		case 1:
			port = port1
		case 2:
			port = port2
		}
		url := url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("localhost:%d", port),
		}
		return &url
	}
	// Create a client with adress getting function.
	client, err := NewClientWithAddrGetter(addrGetter)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Try connecting to nil server.
	rc := client.(*realClient)
	err = rc.tryConnect()
	if err == nil {
		t.Fatal("Successfuly connected to nil server?!")
	}

	// Create a server on port1 and start it right away.
	listenerChan := make(chan net.Listener, 1)
	go runSlowListeningServer(t, port1, listenerChan, 0)
	// Let addrGetter know that the server has moved.
	currentServer++

	// Now try connecting to real server on port1.
	err = rc.tryConnect()
	if err != nil {
		t.Fatal(err)
	}

	// Close original server.
	l, success := <-listenerChan
	if success {
		l.Close()
	} else {
		t.Fatal("Failed to close original server.")
	}

	// Create a new server on different port and start it after a second.
	listenerChan = make(chan net.Listener, 1)
	go runSlowListeningServer(t, port2, listenerChan, 1)
	defer func() {
		l, success := <-listenerChan
		if success {
			l.Close()
		}
	}()
	// Let addrGetter know that the server has moved.
	currentServer++

	// Verify that we can reconnect to a migrated server.
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
