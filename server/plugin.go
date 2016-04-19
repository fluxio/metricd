package server

type Plugin interface {
	// Runs the main plugin loop. Should be only called once,
	// in a separate goroutine. Never returns.
	Run()
}
