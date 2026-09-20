package coordinator

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalContext returns a context that is cancelled on os.Interrupt or
// SIGTERM, in addition to any cancellation of parent.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-signals:
			signal.Stop(signals)
			cancel()
		case <-ctx.Done():
			signal.Stop(signals)
		}
	}()

	return ctx, cancel
}
