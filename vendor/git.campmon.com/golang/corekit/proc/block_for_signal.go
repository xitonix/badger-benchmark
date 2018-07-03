package proc // import "git.campmon.com/golang/corekit/proc"

import (
	"os"
	"os/signal"
	"syscall"
)

// WaitForTermination blocks the caller until SIGINT, SIGKILL or SIGTERM has been captured
func WaitForTermination() os.Signal {
	return WaitForSignal(syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
}

// WaitForSignal blocks the caller until one of the specified OS signals has been captured
func WaitForSignal(signals ...os.Signal) os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, signals...)
	sig := <-ch
	return sig
}
