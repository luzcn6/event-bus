package eventbus

import (
	"time"
)

// ReconnectionScheduler should return the time before the next reconnection
// should be made.
type ReconnectionScheduler interface {
	// Should return an error to indicate that the client should not reconnect
	NextReconnectBackoff() (time.Duration, error)
}

// ReconnectionPolicy returns a ReconnectionScheduler to be used when attempting
// to reconnect.
type ReconnectionPolicy interface {
	NewScheduler() ReconnectionScheduler
}
