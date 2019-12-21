package eventbus

import (
	"errors"
	"math"
	"time"
)

var (
	// DefaultPolicy is used by the Eventbus constructor.
	DefaultPolicy ReconnectionPolicy = NewExponentialReconnectionPolicy(1*time.Second, 32*time.Second)
	// ErrReconnectsExhausted is returned as an error when the reconnection policy
	// has run out of attempts.
	ErrReconnectsExhausted = errors.New("reconnects exhausted")
)

// ConstantReconnectionPolicy reconnects every duration forever.
type ConstantReconnectionPolicy struct {
	duration time.Duration
}

type constantReconnectionScheduler struct {
	duration time.Duration
}

func (s constantReconnectionScheduler) NextReconnectBackoff() (time.Duration, error) {
	return s.duration, nil
}

// NewScheduler implements the ReconnectionPolicy interface and returns a new
// constant reconnection scheduler.
func (p ConstantReconnectionPolicy) NewScheduler() ReconnectionScheduler {
	return constantReconnectionScheduler{p.duration}
}

// NewConstantReconnectionPolicy creates a new ConstantReconnectionPolicy with
// the specified duration.
func NewConstantReconnectionPolicy(t time.Duration) *ConstantReconnectionPolicy {
	return &ConstantReconnectionPolicy{t}
}

// ExponentialReconnectionPolicy reconnects with an exponential delay, up to
// maxDelay forever.
type ExponentialReconnectionPolicy struct {
	baseDelay time.Duration
	maxDelay  time.Duration
}

// NewScheduler implements the ReconnectionPolicy interface and returns a new
// exponential reconnection scheduler.
func (p ExponentialReconnectionPolicy) NewScheduler() ReconnectionScheduler {
	return &exponentialReconnectionScheduler{
		baseDelay: p.baseDelay,
		maxDelay:  p.maxDelay,
	}
}

// NewExponentialReconnectionPolicy creates a new ExponentialReconnectionPolicy with
// the base and max durations.
func NewExponentialReconnectionPolicy(base, max time.Duration) *ExponentialReconnectionPolicy {
	return &ExponentialReconnectionPolicy{base, max}
}

type exponentialReconnectionScheduler struct {
	attempts  int32
	baseDelay time.Duration
	maxDelay  time.Duration
}

func (s *exponentialReconnectionScheduler) NextReconnectBackoff() (time.Duration, error) {
	s.attempts++
	return time.Duration(math.Min(float64(calculateDelay(s.baseDelay, s.attempts)), float64(s.maxDelay))), nil
}

func calculateDelay(base time.Duration, attempts int32) time.Duration {
	return time.Duration(math.Pow(float64(2), float64(attempts-1))) * base
}

// LimitedReconnectionPolicy reconnects with an fixed delay for a limited number
// of attempts, and then returns ErrReconnectsExhausted.
type LimitedReconnectionPolicy struct {
	attempts int32
	delay    time.Duration
}

type limitedReconnectionScheduler struct {
	attempts int32
	delay    time.Duration
}

func (s *limitedReconnectionScheduler) NextReconnectBackoff() (time.Duration, error) {
	s.attempts--
	if s.attempts >= 0 {
		return s.delay, nil
	}
	return s.delay, ErrReconnectsExhausted
}

// NewScheduler implements the ReconnectionPolicy interface and returns a new
// limited reconnection scheduler.
func (p LimitedReconnectionPolicy) NewScheduler() ReconnectionScheduler {
	return &limitedReconnectionScheduler{p.attempts, p.delay}
}

// NewLimitedReconnectionPolicy creates a new LimitedReconnectionPolicy.
func NewLimitedReconnectionPolicy(attempts int32, t time.Duration) *LimitedReconnectionPolicy {
	return &LimitedReconnectionPolicy{attempts, t}
}

// NewScheduler implements the ReconnectionPolicy interface and returns a new
// limited exponential reconnection scheduler.
func (p LimitedExponentialReconnectionPolicy) NewScheduler() ReconnectionScheduler {
	var backoffs []time.Duration
	b := p.baseDelay
	for b <= p.maxDelay {
		backoffs = append(backoffs, b)
		b *= 2
	}
	return &limitedExponentialReconnectionScheduler{
		backoffs: backoffs,
	}
}

type limitedExponentialReconnectionScheduler struct {
	attempts int
	backoffs []time.Duration
}

func (s *limitedExponentialReconnectionScheduler) NextReconnectBackoff() (time.Duration, error) {
	s.attempts++
	if s.attempts > len(s.backoffs) {
		return s.backoffs[len(s.backoffs)-1], ErrReconnectsExhausted
	}
	return s.backoffs[s.attempts-1], nil
}

// LimitedExponentialReconnectionPolicy reconnects with an exponential backoff
// until the backoff is greater than the maximum delay.
type LimitedExponentialReconnectionPolicy struct {
	baseDelay time.Duration
	maxDelay  time.Duration
}

// NewLimitedExponentialReconnectionPolicy creates a new LimitedReconnectionPolicy.
func NewLimitedExponentialReconnectionPolicy(base, max time.Duration) *LimitedExponentialReconnectionPolicy {
	return &LimitedExponentialReconnectionPolicy{base, max}
}
