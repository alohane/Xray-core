package proxy

import (
	"context"
	"sync/atomic"
	"time"
)

// SpliceAction tells CopyRawConnIfExist what to do at a splice chunk boundary.
type SpliceAction uint8

const (
	SpliceContinue SpliceAction = iota
	SpliceFallback
	SpliceReject
)

// SpliceController is an optional per-connection hook. Implementations must be
// safe to call from the copy goroutine and should return quickly.
type SpliceController interface {
	CheckSplice() SpliceAction
	SpliceCheckInterval() time.Duration
	FallbackIdleTimeout() time.Duration
}

type spliceControllerContextKey struct{}

type spliceControllerState struct {
	controller   SpliceController
	lastActivity atomic.Int64
}

func newSpliceControllerState(controller SpliceController) *spliceControllerState {
	state := &spliceControllerState{controller: controller}
	state.lastActivity.Store(time.Now().UnixNano())
	return state
}

func (s *spliceControllerState) recordActivity(transferred int64, now time.Time) {
	if transferred > 0 {
		s.lastActivity.Store(now.UnixNano())
	}
}

func (s *spliceControllerState) idleFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, s.lastActivity.Load()))
}

// ContextWithSpliceController installs a controller checked between splice
// chunks. Core users that do not install one retain the upstream behavior.
func ContextWithSpliceController(ctx context.Context, controller SpliceController) context.Context {
	return context.WithValue(ctx, spliceControllerContextKey{}, newSpliceControllerState(controller))
}

func spliceControllerFromContext(ctx context.Context) *spliceControllerState {
	state, _ := ctx.Value(spliceControllerContextKey{}).(*spliceControllerState)
	return state
}
