package proxy

import (
	"context"
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

// ContextWithSpliceController installs a controller checked between splice
// chunks. Core users that do not install one retain the upstream behavior.
func ContextWithSpliceController(ctx context.Context, controller SpliceController) context.Context {
	return context.WithValue(ctx, spliceControllerContextKey{}, controller)
}

func spliceControllerFromContext(ctx context.Context) SpliceController {
	controller, _ := ctx.Value(spliceControllerContextKey{}).(SpliceController)
	return controller
}
