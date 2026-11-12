package dispatcher

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/features/stats"
)

var userCounterReferences = struct {
	sync.Mutex
	refs map[stats.Counter]int
}{refs: make(map[stats.Counter]int)}

// WithCounterReferences serializes counter attachment with retirement and
// retains every attached counter until ctx ends. The callback runs while the
// registry lock is held so a newly registered counter cannot be retired before
// its connection reference is visible.
func WithCounterReferences(ctx context.Context, attach func() []stats.Counter) {
	userCounterReferences.Lock()
	counters := uniqueCounters(attach())
	if len(counters) == 0 {
		userCounterReferences.Unlock()
		return
	}
	for _, counter := range counters {
		userCounterReferences.refs[counter]++
	}
	context.AfterFunc(ctx, func() {
		userCounterReferences.Lock()
		for _, counter := range counters {
			if userCounterReferences.refs[counter] <= 1 {
				delete(userCounterReferences.refs, counter)
			} else {
				userCounterReferences.refs[counter]--
			}
		}
		userCounterReferences.Unlock()
	})
	userCounterReferences.Unlock()
}

// TryUnregisterUserCounters removes quiet user counters only when no wrapped
// link can still write to either counter.
func TryUnregisterUserCounters(manager stats.Manager, email string) bool {
	userCounterReferences.Lock()
	defer userCounterReferences.Unlock()

	upName := "user>>>" + email + ">>>traffic>>>uplink"
	downName := "user>>>" + email + ">>>traffic>>>downlink"
	up := manager.GetCounter(upName)
	down := manager.GetCounter(downName)
	for _, counter := range uniqueCounters([]stats.Counter{up, down}) {
		if userCounterReferences.refs[counter] != 0 || counter.Value() != 0 {
			return false
		}
	}
	return manager.UnregisterCounter(upName) == nil && manager.UnregisterCounter(downName) == nil
}

func uniqueCounters(counters []stats.Counter) []stats.Counter {
	unique := make([]stats.Counter, 0, len(counters))
	seen := make(map[stats.Counter]struct{}, len(counters))
	for _, counter := range counters {
		if counter == nil {
			continue
		}
		if _, exists := seen[counter]; exists {
			continue
		}
		seen[counter] = struct{}{}
		unique = append(unique, counter)
	}
	return unique
}
