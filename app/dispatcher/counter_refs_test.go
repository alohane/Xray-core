package dispatcher_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	policyapp "github.com/xtls/xray-core/app/policy"
	statsapp "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
)

func TestWrapLinkRetainsUserCountersUntilContextEnds(t *testing.T) {
	statsManager, err := statsapp.NewManager(context.Background(), &statsapp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	policyManager, err := policyapp.New(context.Background(), &policyapp.Config{
		Level: map[uint32]*policyapp.Policy{0: {Stats: &policyapp.Policy_Stats{UserUplink: true, UserDownlink: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	const email = "test|user@example.invalid|7"
	ctx, cancel := context.WithCancel(session.ContextWithInbound(context.Background(), &session.Inbound{
		User: &protocol.MemoryUser{Email: email},
	}))
	dispatcher.WrapLink(ctx, policyManager, statsManager, &transport.Link{
		Reader: buf.NewReader(strings.NewReader("payload")),
		Writer: buf.Discard,
	})
	if dispatcher.TryUnregisterUserCounters(statsManager, email) {
		t.Fatal("unregistered counters while core WrapLink still referenced them")
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for !dispatcher.TryUnregisterUserCounters(statsManager, email) {
		if time.Now().After(deadline) {
			t.Fatal("counter references were not released after context cancellation")
		}
		time.Sleep(time.Millisecond)
	}
}
