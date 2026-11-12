//go:build linux

package proxy_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/proxy"
)

const spliceTestChunk = int64(32 * 1024 * 1024)

type countingCounter struct {
	value atomic.Int64
	calls atomic.Int64
}

func (c *countingCounter) Value() int64 { return c.value.Load() }
func (c *countingCounter) Set(v int64) int64 {
	previous := c.value.Swap(v)
	return previous
}
func (c *countingCounter) Add(v int64) int64 {
	previous := c.value.Add(v) - v
	c.calls.Add(1)
	return previous
}

type spliceTestController struct {
	action   proxy.SpliceAction
	interval time.Duration
	idle     time.Duration
	checks   atomic.Int64
}

func (c *spliceTestController) CheckSplice() proxy.SpliceAction {
	c.checks.Add(1)
	return c.action
}
func (c *spliceTestController) SpliceCheckInterval() time.Duration { return c.interval }
func (c *spliceTestController) FallbackIdleTimeout() time.Duration { return c.idle }

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestSpliceCreditsEveryChunk(t *testing.T) {
	const payloadSize = int64(100 * 1024 * 1024)
	counter, controller, err := runSpliceTransfer(payloadSize, proxy.SpliceContinue)
	if err != nil {
		t.Fatal(err)
	}
	if got := counter.Value(); got != payloadSize {
		t.Fatalf("traffic counter = %d, want %d", got, payloadSize)
	}
	if got := counter.calls.Load(); got != 4 {
		t.Fatalf("counter Add calls = %d, want 4 chunk credits", got)
	}
	if got := controller.checks.Load(); got != 3 {
		t.Fatalf("controller checks = %d, want 3 complete chunk boundaries", got)
	}
}

func TestSpliceFallbackCreditsBufferedTailAndRestoresTimers(t *testing.T) {
	const payloadSize = spliceTestChunk + 1024*1024
	counter, controller, err := runSpliceTransfer(payloadSize, proxy.SpliceFallback)
	if err != nil {
		t.Fatal(err)
	}
	if got := counter.Value(); got != payloadSize {
		t.Fatalf("traffic counter = %d, want %d", got, payloadSize)
	}
	if got := controller.checks.Load(); got != 1 {
		t.Fatalf("controller checks = %d, want 1", got)
	}
}

func TestSpliceRejectStopsAtCheckpoint(t *testing.T) {
	sourceClient, sourceServer := tcpPair(t)
	sinkClient, sinkServer := tcpPair(t)
	defer sourceClient.Close()
	defer sourceServer.Close()
	defer sinkClient.Close()
	defer sinkServer.Close()

	counter := new(countingCounter)
	controller := &spliceTestController{action: proxy.SpliceReject, interval: time.Minute, idle: time.Minute}
	ctx := spliceContext(controller)
	timer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer timer.SetTimeout(0)
	writer := &dispatcher.SizeStatWriter{Counter: counter, Writer: buf.NewWriter(sinkServer)}
	copyResult := make(chan error, 1)
	go func() { copyResult <- proxy.CopyRawConnIfExist(ctx, sourceServer, sinkServer, writer, timer, nil) }()
	writeResult := make(chan error, 1)
	go func() {
		_, err := io.CopyN(sourceClient, zeroReader{}, spliceTestChunk+1024*1024)
		writeResult <- err
	}()
	readResult := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, sinkClient)
		readResult <- n
	}()

	err := <-copyResult
	_ = sourceServer.Close()
	_ = sinkServer.Close()
	_ = sourceClient.Close()
	_ = sinkClient.Close()
	<-writeResult
	<-readResult
	if err == nil || !strings.Contains(err.Error(), "rejected at checkpoint") {
		t.Fatalf("copy error = %v, want checkpoint rejection", err)
	}
	if got := counter.Value(); got != spliceTestChunk {
		t.Fatalf("traffic counter = %d, want one chunk %d", got, spliceTestChunk)
	}
	if got := counter.calls.Load(); got != 1 {
		t.Fatalf("counter Add calls = %d, want 1", got)
	}
}

func TestSpliceTimeoutCreatesCheckpointAndRestoresTimers(t *testing.T) {
	sourceClient, sourceServer := tcpPair(t)
	sinkClient, sinkServer := tcpPair(t)
	defer sourceClient.Close()
	defer sourceServer.Close()
	defer sinkClient.Close()
	defer sinkServer.Close()

	const interval = 50 * time.Millisecond
	controller := &spliceTestController{action: proxy.SpliceFallback, interval: interval, idle: interval}
	ctx := spliceContext(controller)
	mainFired := make(chan struct{}, 1)
	inFired := make(chan struct{}, 1)
	timer := signal.CancelAfterInactivity(ctx, func() {
		mainFired <- struct{}{}
		_ = sourceServer.Close()
	}, time.Minute)
	inTimer := signal.CancelAfterInactivity(ctx, func() { inFired <- struct{}{} }, time.Minute)
	defer timer.SetTimeout(0)
	defer inTimer.SetTimeout(0)
	counter := new(countingCounter)
	writer := &dispatcher.SizeStatWriter{Counter: counter, Writer: buf.NewWriter(sinkServer)}
	copyResult := make(chan error, 1)
	started := time.Now()
	go func() { copyResult <- proxy.CopyRawConnIfExist(ctx, sourceServer, sinkServer, writer, timer, inTimer) }()

	select {
	case <-mainFired:
	case <-time.After(time.Second):
		t.Fatal("main timer was not restored after timeout fallback")
	}
	select {
	case <-inFired:
	case <-time.After(time.Second):
		t.Fatal("inbound timer was not restored after timeout fallback")
	}
	select {
	case <-copyResult:
	case <-time.After(time.Second):
		t.Fatal("copy did not stop after the restored timer closed its source")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("time checkpoint took %v, want under one second", elapsed)
	}
	if got := controller.checks.Load(); got != 1 {
		t.Fatalf("controller checks = %d, want timeout checkpoint", got)
	}
	if got := counter.Value(); got != 0 {
		t.Fatalf("traffic counter = %d, want zero for idle checkpoint", got)
	}
}

func TestSpliceTimeoutCreditsPartialTrafficBeforeFallback(t *testing.T) {
	sourceClient, sourceServer := tcpPair(t)
	sinkClient, sinkServer := tcpPair(t)
	defer sourceClient.Close()
	defer sourceServer.Close()
	defer sinkClient.Close()
	defer sinkServer.Close()

	const (
		interval    = 50 * time.Millisecond
		payloadSize = int64(1024 * 1024)
	)
	controller := &spliceTestController{action: proxy.SpliceFallback, interval: interval, idle: time.Second}
	ctx := spliceContext(controller)
	timer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer timer.SetTimeout(0)
	counter := new(countingCounter)
	writer := &dispatcher.SizeStatWriter{Counter: counter, Writer: buf.NewWriter(sinkServer)}
	copyResult := make(chan error, 1)
	go func() { copyResult <- proxy.CopyRawConnIfExist(ctx, sourceServer, sinkServer, writer, timer, nil) }()

	if _, err := io.CopyN(sourceClient, zeroReader{}, payloadSize); err != nil {
		t.Fatal(err)
	}
	readResult := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, sinkClient, payloadSize)
		readResult <- err
	}()
	select {
	case err := <-readResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("partial payload did not reach the sink")
	}
	deadline := time.Now().Add(time.Second)
	for controller.checks.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := controller.checks.Load(); got != 1 {
		t.Fatalf("controller checks = %d, want one timeout checkpoint", got)
	}
	if got := counter.Value(); got != payloadSize {
		t.Fatalf("traffic counter = %d, want partial payload %d", got, payloadSize)
	}
	if got := counter.calls.Load(); got != 1 {
		t.Fatalf("counter Add calls = %d, want one timeout credit", got)
	}

	if tcp, ok := sourceClient.(*net.TCPConn); ok {
		if err := tcp.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-copyResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("buffered fallback did not finish after source EOF")
	}
}

func TestSpliceIdleClosesAtPolicyTimeout(t *testing.T) {
	sourceClient, sourceServer := tcpPair(t)
	sinkClient, sinkServer := tcpPair(t)
	defer sourceClient.Close()
	defer sourceServer.Close()
	defer sinkClient.Close()
	defer sinkServer.Close()

	const (
		interval = 25 * time.Millisecond
		idle     = 75 * time.Millisecond
	)
	controller := &spliceTestController{action: proxy.SpliceContinue, interval: interval, idle: idle}
	ctx := spliceContext(controller)
	timer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer timer.SetTimeout(0)
	writer := &dispatcher.SizeStatWriter{Counter: new(countingCounter), Writer: buf.NewWriter(sinkServer)}
	copyResult := make(chan error, 1)
	started := time.Now()
	go func() { copyResult <- proxy.CopyRawConnIfExist(ctx, sourceServer, sinkServer, writer, timer, nil) }()

	select {
	case err := <-copyResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle splice did not close at the policy timeout")
	}
	if elapsed := time.Since(started); elapsed < idle || elapsed > time.Second {
		t.Fatalf("idle splice lifetime = %v, want %v..1s", elapsed, idle)
	}
}

func TestSpliceIdleUsesActivityFromBothDirections(t *testing.T) {
	const (
		interval = 20 * time.Millisecond
		idle     = 120 * time.Millisecond
	)
	controller := &spliceTestController{action: proxy.SpliceContinue, interval: interval, idle: idle}
	ctx := spliceContext(controller)

	idleSourceClient, idleSourceServer := tcpPair(t)
	idleSinkClient, idleSinkServer := tcpPair(t)
	defer idleSourceClient.Close()
	defer idleSourceServer.Close()
	defer idleSinkClient.Close()
	defer idleSinkServer.Close()
	idleTimer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer idleTimer.SetTimeout(0)
	idleResult := make(chan error, 1)
	go func() {
		writer := &dispatcher.SizeStatWriter{Counter: new(countingCounter), Writer: buf.NewWriter(idleSinkServer)}
		idleResult <- proxy.CopyRawConnIfExist(ctx, idleSourceServer, idleSinkServer, writer, idleTimer, nil)
	}()

	time.Sleep(70 * time.Millisecond)
	activeSourceClient, activeSourceServer := tcpPair(t)
	activeSinkClient, activeSinkServer := tcpPair(t)
	defer activeSourceClient.Close()
	defer activeSourceServer.Close()
	defer activeSinkClient.Close()
	defer activeSinkServer.Close()
	activeTimer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer activeTimer.SetTimeout(0)
	activeResult := make(chan error, 1)
	go func() {
		writer := &dispatcher.SizeStatWriter{Counter: new(countingCounter), Writer: buf.NewWriter(activeSinkServer)}
		activeResult <- proxy.CopyRawConnIfExist(ctx, activeSourceServer, activeSinkServer, writer, activeTimer, nil)
	}()
	payload := []byte("activity from the other splice direction")
	if _, err := activeSourceClient.Write(payload); err != nil {
		t.Fatal(err)
	}
	if tcp, ok := activeSourceClient.(*net.TCPConn); ok {
		if err := tcp.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(activeSinkClient, received); err != nil {
		t.Fatal(err)
	}
	if err := <-activeResult; err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-idleResult:
		t.Fatalf("opposite-direction activity did not extend idle lifetime: %v", err)
	case <-time.After(70 * time.Millisecond):
	}
	select {
	case err := <-idleResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle splice did not close after shared activity stopped")
	}
}

func runSpliceTransfer(payloadSize int64, action proxy.SpliceAction) (*countingCounter, *spliceTestController, error) {
	sourceClient, sourceServer, err := tcpConnectionPair()
	if err != nil {
		return nil, nil, err
	}
	defer sourceClient.Close()
	defer sourceServer.Close()
	sinkClient, sinkServer, err := tcpConnectionPair()
	if err != nil {
		return nil, nil, err
	}
	defer sinkClient.Close()
	defer sinkServer.Close()

	controller := &spliceTestController{action: action, interval: time.Minute, idle: 50 * time.Millisecond}
	ctx := spliceContext(controller)
	timer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	inTimer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer timer.SetTimeout(0)
	defer inTimer.SetTimeout(0)
	counter := new(countingCounter)
	writer := &dispatcher.SizeStatWriter{Counter: counter, Writer: buf.NewWriter(sinkServer)}
	copyResult := make(chan error, 1)
	go func() { copyResult <- proxy.CopyRawConnIfExist(ctx, sourceServer, sinkServer, writer, timer, inTimer) }()
	writeResult := make(chan error, 1)
	go func() {
		_, writeErr := io.CopyN(sourceClient, zeroReader{}, payloadSize)
		if tcp, ok := sourceClient.(*net.TCPConn); ok {
			writeErr = joinError(writeErr, tcp.CloseWrite())
		}
		writeResult <- writeErr
	}()
	readResult := make(chan error, 1)
	go func() {
		n, readErr := io.CopyN(io.Discard, sinkClient, payloadSize)
		if n != payloadSize && readErr == nil {
			readErr = io.ErrUnexpectedEOF
		}
		readResult <- readErr
	}()
	return counter, controller, joinError(<-writeResult, <-readResult, <-copyResult)
}

func spliceContext(controller proxy.SpliceController) context.Context {
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{CanSpliceCopy: 1})
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{CanSpliceCopy: 1}})
	return proxy.ContextWithSpliceController(ctx, controller)
}

func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	client, server, err := tcpConnectionPair()
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func tcpConnectionPair() (net.Conn, net.Conn, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	defer listener.Close()
	type result struct {
		conn net.Conn
		err  error
	}
	accepted := make(chan result, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		accepted <- result{conn: conn, err: acceptErr}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		return nil, nil, err
	}
	server := <-accepted
	if server.err != nil {
		client.Close()
		return nil, nil, server.err
	}
	return client, server.conn, nil
}

func joinError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
