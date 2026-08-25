package yamux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type contractWriteResult struct {
	n   int
	err error
}

func contractConfig() *Config {
	cfg := DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.ConnectionWriteTimeout = 500 * time.Millisecond
	cfg.StreamOpenTimeout = 2 * time.Second
	cfg.StreamCloseTimeout = 2 * time.Second
	cfg.LogOutput = io.Discard
	cfg.Logger = nil
	return cfg
}

func contractSessions(t *testing.T) (*Session, *Session) {
	t.Helper()
	left, right := net.Pipe()
	client, err := Client(left, contractConfig())
	if err != nil {
		t.Fatalf("create client session: %v", err)
	}
	server, err := Server(right, contractConfig())
	if err != nil {
		_ = client.Close()
		t.Fatalf("create server session: %v", err)
	}
	t.Cleanup(func() {
		done := make(chan struct{}, 2)
		go func() { _ = client.Close(); done <- struct{}{} }()
		go func() { _ = server.Close(); done <- struct{}{} }()
		for i := 0; i < 2; i++ {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				return
			}
		}
	})
	return client, server
}

func contractStreamPair(t *testing.T) (*Session, *Session, *Stream, *Stream) {
	t.Helper()
	client, server := contractSessions(t)
	clientStream, err := client.OpenStream()
	if err != nil {
		t.Fatalf("open client stream: %v", err)
	}
	serverStream, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("accept server stream: %v", err)
	}
	return client, server, clientStream, serverStream
}

func contractReadAll(t *testing.T, stream *Stream, size int) []byte {
	t.Helper()
	var out bytes.Buffer
	buf := make([]byte, size)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			_, _ = out.Write(buf[:n])
		}
		if err == io.EOF {
			return out.Bytes()
		}
		if err != nil {
			t.Fatalf("read payload after %d bytes: %v", out.Len(), err)
		}
		if n == 0 {
			t.Fatal("zero-byte read without EOF")
		}
	}
}

func TestContractHalfClosePreservesPeerTail(t *testing.T) {
	_, _, clientStream, serverStream := contractStreamPair(t)

	if err := serverStream.Close(); err != nil {
		t.Fatalf("half-close server writes: %v", err)
	}

	payload := make([]byte, 9001)
	_, _ = rand.New(rand.NewSource(0x59616d75)).Read(payload)
	if n, err := clientStream.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write peer tail: n=%d err=%v", n, err)
	}
	if err := clientStream.Close(); err != nil {
		t.Fatalf("close peer stream: %v", err)
	}

	got := contractReadAll(t, serverStream, 37)
	if !bytes.Equal(got, payload) {
		t.Fatalf("half-close tail mismatch: got=%d want=%d", len(got), len(payload))
	}
}

func TestContractFinalWindowSurvivesPeerSessionClose(t *testing.T) {
	client, _, clientStream, serverStream := contractStreamPair(t)

	payload := make([]byte, int(initialStreamWindow))
	_, _ = rand.New(rand.NewSource(73013)).Read(payload)
	if n, err := clientStream.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write final window: n=%d err=%v", n, err)
	}
	if err := clientStream.Close(); err != nil {
		t.Fatalf("close sending stream: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close sending session: %v", err)
	}

	got := contractReadAll(t, serverStream, 32771)
	if !bytes.Equal(got, payload) {
		t.Fatalf("final payload mismatch: got=%d want=%d", len(got), len(payload))
	}
}

func TestContractReadDeadlineReplacesBlockedWait(t *testing.T) {
	_, _, clientStream, _ := contractStreamPair(t)
	result := make(chan error, 1)
	go func() {
		_, err := clientStream.Read(make([]byte, 13))
		result <- err
	}()

	time.Sleep(15 * time.Millisecond)
	if err := clientStream.SetReadDeadline(time.Now().Add(80 * time.Millisecond)); err != nil {
		t.Fatalf("set first read deadline: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	extendedAt := time.Now()
	if err := clientStream.SetReadDeadline(extendedAt.Add(120 * time.Millisecond)); err != nil {
		t.Fatalf("extend read deadline: %v", err)
	}

	select {
	case err := <-result:
		t.Fatalf("read used stale deadline: %v", err)
	case <-time.After(85 * time.Millisecond):
	}

	select {
	case err := <-result:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("blocked read returned %v, want timeout", err)
		}
		netErr, ok := err.(net.Error)
		if !ok || !netErr.Timeout() {
			t.Fatalf("timeout does not satisfy net.Error: %T %v", err, err)
		}
		if time.Since(extendedAt) < 100*time.Millisecond {
			t.Fatalf("extended deadline fired too early")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("blocked read did not observe updated deadline")
	}
}

func TestContractTimeoutMatchesNetConnSemantics(t *testing.T) {
	if ErrTimeout.Error() != "i/o deadline reached" {
		t.Fatalf("timeout error message changed: %q", ErrTimeout.Error())
	}
	var netErr net.Error
	if !errors.As(ErrTimeout, &netErr) || !netErr.Timeout() || !netErr.Temporary() {
		t.Fatalf("timeout is not a temporary net.Error: %T %v", ErrTimeout, ErrTimeout)
	}
	if !errors.Is(ErrTimeout, os.ErrDeadlineExceeded) {
		t.Fatalf("timeout does not wrap os.ErrDeadlineExceeded: %v", ErrTimeout)
	}
}

func TestContractWriteDeadlineWakesFlowControlWait(t *testing.T) {
	_, _, clientStream, _ := contractStreamPair(t)
	payload := make([]byte, int(initialStreamWindow)+4096)
	result := make(chan contractWriteResult, 1)
	go func() {
		n, err := clientStream.Write(payload)
		result <- contractWriteResult{n: n, err: err}
	}()

	contractWaitFor(t, 2*time.Second, func() bool {
		return atomic.LoadUint32(&clientStream.sendWindow) == 0
	}, "writer did not consume its initial flow-control window")

	if err := clientStream.SetWriteDeadline(time.Now().Add(40 * time.Millisecond)); err != nil {
		t.Fatalf("set live write deadline: %v", err)
	}

	select {
	case got := <-result:
		if got.n != int(initialStreamWindow) {
			t.Fatalf("write count=%d want=%d", got.n, initialStreamWindow)
		}
		if !errors.Is(got.err, ErrTimeout) {
			t.Fatalf("blocked write returned %v, want timeout", got.err)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("blocked write did not observe updated deadline")
	}
}

func TestContractReadDeadlineCanBeClearedWhileBlocked(t *testing.T) {
	_, _, clientStream, serverStream := contractStreamPair(t)
	if err := clientStream.SetReadDeadline(time.Now().Add(80 * time.Millisecond)); err != nil {
		t.Fatalf("set initial read deadline: %v", err)
	}

	type readResult struct {
		n   int
		b   byte
		err error
	}
	started := make(chan struct{})
	result := make(chan readResult, 1)
	go func() {
		close(started)
		buf := make([]byte, 1)
		n, err := clientStream.Read(buf)
		result <- readResult{n: n, b: buf[0], err: err}
	}()
	<-started
	time.Sleep(15 * time.Millisecond)
	if err := clientStream.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}

	select {
	case got := <-result:
		t.Fatalf("cleared read deadline still completed the read: n=%d err=%v", got.n, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	if n, err := serverStream.Write([]byte{0xa7}); err != nil || n != 1 {
		t.Fatalf("write after clearing read deadline: n=%d err=%v", n, err)
	}
	select {
	case got := <-result:
		if got.n != 1 || got.b != 0xa7 || got.err != nil {
			t.Fatalf("read after clearing deadline: n=%d b=%x err=%v", got.n, got.b, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleared read deadline did not leave the blocked read live")
	}
}

func TestContractWriteDeadlineCanBeClearedWhileBlocked(t *testing.T) {
	_, _, clientStream, serverStream := contractStreamPair(t)
	if err := clientStream.SetWriteDeadline(time.Now().Add(80 * time.Millisecond)); err != nil {
		t.Fatalf("set initial write deadline: %v", err)
	}
	payload := make([]byte, int(initialStreamWindow)+4096)
	_, _ = rand.New(rand.NewSource(61001)).Read(payload)
	result := make(chan contractWriteResult, 1)
	go func() {
		n, err := clientStream.Write(payload)
		result <- contractWriteResult{n: n, err: err}
	}()

	contractWaitFor(t, 2*time.Second, func() bool {
		return atomic.LoadUint32(&clientStream.sendWindow) == 0
	}, "writer did not block on the initial flow-control window")
	if err := clientStream.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear write deadline: %v", err)
	}
	select {
	case got := <-result:
		t.Fatalf("cleared write deadline still completed the blocked write: n=%d err=%v", got.n, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	gotPayload := make([]byte, len(payload))
	readResult := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(serverStream, gotPayload)
		readResult <- err
	}()
	select {
	case got := <-result:
		if got.n != len(payload) || got.err != nil {
			t.Fatalf("write after clearing deadline: n=%d err=%v", got.n, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleared write deadline did not leave the blocked write live")
	}
	select {
	case err := <-readResult:
		if err != nil {
			t.Fatalf("read payload after clearing write deadline: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not receive payload after clearing write deadline")
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatal("payload changed after clearing write deadline")
	}
}

func TestContractShortFrameChargesActualBytes(t *testing.T) {
	cfg := contractConfig()
	session := &Session{config: cfg, logger: log.New(io.Discard, "", 0)}
	stream := newStream(session, 17, streamEstablished)

	actual := make([]byte, 73)
	_, _ = rand.New(rand.NewSource(9901)).Read(actual)
	const declared = uint32(4096)
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeData, 0, stream.id, declared)
	before := stream.recvWindow

	if err := stream.readData(hdr, 0, bytes.NewReader(actual)); err != nil {
		t.Fatalf("read short frame: %v", err)
	}
	if stream.recvBuf == nil || !bytes.Equal(stream.recvBuf.Bytes(), actual) {
		t.Fatalf("short frame buffered the wrong bytes")
	}
	want := before - uint32(len(actual))
	if stream.recvWindow != want {
		t.Fatalf("receive window=%d want=%d (declared=%d actual=%d)", stream.recvWindow, want, declared, len(actual))
	}
}

type contractStagedConn struct {
	blockAt int32
	attempt int32

	started chan struct{}
	release chan struct{}
	closed  chan struct{}

	startOnce   sync.Once
	releaseOnce sync.Once
	closeOnce   sync.Once

	mu       sync.Mutex
	captured [][]byte
}

func newContractStagedConn(blockAt int32) *contractStagedConn {
	return &contractStagedConn{
		blockAt: blockAt,
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (c *contractStagedConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *contractStagedConn) Write(p []byte) (int, error) {
	index := atomic.AddInt32(&c.attempt, 1)
	if index == c.blockAt {
		c.startOnce.Do(func() { close(c.started) })
		select {
		case <-c.release:
		case <-c.closed:
			return 0, io.ErrClosedPipe
		}
	}
	copyOfP := append([]byte(nil), p...)
	c.mu.Lock()
	c.captured = append(c.captured, copyOfP)
	c.mu.Unlock()
	return len(p), nil
}

func (c *contractStagedConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *contractStagedConn) releaseWrite() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *contractStagedConn) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.captured))
	for i := range c.captured {
		out[i] = append([]byte(nil), c.captured[i]...)
	}
	return out
}

func contractStagedStream(t *testing.T) (*Session, *Stream, *contractStagedConn) {
	t.Helper()
	conn := newContractStagedConn(2)
	cfg := contractConfig()
	cfg.ConnectionWriteTimeout = 70 * time.Millisecond
	cfg.StreamOpenTimeout = 0
	session, err := Client(conn, cfg)
	if err != nil {
		t.Fatalf("create staged session: %v", err)
	}
	t.Cleanup(func() {
		conn.releaseWrite()
		done := make(chan struct{})
		go func() { _ = session.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})
	stream, err := session.OpenStream()
	if err != nil {
		t.Fatalf("open staged stream: %v", err)
	}
	contractWaitFor(t, time.Second, func() bool { return len(conn.snapshot()) == 1 }, "initial SYN was not written")
	return session, stream, conn
}

func contractWaitFor(t *testing.T, limit time.Duration, predicate func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

func TestContractQueuedDataFrameIsSnapshot(t *testing.T) {
	session, stream, conn := contractStagedStream(t)

	first := make([]byte, 257)
	_, _ = rand.New(rand.NewSource(44121)).Read(first)
	originalFirst := append([]byte(nil), first...)
	firstResult := make(chan contractWriteResult, 1)
	go func() {
		n, err := stream.Write(first)
		firstResult <- contractWriteResult{n: n, err: err}
	}()

	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("first data header never reached delayed transport")
	}

	select {
	case got := <-firstResult:
		if got.n != 0 || !errors.Is(got.err, ErrConnectionWriteTimeout) {
			t.Fatalf("first write result: n=%d err=%v", got.n, got.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first delayed write did not time out")
	}

	for i := range first {
		first[i] ^= 0xff
	}
	second := make([]byte, 91)
	_, _ = rand.New(rand.NewSource(44122)).Read(second)
	secondResult := make(chan contractWriteResult, 1)
	go func() {
		n, err := stream.Write(second)
		secondResult <- contractWriteResult{n: n, err: err}
	}()

	contractWaitFor(t, time.Second, func() bool { return len(session.sendCh) >= 1 }, "retry was not queued")
	conn.releaseWrite()
	contractWaitFor(t, time.Second, func() bool { return len(conn.snapshot()) >= 5 }, "queued data frames did not drain")

	select {
	case got := <-secondResult:
		if got.n != len(second) || got.err != nil {
			t.Fatalf("second write result: n=%d err=%v", got.n, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("second write did not finish")
	}

	writes := conn.snapshot()
	firstHeader := header(writes[1])
	if len(firstHeader) != headerSize || firstHeader.MsgType() != typeData || firstHeader.Length() != uint32(len(originalFirst)) {
		t.Fatalf("first queued header mutated: %v", firstHeader)
	}
	if !bytes.Equal(writes[2], originalFirst) {
		t.Fatalf("first queued body aliased caller buffer")
	}
	secondHeader := header(writes[3])
	if len(secondHeader) != headerSize || secondHeader.MsgType() != typeData || secondHeader.Length() != uint32(len(second)) {
		t.Fatalf("second queued header invalid: %v", secondHeader)
	}
	if !bytes.Equal(writes[4], second) {
		t.Fatalf("second queued body mismatch")
	}
}

func TestContractQueuedControlHeaderIsSnapshot(t *testing.T) {
	session, stream, conn := contractStagedStream(t)

	stream.recvLock.Lock()
	stream.recvWindow = 0
	stream.recvBuf = nil
	stream.recvLock.Unlock()

	firstResult := make(chan error, 1)
	go func() { firstResult <- stream.sendWindowUpdate() }()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("window update never reached delayed transport")
	}
	select {
	case err := <-firstResult:
		if !errors.Is(err, ErrConnectionWriteTimeout) {
			t.Fatalf("window update returned %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("window update did not time out")
	}

	secondResult := make(chan error, 1)
	go func() { secondResult <- stream.sendClose() }()
	contractWaitFor(t, time.Second, func() bool { return len(session.sendCh) >= 1 }, "close frame was not queued")
	conn.releaseWrite()
	contractWaitFor(t, time.Second, func() bool { return len(conn.snapshot()) >= 3 }, "queued control frames did not drain")

	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("close frame returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close frame did not finish")
	}

	writes := conn.snapshot()
	windowHeader := header(writes[1])
	if len(windowHeader) != headerSize || windowHeader.MsgType() != typeWindowUpdate || windowHeader.Flags() != 0 || windowHeader.Length() != stream.session.config.MaxStreamWindowSize {
		t.Fatalf("queued window header mutated: %v", windowHeader)
	}
	closeHeader := header(writes[2])
	if len(closeHeader) != headerSize || closeHeader.MsgType() != typeWindowUpdate || closeHeader.Flags()&flagFIN == 0 || closeHeader.Length() != 0 {
		t.Fatalf("queued close header invalid: %v", closeHeader)
	}
}

func TestContractPreservesOrdinaryBidirectionalStream(t *testing.T) {
	client, _, clientStream, serverStream := contractStreamPair(t)
	request := []byte("ordinary-mux-request")
	response := []byte("ordinary-response-with-a-different-length")

	if n, err := clientStream.Write(request); err != nil || n != len(request) {
		t.Fatalf("write request: n=%d err=%v", n, err)
	}
	gotRequest := make([]byte, len(request))
	if _, err := io.ReadFull(serverStream, gotRequest); err != nil || !bytes.Equal(gotRequest, request) {
		t.Fatalf("read request: err=%v got=%q", err, gotRequest)
	}
	if n, err := serverStream.Write(response); err != nil || n != len(response) {
		t.Fatalf("write response: n=%d err=%v", n, err)
	}
	gotResponse := make([]byte, len(response))
	if _, err := io.ReadFull(clientStream, gotResponse); err != nil || !bytes.Equal(gotResponse, response) {
		t.Fatalf("read response: err=%v got=%q", err, gotResponse)
	}
	if _, err := client.Ping(); err != nil {
		t.Fatalf("ping after stream traffic: %v", err)
	}
}

func TestContractConcurrentIndependentStreams(t *testing.T) {
	client, server := contractSessions(t)
	const streamCount = 8
	type pair struct {
		client   *Stream
		server   *Stream
		request  []byte
		response []byte
	}
	pairs := make([]pair, 0, streamCount)
	for i := 0; i < streamCount; i++ {
		clientStream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("open stream %d: %v", i, err)
		}
		serverStream, err := server.AcceptStream()
		if err != nil {
			t.Fatalf("accept stream %d: %v", i, err)
		}
		pairs = append(pairs, pair{
			client:   clientStream,
			server:   serverStream,
			request:  bytes.Repeat([]byte{byte(i + 1), 0x00, 0xff}, 90+i),
			response: bytes.Repeat([]byte{0xfe, byte(0x80 + i)}, 130+i),
		})
	}

	errCh := make(chan error, streamCount*2)
	var wg sync.WaitGroup
	for i := range pairs {
		p := pairs[i]
		wg.Add(2)
		go func(index int) {
			defer wg.Done()
			defer func() { _ = p.client.Close() }()
			if n, err := p.client.Write(p.request); err != nil || n != len(p.request) {
				errCh <- fmt.Errorf("stream %d request write: n=%d err=%v", index, n, err)
				return
			}
			got := make([]byte, len(p.response))
			if _, err := io.ReadFull(p.client, got); err != nil {
				errCh <- fmt.Errorf("stream %d response read: %w", index, err)
				return
			}
			if !bytes.Equal(got, p.response) {
				errCh <- fmt.Errorf("stream %d response crossed stream boundary", index)
			}
		}(i)
		go func(index int) {
			defer wg.Done()
			defer func() { _ = p.server.Close() }()
			got := make([]byte, len(p.request))
			if _, err := io.ReadFull(p.server, got); err != nil {
				errCh <- fmt.Errorf("stream %d request read: %w", index, err)
				return
			}
			if !bytes.Equal(got, p.request) {
				errCh <- fmt.Errorf("stream %d request crossed stream boundary", index)
				return
			}
			if n, err := p.server.Write(p.response); err != nil || n != len(p.response) {
				errCh <- fmt.Errorf("stream %d response write: n=%d err=%v", index, n, err)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("concurrent independent streams did not finish")
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestContractGoAwayRejectsOnlyNewStreams(t *testing.T) {
	client, server, clientStream, serverStream := contractStreamPair(t)
	if err := server.GoAway(); err != nil {
		t.Fatalf("send go-away: %v", err)
	}
	contractWaitFor(t, time.Second, func() bool {
		return atomic.LoadInt32(&client.remoteGoAway) == 1
	}, "client did not observe peer go-away")

	payload := []byte("existing-stream-survives-go-away")
	if n, err := clientStream.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write existing stream after go-away: n=%d err=%v", n, err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(serverStream, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("read existing stream after go-away: err=%v got=%q", err, got)
	}
	if _, err := client.OpenStream(); !errors.Is(err, ErrRemoteGoAway) {
		t.Fatalf("new stream after go-away returned %v, want %v", err, ErrRemoteGoAway)
	}
	if _, err := client.Ping(); err != nil {
		t.Fatalf("ping after go-away: %v", err)
	}
}

func TestContractResetPropagatesAfterCloseTimeout(t *testing.T) {
	_, server, clientStream, serverStream := contractStreamPair(t)
	server.config.StreamCloseTimeout = 60 * time.Millisecond
	if err := serverStream.Close(); err != nil {
		t.Fatalf("close remote stream: %v", err)
	}
	contractWaitFor(t, time.Second, func() bool {
		clientStream.stateLock.Lock()
		defer clientStream.stateLock.Unlock()
		return clientStream.state == streamReset
	}, "peer did not receive reset after close timeout")
	if n, err := clientStream.Write([]byte("after-reset")); n != 0 || !errors.Is(err, ErrConnectionReset) {
		t.Fatalf("write after reset: n=%d err=%v", n, err)
	}
	if n, err := clientStream.Read(make([]byte, 1)); n != 0 || !errors.Is(err, ErrConnectionReset) {
		t.Fatalf("read after reset: n=%d err=%v", n, err)
	}
}

func TestContractPublicStreamMetadataAndAliases(t *testing.T) {
	client, server := contractSessions(t)
	clientConn, err := client.Open()
	if err != nil {
		t.Fatalf("Open alias: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	serverStream, err := server.AcceptStreamWithContext(ctx)
	if err != nil {
		t.Fatalf("AcceptStreamWithContext: %v", err)
	}
	clientStream, ok := clientConn.(*Stream)
	if !ok {
		t.Fatalf("Open returned %T, want *Stream implementing net.Conn", clientConn)
	}
	if clientStream.Session() != client || serverStream.Session() != server {
		t.Fatal("stream Session metadata does not identify its owner")
	}
	if clientStream.StreamID() == 0 || clientStream.StreamID()%2 != 1 || clientStream.StreamID() != serverStream.StreamID() {
		t.Fatalf("unexpected client stream ID parity: client=%d server=%d", clientStream.StreamID(), serverStream.StreamID())
	}
	if client.NumStreams() != 1 || server.NumStreams() != 1 {
		t.Fatalf("NumStreams mismatch: client=%d server=%d", client.NumStreams(), server.NumStreams())
	}
	if clientStream.LocalAddr() == nil || clientStream.RemoteAddr() == nil || client.LocalAddr() == nil || client.RemoteAddr() == nil {
		t.Fatal("public address metadata must remain available")
	}
	if clientStream.LocalAddr().String() != client.LocalAddr().String() || clientStream.RemoteAddr().String() != client.RemoteAddr().String() {
		t.Fatal("stream address metadata differs from its session")
	}
	if err := clientStream.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("clear combined deadline: %v", err)
	}
	clientStream.Shrink()
	if err := clientStream.Close(); err != nil {
		t.Fatalf("close client stream: %v", err)
	}
	if err := serverStream.Close(); err != nil {
		t.Fatalf("close server stream: %v", err)
	}
}

type contractSilentConn struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newContractSilentConn() *contractSilentConn {
	return &contractSilentConn{closed: make(chan struct{})}
}

func (c *contractSilentConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *contractSilentConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}

func (c *contractSilentConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func TestContractOpenTimeoutClosesUnacknowledgedSession(t *testing.T) {
	conn := newContractSilentConn()
	cfg := contractConfig()
	cfg.StreamOpenTimeout = 40 * time.Millisecond
	cfg.ConnectionWriteTimeout = 200 * time.Millisecond
	session, err := Client(conn, cfg)
	if err != nil {
		t.Fatalf("create silent-peer session: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	if _, err := session.OpenStream(); err != nil {
		t.Fatalf("queue unacknowledged stream: %v", err)
	}
	select {
	case <-session.CloseChan():
		if !session.IsClosed() {
			t.Fatal("open timeout closed the signal channel without closing the session")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("unacknowledged stream did not close its session at StreamOpenTimeout")
	}
}

type contractKeepaliveBlockedConn struct {
	unblock     chan struct{}
	unblockOnce sync.Once
}

func newContractKeepaliveBlockedConn() *contractKeepaliveBlockedConn {
	return &contractKeepaliveBlockedConn{unblock: make(chan struct{})}
}

func (c *contractKeepaliveBlockedConn) Read([]byte) (int, error) {
	<-c.unblock
	return 0, io.EOF
}

func (c *contractKeepaliveBlockedConn) Write([]byte) (int, error) {
	<-c.unblock
	return 0, io.ErrClosedPipe
}

func (c *contractKeepaliveBlockedConn) Close() error { return nil }

func (c *contractKeepaliveBlockedConn) release() {
	c.unblockOnce.Do(func() { close(c.unblock) })
}

func TestContractCloseAfterKeepaliveFailureIsIdempotent(t *testing.T) {
	conn := newContractKeepaliveBlockedConn()
	cfg := contractConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = 10 * time.Millisecond
	cfg.ConnectionWriteTimeout = 25 * time.Millisecond
	session, err := Server(conn, cfg)
	if err != nil {
		t.Fatalf("create blocked keepalive session: %v", err)
	}
	t.Cleanup(conn.release)

	select {
	case <-session.CloseChan():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("keepalive failure did not start session shutdown")
	}

	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("repeat Close after keepalive failure: %v", err)
		}
	case <-time.After(125 * time.Millisecond):
		conn.release()
		<-closed
		t.Fatal("Close blocked after keepalive-triggered shutdown already won ownership")
	}
}

type contractSplitCloseConn struct {
	readClosed   chan struct{}
	writeStarted chan struct{}
	writeRelease chan struct{}

	readOnce    sync.Once
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newContractSplitCloseConn() *contractSplitCloseConn {
	return &contractSplitCloseConn{
		readClosed:   make(chan struct{}),
		writeStarted: make(chan struct{}),
		writeRelease: make(chan struct{}),
	}
}

func (c *contractSplitCloseConn) Read([]byte) (int, error) {
	<-c.readClosed
	return 0, io.EOF
}

func (c *contractSplitCloseConn) Write(p []byte) (int, error) {
	c.startedOnce.Do(func() { close(c.writeStarted) })
	<-c.writeRelease
	return len(p), nil
}

func (c *contractSplitCloseConn) Close() error {
	c.readOnce.Do(func() { close(c.readClosed) })
	return nil
}

func (c *contractSplitCloseConn) releaseWrite() {
	c.releaseOnce.Do(func() { close(c.writeRelease) })
}

func TestContractNormalCloseWaitsForSendQuiescence(t *testing.T) {
	conn := newContractSplitCloseConn()
	cfg := contractConfig()
	cfg.ConnectionWriteTimeout = 500 * time.Millisecond
	session, err := Client(conn, cfg)
	if err != nil {
		t.Fatalf("create split-close session: %v", err)
	}
	t.Cleanup(conn.releaseWrite)

	hdr := header(make([]byte, headerSize))
	hdr.encode(typePing, flagSYN, 0, 0x51554945)
	go func() { _ = session.waitForSend(hdr, nil) }()
	select {
	case <-conn.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("send loop did not enter the delayed transport write")
	}

	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()
	select {
	case err := <-closed:
		conn.releaseWrite()
		t.Fatalf("Close returned before the send loop stopped: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	conn.releaseWrite()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after send quiescence: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the delayed send loop stopped")
	}
}

func TestContractPingReplyDoesNotBlockReceiveDispatch(t *testing.T) {
	cfg := contractConfig()
	cfg.ConnectionWriteTimeout = 250 * time.Millisecond
	session := &Session{
		config:     cfg,
		logger:     log.New(io.Discard, "", 0),
		pings:      make(map[uint32]chan struct{}),
		sendCh:     make(chan *sendReady, 1),
		shutdownCh: make(chan struct{}),
	}
	session.sendCh <- &sendReady{}

	hdr := header(make([]byte, headerSize))
	hdr.encode(typePing, flagSYN, 0, 0x70696e67)
	result := make(chan error, 1)
	go func() { result <- session.handlePing(hdr) }()

	select {
	case err := <-result:
		close(session.shutdownCh)
		if err != nil {
			t.Fatalf("dispatch ping query: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		close(session.shutdownCh)
		<-result
		t.Fatal("ping reply blocked receive dispatch behind a congested send queue")
	}
}

func TestContractFailedPingsReleaseTracking(t *testing.T) {
	cfg := contractConfig()
	cfg.ConnectionWriteTimeout = 20 * time.Millisecond
	session := &Session{
		config:     cfg,
		logger:     log.New(io.Discard, "", 0),
		pings:      make(map[uint32]chan struct{}),
		sendCh:     make(chan *sendReady, 1),
		shutdownCh: make(chan struct{}),
	}
	session.sendCh <- &sendReady{}

	for i := 0; i < 4; i++ {
		if _, err := session.Ping(); !errors.Is(err, ErrConnectionWriteTimeout) {
			t.Fatalf("failed ping %d returned %v, want write timeout", i, err)
		}
		session.pingLock.Lock()
		pending := len(session.pings)
		session.pingLock.Unlock()
		if pending != 0 {
			t.Fatalf("failed ping %d retained %d pending entries", i, pending)
		}
	}
}
