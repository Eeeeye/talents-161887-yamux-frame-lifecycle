package yamux

import (
	"bytes"
	"errors"
	"io"
	"log"
	"math/rand"
	"net"
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
