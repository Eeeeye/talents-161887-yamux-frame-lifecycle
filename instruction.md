# Restore Yamux lifecycle, liveness, and delayed-write integrity

## Background

A relay service multiplexes long-lived RPC streams over one ordered
connection with the Go library in `/app`. Ordinary request/response traffic
usually succeeds, but production-style shutdowns and congested transports
expose several correctness failures: a response tail can disappear after the
request side closes, goroutines can remain blocked after their deadlines are
changed, a short final frame can corrupt flow-control accounting, a frame that
is still queued after a write timeout can change when its caller reuses a
buffer or retries, and session setup or shutdown can strand control-plane
goroutines behind a silent peer or congested transport.

Repair the production implementation in `/app`. The result must preserve the
documented Yamux wire protocol and public API while satisfying all behavior
below.

## Required behavior

### 1. Half-close and final-data delivery

- `Stream.Close` closes the local write direction. Subsequent local writes
  fail, but local reads remain valid.
- After the local side calls `Close`, bytes already buffered or later sent by
  the peer must be returned in order. Once the peer also closes and the
  receive buffer is empty, the next read returns `io.EOF`.
- When a peer sends a final payload, closes that stream, and then closes its
  session, the receiver must be able to drain the complete buffered payload.
  Failure to send a window update on an already-shutting-down session must not
  replace successfully read payload bytes with a session-shutdown error.

These rules apply to binary payloads, payloads larger than a small application
read buffer, and payloads that consume at least one full initial stream
window.

### 2. Deadlines changed while an operation is blocked

- `SetReadDeadline` and `SetWriteDeadline` affect both future calls and calls
  that are already blocked.
- A blocked operation must promptly re-evaluate a newly set, cleared, or
  extended deadline. It must not remain asleep on the deadline value that was
  present when the operation first blocked.
- Expiration returns the package's timeout error. Its existing text remains
  `i/o deadline reached`; it must directly implement `net.Error`, return
  `true` from both `Timeout()` and `Temporary()`, and satisfy
  `errors.Is(err, os.ErrDeadlineExceeded)` for `net.Conn`, TLS, and HTTP
  compatibility.
- Updating a deadline is only a wake-up signal: clearing or extending it must
  not itself manufacture a timeout.

### 3. Exact receive-window accounting on short frames

The frame header declares an unsigned payload length, but an underlying
reader can reach EOF after returning fewer bytes. Buffer exactly the bytes
that were copied and reduce the stream's receive window by that actual count,
not by the declared count. A zero or short copy must not wrap the window or
pretend that missing bytes were consumed. Existing rejection of a declared
frame that exceeds the available receive window must remain intact.

### 4. Immutable ownership of queued frames

Encoding a header and accepting a body into the session send queue creates a
logical snapshot of that frame. If the waiting caller receives
`ErrConnectionWriteTimeout` or session shutdown while the underlying
transport write is still pending:

- later mutation or reuse of the caller's body slice must not change the body
  of the already-queued frame;
- later data, window-update, or close operations on the same stream must not
  overwrite the already-queued frame header;
- when the transport eventually progresses, each queued header must retain
  its original type, flags, stream ID, and length, and each body must still
  correspond to that header.

Do not solve this by disabling timeouts, discarding timed-out frames,
serializing the whole session behind an unbounded lock, or retaining caller
buffers forever.

### 5. Bounded stream opening and shutdown ownership

- With a positive `StreamOpenTimeout`, an outbound stream whose SYN is sent
  but never acknowledged must cause its session to begin shutdown by that
  timeout. A zero value must continue to disable this open timeout.
- A keepalive, receive, or send failure may begin shutdown concurrently with
  an explicit `Session.Close`. Shutdown notification and the underlying close
  happen at most once. If an error path already owns shutdown, later or
  concurrent `Close` calls must return promptly instead of waiting behind that
  same blocked error path.
- For an ordinary explicit close that wins shutdown ownership, `Close` must
  not return while the session send loop can still access the underlying
  connection. Once it returns, no delayed protocol write may occur.
- These rules must not leak in-flight stream/backlog bookkeeping or require an
  unbounded goroutine per retry.

### 6. Ping progress and bounded tracking

- Receiving a ping query must not block the receive dispatcher behind a full
  or delayed send queue. Attempting the ACK remains bounded by the configured
  connection-write timeout, while subsequent inbound frames can still be
  dispatched.
- Every locally initiated ping must remove its pending tracking entry on every
  terminal path, including failure to enqueue or write the query, response
  timeout, and session shutdown. Repeated failed pings must keep pending state
  bounded and a late ACK for a retired ping ID must be ignored safely.
- Successful `Ping` round trips and keepalive behavior must remain compatible.

## Compatibility constraints

- Keep the 12-byte, big-endian Yamux framing and existing stream-ID parity.
- Keep the initial 256 KiB per-stream window and the configured maximum-window
  checks.
- Preserve the exported API, ping/go-away behavior, concurrent independent
  streams, reset behavior, configured stream open/close timeouts, and bounded
  connection-write timeout.
- Do not add a background daemon, external service, credential, or network
  dependency. Do not replace the protocol with a different implementation.
- Production Go source under `/app` may be edited. Documentation and license
  notices must remain present.

The repaired library must compile with the supplied Go toolchain. You may use
`go test ./...` and additional local diagnostics while working.
