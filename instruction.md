# Restore Yamux stream lifecycle and delayed-write integrity

## Background

A relay service multiplexes long-lived RPC streams over one ordered
connection with the Go library in `/app`. Ordinary request/response traffic
usually succeeds, but production-style shutdowns and congested transports
expose several correctness failures: a response tail can disappear after the
request side closes, goroutines can remain blocked after their deadlines are
changed, a short final frame can corrupt flow-control accounting, and a frame
that is still queued after a write timeout can change when its caller reuses a
buffer or retries.

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
- Expiration returns the package's timeout error. That error must continue to
  implement `net.Error`, and `Timeout()` must return `true`.
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

## Compatibility constraints

- Keep the 12-byte, big-endian Yamux framing and existing stream-ID parity.
- Keep the initial 256 KiB per-stream window and the configured maximum-window
  checks.
- Preserve the exported API, ping/go-away behavior, concurrent independent
  streams, reset behavior, and bounded connection-write timeout.
- Do not add a background daemon, external service, credential, or network
  dependency. Do not replace the protocol with a different implementation.
- Production Go source under `/app` may be edited. Documentation and license
  notices must remain present.

The repaired library must compile with the supplied Go toolchain. You may use
`go test ./...` and additional local diagnostics while working.
