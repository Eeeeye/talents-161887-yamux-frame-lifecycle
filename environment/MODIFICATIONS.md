# Evaluator modifications

This source snapshot is based on HashiCorp Yamux commit
`76348a0c5ab413fe3c5c564077ea26e21a8769a3` and remains licensed under the
Mozilla Public License 2.0. For this debugging evaluator, production source
has been modified to reproduce stream-lifecycle, timeout, flow-control, and
queued-write regressions, together with silent-open, ping-dispatch, and
shutdown-ordering failures. It must not be treated as an unmodified upstream
release.
