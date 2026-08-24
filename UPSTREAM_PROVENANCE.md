# Upstream provenance

- Repository: <https://github.com/hashicorp/yamux.git>
- Upstream commit: `76348a0c5ab413fe3c5c564077ea26e21a8769a3`
- Captured source: complete verified Git bundle
- Bundle SHA-256: `0390c5d248f758335cb2ba0daf87db80e24f83db9a10d1838a9fa17e2f0f6a60`
- License: Mozilla Public License 2.0

The production Go files and their copyright/SPDX notices come from the
upstream commit. `stream.go` and `session.go` were modified to create this
isolated debugging evaluator. The reference repair returns those behaviors to
the reviewed upstream semantics; tests are independently authored for this
activity.
