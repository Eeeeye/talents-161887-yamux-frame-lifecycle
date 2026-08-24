#!/bin/bash
set -euo pipefail

cd /app
git apply --check --unidiff-zero /solution/fix.patch
git apply --unidiff-zero /solution/fix.patch
gofmt -w stream.go session.go
go test ./...
