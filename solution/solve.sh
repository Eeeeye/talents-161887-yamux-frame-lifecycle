#!/bin/bash
set -euo pipefail

cd /app
git apply --check --unidiff-zero /solution/fix.patch
git apply --unidiff-zero /solution/fix.patch
gofmt -w const.go stream.go session.go
go test ./...
