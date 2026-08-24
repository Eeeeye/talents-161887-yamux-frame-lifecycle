#!/bin/bash
set -u -o pipefail

reward=0
target=/app/zz_yamux_contract_test.go

mkdir -p /logs/verifier
printf '0\n' > /logs/verifier/reward.txt

finish() {
  rm -f "$target"
  printf '%s\n' "$reward" > /logs/verifier/reward.txt
}
trap finish EXIT

if ! cp /tests/yamux_contract_test.go "$target"; then
  exit 0
fi

if timeout 150s env GOMAXPROCS=4 go test -count=1 -timeout=120s ./...; then
  reward=1
fi

exit 0
