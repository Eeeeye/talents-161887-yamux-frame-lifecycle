FROM golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2

WORKDIR /app

ENV CGO_ENABLED=0 \
    GOFLAGS=-buildvcs=false

COPY . /app/

RUN go test ./...

CMD ["/bin/bash"]
