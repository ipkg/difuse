

NAME = difuse
VERSION = $(shell grep 'const VERSION' version.go | cut -d'"' -f 2)

BRANCH = $(shell git rev-parse --abbrev-ref HEAD || echo unknown)
COMMIT = $(shell git rev-parse --short HEAD || echo unknown)
BUILDTIME = $(shell date +%Y-%m-%dT%T%z)

LD_OPTS = -ldflags="-X main.branch=${BRANCH} -X main.commit=${COMMIT} -X main.buildtime=${BUILDTIME} -w"
BUILD_CMD = CGO_ENABLED=0 go build -a -tags netgo -installsuffix netgo

clean:
	rm -f testube

protoc-types:
	@rm -f types/types.pb.go
	protoc -I ../../../ -I ./types ./types/types.proto --go_out=plugins=grpc:./types

protoc-rpc:
	@rm -f rpc/rpc.pb.go
	protoc -I ../../../ -I ./types -I ./rpc ./rpc/rpc.proto --go_out=plugins=grpc:./rpc

protoc: protoc-types protoc-rpc

test:
	@go test -cover .
	@go test -cover ./txlog
	@go test -cover ./types
	@go test -cover ./keypairs
	@go test -cover ./utils

testube:
	$(BUILD_CMD) -o testube ./cmd/testube/main.go
