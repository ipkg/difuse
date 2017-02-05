
NAME = difused
VERSION = $(shell grep 'const VERSION' version.go | cut -d'"' -f 2)

BRANCH = $(shell git rev-parse --abbrev-ref HEAD || echo unknown)
COMMIT = $(shell git rev-parse --short HEAD || echo unknown)
BUILDTIME = $(shell date +%Y-%m-%dT%T%z)


LD_OPTS = -ldflags="-X main.branch=${BRANCH} -X main.commit=${COMMIT} -X main.buildtime=${BUILDTIME} -w"
BUILD_CMD = CGO_ENABLED=0 go build -a -tags netgo -installsuffix netgo ${LD_OPTS}

clean:
	rm -f ${NAME}
	rm -f ${NAME}-darwin
	rm -f ${NAME}-linux
	rm -f ${NAME}-win
	go clean -i ./...
	rm -rf vendor/

deps:
	go get -d -v ./...

fbs:
	rm -rf fbtypes/*.go
	flatc -I ../../ -g ./fbtypes/fbtypes.fbs

protoc:
	protoc -I ../../../ -I ./netrpc ./netrpc/net.proto --go_out=plugins=grpc:./netrpc

test:
	go test -cover . ./store ./txlog

${NAME}:
	go build ${LD_OPTS} -o ${NAME} cmd/*.go

dist:
	@# Darwin
	GOOS=darwin ${BUILD_CMD} -o ${NAME}-darwin cmd/*.go
	@# Linux
	GOOS=linux ${BUILD_CMD} -o ${NAME}-linux cmd/*.go
	@# Windows
	GOOS=windows ${BUILD_CMD} -o ${NAME}-win cmd/*.go
