

NAME = difused
VERSION = $(shell grep 'const VERSION' version.go | cut -d'"' -f 2)

BRANCH = $(shell git rev-parse --abbrev-ref HEAD || echo unknown)
COMMIT = $(shell git rev-parse --short HEAD || echo unknown)
BUILDTIME = $(shell date +%Y-%m-%dT%T%z)

LD_OPTS = -ldflags="-X main.branch=${BRANCH} -X main.commit=${COMMIT} -X main.buildtime=${BUILDTIME} -w"
BUILD_CMD = CGO_ENABLED=0 go build -a -tags netgo -installsuffix netgo

clean:
	rm -f testube
	rm -f keyber

fbs:
	@rm -rf gentypes/*.go
	flatc -I ../../../ -I . -g ./gentypes/types.fbs

	@#sed -e $$'s/import (/import (\\\n    "github.com\/ipkg\/go-chord\/fbtypes"/g' -e 's/Vnode/fbtypes.Vnode/g' ./gentypes/TransferRequest.go > ./gentypes/TransferRequest.go-sed
	@#mv ./gentypes/TransferRequest.go-sed ./gentypes/TransferRequest.go

	@#sed -e $$'s/import (/import (\\\n    "github.com\/ipkg\/go-chord\/fbtypes"/g' -e 's/*Vnode/*fbtypes.Vnode/g' -e 's/new(Vnode)/new(fbtypes.Vnode)/g' ./gentypes/TxRequest.go > ./gentypes/TxRequest.go-sed
	@#mv ./gentypes/TxRequest.go-sed ./gentypes/TxRequest.go


protoc:
	@rm -f rpc/rpc.pb.go
	protoc -I ../../../ -I ./rpc ./rpc/rpc.proto --go_out=plugins=grpc:./rpc

prep: fbs protoc

test:
	@go test -cover .
	@go test -cover ./txlog
	@go test -cover ./keypairs
	@go test -cover ./utils

testube:
	$(BUILD_CMD) -o testube ./cmd/testube/main.go
