package main

import (
	"fmt"
	"log"
	"net"
	"net/http"

	"google.golang.org/grpc"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse"
)

func version() string {
	return fmt.Sprintf("%s %s/%s/%s", difuse.VERSION, commit, branch, buildtime)
}

func printBanner(cfg *difuse.Config) {
	fmt.Println("difuse", version())
	fmt.Printf(`
  Bind       : %s
  Advertise  : %s
  Successors : %d
  Vnodes     : %d

  HTTP       : http://%s

`, cfg.BindAddr, cfg.AdvAddr, cfg.Chord.NumSuccessors, cfg.Chord.NumVnodes, *adminAddr)
}

func initNet(addr string) (net.Listener, *grpc.Server) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	opt := grpc.CustomCodec(&chord.PayloadCodec{})
	return ln, grpc.NewServer(opt)
}

func parseOptions(r *http.Request) *difuse.RequestOptions {
	cst, ok := r.URL.Query()["consistency"]
	if !ok || len(cst) == 0 {
		return nil
	}

	opts := &difuse.RequestOptions{}
	switch cst[0] {
	case "lazy":
		opts.Consistency = difuse.ConsistencyLazy
	case "quorum":
		opts.Consistency = difuse.ConsistencyQuorum
	case "leader":
		opts.Consistency = difuse.ConsistencyLeader
	case "all":
		opts.Consistency = difuse.ConsistencyAll

	default:
		return nil
	}
	return opts
}
