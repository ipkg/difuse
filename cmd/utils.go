package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	chord "github.com/euforia/go-chord"
	"github.com/ipkg/difuse"
)

func version() string {
	return fmt.Sprintf("%s %s/%s/%s", difuse.VERSION, commit, branch, buildtime)
}

func printBanner() {
	fmt.Println("difuse", version())
	fmt.Printf(`
  Bind       : %s
  Advertise  : %s
  Successors : %d
  Vnodes     : %d

  HTTP       : http://%s

`, cfg.BindAddr, cfg.AdvAddr, cfg.Chord.NumSuccessors, cfg.Chord.NumVnodes, *adminAddr)
}

func initNet() (net.Listener, *grpc.Server) {
	ln, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		log.Fatal(err)
	}

	opt := grpc.CustomCodec(&chord.PayloadCodec{})
	return ln, grpc.NewServer(opt)
}
