package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	chord "github.com/euforia/go-chord"
	"github.com/ipkg/difuse"
	"github.com/ipkg/difuse/netrpc"
)

var (
	branch    string
	commit    string
	buildtime string
)

var (
	cfg = difuse.DefaultConfig()

	joinAddrs   = flag.String("j", "", "List of existing peers to join")
	adminAddr   = flag.String("a", "127.0.0.1:9090", "HTTP admin address")
	debugMode   = flag.Bool("debug", false, "Turn on debug mode")
	showVersion = flag.Bool("version", false, "Show version")
)

func init() {
	flag.StringVar(&cfg.BindAddr, "b", "127.0.0.1:4624", "Bind address")
	flag.StringVar(&cfg.AdvAddr, "adv", "", "Advertise address")
	flag.Parse()

	if *showVersion {
		fver := version()
		fmt.Printf("difuse %s\n", fver)
		os.Exit(0)
	}

	if *debugMode {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
	}

	if err := cfg.ValidateAddrs(); err != nil {
		log.Fatal(err)
	}

	cfg.SetPeers(*joinAddrs)
}

func main() {
	printBanner()

	ln, server := initNet()

	// Initialize difuse transport
	dtrans := difuse.NewNetTransport()
	netrpc.RegisterDifuseRPCServer(server, dtrans)
	// Initialize difuse
	difused := difuse.NewDifuse(cfg, dtrans)
	cfg.Chord.Delegate = difused

	// Init chord transport
	ctrans := chord.NewGRPCTransport(cfg.Timeouts.RPC, cfg.Timeouts.Idle)
	chord.RegisterChordServer(server, ctrans)

	// Start serving transports
	go server.Serve(ln)

	// Init chord ring
	var (
		ring *chord.Ring
		err  error
	)
	if len(cfg.Peers) > 0 {
		ring, err = chord.Join(cfg.Chord, ctrans, cfg.Peers[0])
	} else {
		ring, err = chord.Create(cfg.Chord, ctrans)
	}

	if err != nil {
		log.Fatal(err)
	}

	difused.RegisterRing(ring)

	// Start admin server
	hs := &httpServer{tt: difused}
	http.ListenAndServe(*adminAddr, hs)

}
