package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse"
	"github.com/ipkg/difuse/netrpc"
)

var (
	branch    string
	commit    string
	buildtime string
)

var (
	// Conf holds the global config.
	Conf = difuse.DefaultConfig()

	joinAddrs   = flag.String("j", "", "List of existing peers to join")
	adminAddr   = flag.String("a", "127.0.0.1:9090", "HTTP admin address")
	debugMode   = flag.Bool("debug", false, "Turn on debug mode")
	showVersion = flag.Bool("version", false, "Show version")
)

func initLogger() {
	if *debugMode {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
	}
	log.SetPrefix(fmt.Sprintf("[%s] ", Conf.Chord.Hostname))
}

func init() {
	flag.StringVar(&Conf.BindAddr, "b", "127.0.0.1:4624", "Bind address")
	flag.StringVar(&Conf.AdvAddr, "adv", "", "Advertise address")
	flag.Parse()

	if *showVersion {
		fver := version()
		fmt.Printf("difuse %s\n", fver)
		os.Exit(0)
	}

	if err := Conf.ValidateAddrs(); err != nil {
		log.Fatal(err)
	}

	initLogger()

	Conf.SetPeers(*joinAddrs)
}

func initChordRing(cfg *difuse.Config, trans chord.Transport) (*chord.Ring, error) {
	if len(cfg.Peers) == 0 {
		return chord.Create(cfg.Chord, trans)
	}

	for _, peer := range cfg.Peers {
		if ring, err := chord.Join(cfg.Chord, trans, peer); err == nil {
			return ring, nil
		}
	}

	return nil, fmt.Errorf("all peers exhausted")
}

func main() {
	printBanner(Conf)

	ln, server := initNet(Conf.BindAddr)

	// Initialize difuse transport
	dtrans := difuse.NewNetTransport()
	netrpc.RegisterDifuseRPCServer(server, dtrans)
	// Initialize difuse
	difused := difuse.NewDifuse(Conf, dtrans)
	// Set difuse as the chord delegate
	Conf.Chord.Delegate = difused

	// Init chord transport
	ctrans := chord.NewGRPCTransport(Conf.Timeouts.RPC, Conf.Timeouts.Idle)
	chord.RegisterChordServer(server, ctrans)

	// Start serving transports
	go server.Serve(ln)

	// Init chord ring
	ring, err := initChordRing(Conf, ctrans)
	if err != nil {
		log.Fatal(err)
	}
	difused.RegisterRing(ring)

	// Start admin server
	hs := &httpServer{tt: difused}
	http.ListenAndServe(*adminAddr, hs)

}
