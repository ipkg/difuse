package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/ipkg/difuse"
	"github.com/ipkg/difuse/keypairs"
	"github.com/ipkg/difuse/rpc"
	"github.com/ipkg/difuse/types"
	chord "github.com/ipkg/go-chord"
)

var (
	bindAddr = flag.String("b", "127.0.0.1:12345", "Bind address")
	joinAddr = flag.String("j", "", "Join address")
	httpAddr = flag.String("http", "127.0.0.1:65432", "HTTP bind address")
)

func initConfig() (*difuse.Config, error) {
	c1 := difuse.DefaultConfig()
	c1.Chord.StabilizeMin = 3 * time.Second
	c1.Chord.StabilizeMax = 10 * time.Second
	c1.BindAddr = *bindAddr
	//c1.HTTPAddr = *httpAddr

	err := c1.ValidateAddrs()
	if err != nil {
		return nil, err
	}

	c1.Signator, err = keypairs.GenerateECDSAKeypair()
	return c1, err
}

func newGRPCServer() (net.Listener, *grpc.Server, error) {
	ln, err := net.Listen("tcp", *bindAddr)
	if err != nil {
		return nil, nil, err
	}

	//	opt := grpc.CustomCodec(&chord.PayloadCodec{})
	return ln, grpc.NewServer(), nil
}

func init() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

func readStdin() chan []byte {

	cb := make(chan []byte)
	sc := bufio.NewScanner(os.Stdin)

	go func() {
		if sc.Scan() {
			cb <- sc.Bytes()
		}
	}()

	return cb
}

func main() {
	conf, err := initConfig()
	if err != nil {
		log.Fatal(err)
	}

	log.SetPrefix(fmt.Sprintf("[%s] ", conf.Chord.Hostname))

	ln, gserver, err := newGRPCServer()
	if err != nil {
		log.Fatal(err)
	}

	// Init difuse net
	dtrans := difuse.NewNetTransport(conf.Chord.Hostname)
	// Init difuse
	df := difuse.NewDifuse(conf, dtrans)
	// Register difuse net to grpc
	rpc.RegisterDifuseRPCServer(gserver, dtrans)
	// Set chord delegate before init'ing the ring.
	conf.Chord.Delegate = df

	// Init chord transport
	ctrans := chord.NewGRPCTransport(ln, gserver, 3*time.Second, 300*time.Second)
	// Register chord transport
	//chord.RegisterChordServer(gserver, ctrans)

	// start grpc
	go gserver.Serve(ln)

	var ring *chord.Ring
	if len(*joinAddr) > 0 {
		conf.SetPeers(*joinAddr)
		ring, err = chord.Join(conf.Chord, ctrans, conf.Peers[0])
	} else {
		ring, err = chord.Create(conf.Chord, ctrans)
	}

	if err != nil {
		log.Fatal(err)
	}

	// register ring i.e. init stores.
	if err = df.RegisterChord(ring, ctrans); err != nil {
		log.Fatal(err)
	}

	httpServer := difuse.NewHTTPAdminServer(df, "/")
	go http.ListenAndServe(*httpAddr, httpServer)

	for {
		key := <-readStdin()

		// Create new tx
		var opts types.RequestOptions
		ntx, _, err := df.NewTx(key, opts)
		if err != nil {
			log.Println("ERR", err)
			continue
		}
		// Assign data
		ntx.Data = key
		// Sign Tx
		if err = df.SignTx(ntx); err != nil {
			log.Println("ERR", err)
			continue
		}
		// Send tx proposal. We do not set the source vnode on the first proposal.
		if _, err = df.ProposeTx(ntx, opts); err != nil {
			log.Println("ERR", err)
		}

	}

}
