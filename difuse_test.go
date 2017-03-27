package difuse

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/ipkg/difuse/keypairs"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

func initConfig(p int) (*Config, error) {
	c1 := DefaultConfig()
	c1.Chord.StabilizeMin = 20 * time.Millisecond
	c1.Chord.StabilizeMax = 50 * time.Millisecond
	c1.BindAddr = fmt.Sprintf("127.0.0.1:%d", p)
	c1.DataDir = "/tmp/"
	err := c1.ValidateAddrs()
	if err != nil {
		return nil, err
	}

	c1.Signator, err = keypairs.GenerateECDSAKeypair()
	return c1, err
}

func prepDifuse(p int, j ...string) (*Difuse, error) {
	c1, err := initConfig(p)
	if err != nil {
		return nil, err
	}

	ln, s1, t1, err := prepGRPCTransport(p)
	if err != nil {
		return nil, err
	}

	sault1 := NewDifuse(c1, t1)
	c1.Chord.Delegate = sault1

	ct1 := chord.NewGRPCTransport(3*time.Second, 300*time.Second)
	chord.RegisterChordServer(s1, ct1)

	go s1.Serve(ln)

	var ring *chord.Ring
	if len(j) > 0 {
		ring, err = chord.Join(c1.Chord, ct1, j[0])
	} else {
		ring, err = chord.Create(c1.Chord, ct1)
	}

	if err == nil {
		sault1.RegisterChord(ring, ct1)
	}

	return sault1, err
}

func TestDifuse(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	s1, err := prepDifuse(23456)
	if err != nil {
		t.Fatal(err)
	}

	s2, err := prepDifuse(23467, "127.0.0.1:23456")
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(1000 * time.Millisecond)

	//lt1 := s1.transport.(*localTransport)
	lt1 := s1.transport
	if len(lt1.local) != 8 {
		t.Fatal("store count", len(lt1.local))
	}

	lt2 := s2.transport
	if len(lt2.local) != 8 {
		t.Fatal("store count", len(lt2.local))
	}

	<-time.After(750 * time.Millisecond)

	var opts utils.RequestOptions

	testkey := []byte("key")
	ntx, meta, err := s1.cs.NewTx(testkey, opts)
	if err != nil {
		t.Fatal(err)
	}
	ntx.Data = []byte("value")

	if err = ntx.Sign(s1.conf.Signator); err != nil {
		t.Fatal(err)
	}

	//s1.cs.ProposeTx(ntx, opts)

	err = s1.transport.ProposeTx(meta.Vnode, ntx)
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(2 * time.Second)

	_, err = s2.transport.GetTx(meta.Vnode, ntx.Hash())
	if err != nil {
		t.Fatal(err)
	}

}
