package difuse

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ipkg/difuse/store"
	chord "github.com/ipkg/go-chord"
)

func prepDifuse(p int, j ...string) (*Difuse, error) {
	c1 := DefaultConfig()
	c1.Chord.StabilizeMin = 20 * time.Millisecond
	c1.Chord.StabilizeMax = 50 * time.Millisecond

	c1.BindAddr = fmt.Sprintf("127.0.0.1:%d", p)
	if err := c1.ValidateAddrs(); err != nil {
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
		sault1.RegisterRing(ring)
	}

	return sault1, err
}

func TestDifuseLookupLeader(t *testing.T) {
	s1, err := prepDifuse(12345)
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(200 * time.Millisecond)

	s2, err := prepDifuse(12346, "127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(2 * time.Second)

	tn1 := s1.transport
	tn2 := s2.transport

	if len(tn1.local) != 8 || len(tn2.local) != 8 {
		t.Error("not all vn's init'd")
	}

	testkey := []byte("test-key-for-leader-election")

	lvn1, _, vm1, err := s1.transport.LookupLeader("127.0.0.1:12346", testkey)
	if err != nil {
		t.Fatal(err)
	}

	lvn2, _, vm2, err := s2.transport.LookupLeader("127.0.0.1:12345", testkey)
	if err != nil {
		t.Fatal(err)
	}

	if lvn1.String() != lvn2.String() {
		t.Errorf("leader mismatch %s!=%s", lvn1.String(), lvn2.String())
	}

	if _, _, err = s1.Stat([]byte("Key")); err == nil {
		t.Fatal("should fail")
	}

	if _, _, err = s1.Stat([]byte("Key"), RequestOptions{Consistency: 99}); err == nil {
		t.Fatal("should fail")
	}

	if _, ok := vm1[lvn1.Host]; !ok {
		t.Error("leader not in map")
	}
	if _, ok := vm2[lvn2.Host]; !ok {
		t.Error("leader not in map")
	}

}

func TestDifuseSetStat(t *testing.T) {
	s1, err := prepDifuse(23456)
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(300 * time.Millisecond)

	s2, err := prepDifuse(23467, "127.0.0.1:23456")
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(400 * time.Millisecond)

	testkey := []byte("testkey")
	testval := []byte("testvalue")
	testInode := store.NewInodeFromData(testkey, testval)

	if _, err = s1.SetInode(testInode, nil); err != nil {
		t.Fatal(err)
	}

	ind1, _, err := s1.Stat(testkey)
	if err != nil {
		t.Fatal(err)
	}

	ind2, _, err := s2.Stat(testkey)
	if err != nil {
		t.Fatal(err)
	}

	if ind1.Size != ind2.Size {
		t.Error("size mismatch")
	}

	if !reflect.DeepEqual(ind1.Blocks[0], ind2.Blocks[0]) {
		t.Fatal("block hash mismatch")
	}

	_, _, err = s1.Stat(testkey, RequestOptions{Consistency: ConsistencyLazy})
	if err != nil {
		t.Error(err)
	}

	if _, err = s2.SetBlock([]byte("testdata")); err != nil {
		t.Fatal(err)
	}

	if _, err = s2.Set(testkey, testval); err != nil {
		t.Fatal(err)
	}

	val, _, err := s1.Get(testkey)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != string(testval) {
		t.Fatal("value mismatch")
	}

	if _, _, err = s2.Delete(testkey); err != nil {
		//t.Fatal("should fail")
		t.Fatal(err)
	}

	//<-time.After(1 * time.Second)
	if _, _, err = s1.Delete(testkey); err == nil {
		t.Fatal("should fail")
	}

	/*if _, _, err = s1.LastTx(testkey); err != nil {
		t.Fatal(err)
	}*/
}
