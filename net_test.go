package difuse

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/netrpc"
	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

func newGRPCServer(p int) (net.Listener, *grpc.Server, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", p)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	opt := grpc.CustomCodec(&chord.PayloadCodec{})
	server := grpc.NewServer(opt)
	return ln, server, nil
}

func prepGRPCTransport(p int) (net.Listener, *grpc.Server, *NetTransport, error) {
	ln, server, err := newGRPCServer(p)
	if err != nil {
		return nil, nil, nil, err
	}

	nt := NewNetTransport()
	netrpc.RegisterDifuseRPCServer(server, nt)

	return ln, server, nt, nil
}

func TestNetTransportBlock(t *testing.T) {
	kp1, _ := txlog.GenerateECDSAKeypair()

	ln1, svr1, nt1, err := prepGRPCTransport(32325)
	if err != nil {
		t.Fatal(err)
	}

	go svr1.Serve(ln1)

	vn1 := &chord.Vnode{Id: []byte("vnode-32325-1"), Host: "127.0.0.1:32325"}
	vn2 := &chord.Vnode{Id: []byte("vnode-32325-2"), Host: "127.0.0.1:32325"}
	vn3 := &chord.Vnode{Id: []byte("vnode-32325-3"), Host: "127.0.0.1:32325"}

	st1 := store.NewMemLoggedStore(vn1, kp1)
	st2 := store.NewMemLoggedStore(vn2, kp1)
	st3 := store.NewMemLoggedStore(vn3, kp1)
	nt1.Register(vn1, st1)
	nt1.Register(vn2, st2)
	nt1.Register(vn3, st3)

	vs := []*chord.Vnode{vn1, vn2}

	resp, err := nt1.SetBlock([]byte("lsdkfjsldkfjsdlfjsldfkjsldkfjslkfjsl"), nil, vs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 2 {
		t.Fatal("incomplete results")
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Error(err)
		}
	}

	h := resp[0].Data.([]byte)
	if resp, err = nt1.GetBlock(h, nil, vs...); err != nil {
		t.Fatal(err)
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Error(err)
		} else if r.Data == nil {
			t.Error("nil data")
		}
	}

	if err = nt1.ReplicateBlocks(vn1, vn3); err != nil {
		t.Fatal(err)
	}

	cb1 := 0
	st1.IterBlocks(func(k []byte, v []byte) error {
		cb1++
		return nil
	})

	cb3 := 0
	st3.IterBlocks(func(k []byte, v []byte) error {
		cb3++
		return nil
	})

	if cb1 != cb3 {
		t.Error("clone failed")
	}

	if resp, err = nt1.DeleteBlock(h, nil, vs...); err != nil {
		t.Fatal(err)
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Error(err)
		}
	}

}

func TestNetTransportTx(t *testing.T) {
	kp1, _ := txlog.GenerateECDSAKeypair()

	ln1, svr1, nt1, err := prepGRPCTransport(32324)
	if err != nil {
		t.Fatal(err)
	}

	go svr1.Serve(ln1)

	vn1 := &chord.Vnode{Id: []byte("vnode-1-32324-1"), Host: "127.0.0.1:32324"}
	vn2 := &chord.Vnode{Id: []byte("vnode-2-32324-2"), Host: "127.0.0.1:32324"}
	vn3 := &chord.Vnode{Id: []byte("vnode-3-32324-3"), Host: "127.0.0.1:32324"}

	st1 := store.NewMemLoggedStore(vn1, kp1)
	st2 := store.NewMemLoggedStore(vn2, kp1)
	st3 := store.NewMemLoggedStore(vn3, kp1)
	nt1.Register(vn1, st1)
	nt1.Register(vn2, st2)
	nt1.Register(vn3, st3)

	inode1 := store.NewInodeFromData([]byte("key"), []byte("foobar"))
	fb := flatbuffers.NewBuilder(0)
	ofs := inode1.Serialize(fb)
	fb.Finish(ofs)
	data := fb.Bytes[fb.Head():]

	tx, _ := st1.NewTx([]byte("key"))
	tx.Data = append([]byte{store.TxTypeSet}, data...)
	tx.Sign(kp1)

	vs := []*chord.Vnode{vn1, vn2}

	resp, err := nt1.AppendTx(tx, nil, vs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 2 {
		t.Fatal("incomplete results")
	}

	resp, err = nt1.GetTx([]byte("key"), tx.Hash(), nil, vs...)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Error(r.Err)
		}
	}

	if len(resp) != 2 {
		t.Fatalf("result size mismatch")
	}

	resp, err = nt1.LastTx([]byte("key"), nil, vs...)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Error(r.Err)
		} else {
			rtx := r.Data.(*txlog.Tx)
			if !reflect.DeepEqual(tx.Hash(), rtx.Hash()) {
				t.Error("hash mismatch")
			}
		}
	}

	resp, err = nt1.Stat([]byte("key"), nil, vs...)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range resp {
		if r.Err != nil {
			t.Error(r.Err)
		} else {
			inode := r.Data.(*store.Inode)
			if string(inode.Id) != "key" {
				t.Error("key mismatch")
			}
			if inode.Inline {
				t.Error("should not be inline")
			}
			if len(inode.Blocks) < 1 {
				t.Error("no blocks")
			}
			if inode.Size != 6 {
				t.Error("wrong size")
			}
		}
	}

	if err = nt1.ReplicateTx(vn1, vn3); err != nil {
		t.Fatal(err)
	}

	<-time.After(1 * time.Second)

	tc1 := 0
	st1.IterTx(func(k []byte, txs txlog.TxSlice) error {
		tc1++
		return nil
	})

	tc3 := 0
	st3.IterTx(func(k []byte, txs txlog.TxSlice) error {
		tc3++
		return nil
	})

	if tc1 != tc3 {
		t.Fatal("not all tx replicated")
	}

	if tc1 == 0 || tc3 == 0 {
		t.Fatal("should have tx's")
	}

	ttx1, err := st1.LastTx([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	ttx3, err := st3.LastTx([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(ttx1.Hash(), ttx3.Hash()) {
		t.Fatal("hash mismatch")
	}

	ind, err := st3.Stat([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(ind.Id) != "key" {
		t.Fatal("inode id mismatch")
	}
}
