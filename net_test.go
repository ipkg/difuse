package difuse

import (
	"encoding/json"
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

	ch := make(chan *ReplRequest, 128)
	go func() {
		<-ch
	}()
	nt.RegisterReplicationQ(ch)

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
	nt1.RegisterVnode(vn1, st1)
	nt1.RegisterVnode(vn2, st2)
	nt1.RegisterVnode(vn3, st3)

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
	nt1.RegisterVnode(vn1, st1)
	nt1.RegisterVnode(vn2, st2)
	nt1.RegisterVnode(vn3, st3)

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

	<-time.After(300 * time.Millisecond)
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

	if err = nt1.TransferKeys(vn1, vn3); err != nil {
		t.Fatal(err)
	}

	/*ttx1, err := st1.LastTx([]byte("key"))
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

	mr, err := nt1.MerkleRootTx([]byte("key"), nil, vn1, vn3)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range mr {
		if r.Err != nil {
			t.Error(r.Err)
		}
	}

	mr1 := mr[0].Data.([]byte)
	mr2 := mr[1].Data.([]byte)

	if !txlog.EqualBytes(mr1, mr2) {
		t.Fatal("merkle mismatch")
	}*/
}

func TestVnodeResponseMarshalJSON(t *testing.T) {
	vr := []*VnodeResponse{
		&VnodeResponse{Id: []byte("foo")},
	}
	b, _ := json.Marshal(vr)

	var vr1 []*VnodeResponse
	if err := json.Unmarshal(b, &vr1); err != nil {
		t.Fatal(err)
	}

	if len(vr1) != 1 {
		t.Fatal("wrong length")
	}
	if vr1[0].Data != nil || vr1[0].Err != nil {
		t.Fatal("should be nil")
	}

	vr2 := vr[0]
	vr2.Data = map[string]string{}
	vr2.Err = fmt.Errorf("foo")

	b, _ = json.Marshal(vr2)

	var vr3 VnodeResponse
	json.Unmarshal(b, &vr3)

	if vr3.Data == nil {
		t.Fatal("should have data")
	}
}
