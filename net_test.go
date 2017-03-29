package difuse

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/keypairs"
	"github.com/ipkg/difuse/rpc"
	"github.com/ipkg/difuse/txlog"
	"github.com/ipkg/difuse/types"
)

type testTxlogTrans struct {
	tl *txlog.TxLog
}

func (tb *testTxlogTrans) GetTx(hash []byte, opts types.RequestOptions) (*types.Tx, *types.ResponseMeta, error) {
	return nil, nil, fmt.Errorf("tbi")
}

func (tb *testTxlogTrans) ProposeTx(tx *types.Tx, opts types.RequestOptions) (*types.ResponseMeta, error) {
	var err error
	for i := 0; i < 4; i++ {
		if er := tb.tl.ProposeTx(tx); er != nil {
			err = er
		}
	}
	return nil, err
}

func newGRPCServer(p int) (net.Listener, *grpc.Server, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", p)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	//opt := grpc.CustomCodec(&chord.PayloadCodec{})
	server := grpc.NewServer()
	return ln, server, nil
}

func prepGRPCTransport(p int) (net.Listener, *grpc.Server, *NetTransport, error) {
	ln, server, err := newGRPCServer(p)
	if err != nil {
		return nil, nil, nil, err
	}

	nt := NewNetTransport(ln.Addr().String())
	rpc.RegisterDifuseRPCServer(server, nt)

	return ln, server, nt, nil
}

func TestNetTransportTx(t *testing.T) {
	kp1, _ := keypairs.GenerateECDSAKeypair()

	ln1, svr1, nt1, err := prepGRPCTransport(32324)
	if err != nil {
		t.Fatal(err)
	}

	go svr1.Serve(ln1)

	vn1 := &chord.Vnode{Id: []byte("vnode-1-32324-1"), Host: "127.0.0.1:32324"}
	//vn2 := &chord.Vnode{Id: []byte("vnode-2-32324-2"), Host: "127.0.0.1:32324"}
	//vn3 := &chord.Vnode{Id: []byte("vnode-3-32324-3"), Host: "127.0.0.1:32324"}
	dir, _ := ioutil.TempDir("", "difuse-test")
	defer os.RemoveAll(dir)

	//txs1 := txlog.NewBoltTxStore(dir, vn1.String())
	//if er := txs1.Open(0600); er != nil {
	//t.Fatal(er)
	//}
	txs1 := txlog.NewMemTxStore()

	tt := &testTxlogTrans{}
	st1 := NewVnodeStore(txs1, tt, &DummyFSM{vn: vn1})
	//st1.RegisterTxTransport(tt)
	tt.tl = st1.txl

	//st1 := prepVnodeStore(kp1, vn1, t)
	//st2 := prepVnodeStore(kp1, vn2, t)
	//st3 := prepVnodeStore(kp1, vn3, t)

	nt1.Register(vn1, st1)

	//nt1.RegisterVnode(vn2, st2)
	//nt1.RegisterVnode(vn3, st3)

	testkey := []byte("key")

	tx, _ := nt1.NewTx(vn1, testkey)
	tx.Data = append([]byte{0}, []byte("value")...)
	tx.Sign(kp1)

	if err = nt1.ProposeTx(vn1, tx); err != nil {
		t.Fatal(err)
	}

	<-time.After(300 * time.Millisecond)

	if _, err = nt1.GetTx(vn1, tx.Hash()); err != nil {
		t.Fatal(err)
	}

	/*blk, err := nt1.GetTxBlock(vn1, testkey)
	if err != nil {
		t.Fatal(err)
	}

	if len(blk.TxIds()) == 0 {
		t.Fatal("no tx's")
	}*/

}
