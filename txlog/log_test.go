package txlog

import (
	"fmt"
	"testing"
	"time"

	"github.com/ipkg/difuse/keypairs"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

type testBroadcast struct {
	tl *TxLog
}

//func (tb *testBroadcast) Register(vn *chord.Vnode, st TxStore) {}

func (tb *testBroadcast) GetTx(id []byte, opts utils.RequestOptions) (*Tx, *utils.ResponseMeta, error) {
	return nil, nil, fmt.Errorf("TBI")
}

func (tb *testBroadcast) ProposeTx(tx *Tx, opts utils.RequestOptions) (*utils.ResponseMeta, error) {
	var err error
	for i := 0; i < 3; i++ {
		if er := tb.tl.ProposeTx(tx); er != nil {
			err = er
		}
	}
	return nil, err
}

type testFsm struct {
	t  *testing.T
	vn *chord.Vnode
}

func (tf *testFsm) Vnode() *chord.Vnode {
	return tf.vn
}

func (tf *testFsm) Apply(ktx *Tx) error {
	h := ktx.Hash()
	tf.t.Logf("Applied k='%s' p=%x h=%x", ktx.Key, ktx.PrevHash[:8], h[:8])
	return nil
}

func Test_TxLog(t *testing.T) {
	kp, _ := keypairs.GenerateECDSAKeypair()
	txstore := NewMemTxStore()
	tbstore := NewMemTxBlockStore()
	fsm := &testFsm{t: t, vn: &chord.Vnode{Id: []byte("foobarbaz"), Host: "localhost"}}
	trans := &testBroadcast{}

	txl := NewTxLog(kp, tbstore, txstore, trans, fsm)
	trans.tl = txl

	txl.Start()

	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		ntx, err := txl.NewTx(k)
		if err != nil {
			t.Fatal(err)
		}
		ntx.Data = []byte("value")
		ntx.Sign(kp)
		if err = txl.ProposeTx(ntx); err != nil {
			t.Error(err)
		}
	}

	<-time.After(500 * time.Millisecond)

	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		ntx, err := txl.NewTx(k)
		if err != nil {
			t.Fatal(err)
		}
		ntx.Data = []byte("updated")
		ntx.Sign(kp)

		if err := txl.ProposeTx(ntx); err != nil {
			t.Error(err)
		}
	}

	//<-time.After(500 * time.Millisecond)

	tb, err := txl.tbstore.Get([]byte("key-2"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tb.txs) == 0 {
		t.Error("tx not written")
	}

	gtx, err := txl.NewTx([]byte("get-test"))
	if err != nil {
		t.Fatal(err)
	}
	gtx.Data = []byte("test-value")
	gtx.Sign(kp)
	txl.ProposeTx(gtx)

	<-time.After(1 * time.Second)

	tx, err := txstore.Get(gtx.Hash())
	if err != nil {
		t.Fatal(err)
	}

	if !utils.EqualBytes(tx.Hash(), gtx.Hash()) {
		t.Fatal("tx hash mismatch")
	}

	if !utils.EqualBytes(tx.Data, gtx.Data) {
		t.Fatal("tx data mismatch")
	}

	// Test snapshot
	ntbs, err := txl.tbstore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	s1 := txl.tbstore.(*MemTxBlockStore)
	s2 := ntbs.(*MemTxBlockStore)

	if len(s1.m) != len(s2.m) {
		t.Error("snapshot failed")
	}
}
