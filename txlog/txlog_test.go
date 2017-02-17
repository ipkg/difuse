package txlog

import (
	"fmt"
	"testing"
	"time"

	chord "github.com/ipkg/go-chord"
)

type testBroadcast struct {
	tl *TxLog
}

func (tb *testBroadcast) BroadcastTx(tx *Tx, vn *chord.Vnode) error {
	var err error
	for i := 0; i < 3; i++ {
		if er := tb.tl.ProposeTx(tx); er != nil {
			err = er
		}
	}
	return err
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
	kp, _ := GenerateECDSAKeypair()
	store := NewMemTxStore()
	fsm := &testFsm{t: t, vn: &chord.Vnode{Id: []byte("foobarbaz"), Host: "localhost"}}

	trans := &testBroadcast{}
	txl := NewTxLog(kp, store, trans, fsm)
	trans.tl = txl
	go txl.Start()

	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		ntx, err := txl.NewTx(k)
		if err != nil {
			t.Fatal(err)
		}
		ntx.Data = []byte("value")
		ntx.Sign(kp)
		txl.ProposeTx(ntx)
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

		txl.ProposeTx(ntx)
	}

	txs, _ := txl.store.Transactions([]byte("key-2"), nil)
	if len(txs) == 0 {
		t.Error("tx not written")
	}

	gtx, err := txl.NewTx([]byte("get-test"))
	if err != nil {
		t.Fatal(err)
	}
	gtx.Data = []byte("test-value")
	gtx.Sign(kp)
	txl.ProposeTx(gtx)

	<-time.After(2 * time.Second)

	tx, err := store.Get([]byte("get-test"), gtx.Hash())
	if err != nil {
		t.Fatal(err)
	}

	if !EqualBytes(tx.Hash(), gtx.Hash()) {
		t.Fatal("tx hash mismatch")
	}

	if !EqualBytes(tx.Data, gtx.Data) {
		t.Fatal("tx data mismatch")
	}

	/*txs, err := store.GetAll([]byte("get-test"))
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) < 1 {
		t.Fatal("not tx's")
	}*/
}
