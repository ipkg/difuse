package txlog

import (
	"fmt"
	"testing"
	"time"
)

type testFsm struct {
	t *testing.T
}

func (tf *testFsm) Apply(ktx *Tx) error {
	h := ktx.Hash()
	tf.t.Logf("Applied k='%s' p=%x h=%x", ktx.Key, ktx.PrevHash[:8], h[:8])
	return nil
}

func Test_TxLog(t *testing.T) {
	kp, _ := GenerateECDSAKeypair()
	store := NewMemTxStore()
	fsm := &testFsm{t: t}

	txl := NewTxLog(kp, store, fsm)
	go txl.Start()

	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		ntx, err := txl.NewTx(k)
		if err != nil {
			t.Fatal(err)
		}
		ntx.Data = []byte("value")
		ntx.Sign(kp)
		if err = txl.AppendTx(ntx); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		ntx, err := txl.NewTx(k)
		if err != nil {
			t.Fatal(err)
		}
		ntx.Data = []byte("updated")
		ntx.Sign(kp)

		if err = txl.AppendTx(ntx); err != nil {
			t.Fatal(err)
		}
	}

	gtx, _ := txl.NewTx([]byte("get-test"))
	gtx.Data = []byte("test-value")
	gtx.Sign(kp)
	if err := txl.AppendTx(gtx); err != nil {
		t.Fatal(err)
	}

	<-time.After(1 * time.Second)

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

	txs, err := store.GetAll([]byte("get-test"))
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) < 1 {
		t.Fatal("not tx's")
	}
}
