package txlog

import (
	"reflect"
	"testing"
)

func Test_TxSlice(t *testing.T) {
	txs := genSlices()
	b, err := txs.MerkleRoot()
	if err != nil {
		t.Fatal(err)
	}

	if reflect.DeepEqual(ZeroHash(), b) {
		t.Fatal("should be non zero hash")
	}

	if !txs.Exists(txs[0]) {
		t.Error("should exist")
	}

	tx := NewTx([]byte("key"), ZeroHash(), []byte("value"))
	if txs.Exists(tx) {
		t.Error("should not exist")
	}

	ftx := txs.First()
	if !EqualBytes(ftx.Hash(), txs[0].Hash()) {
		t.Error("first tx mismatch")
	}

	txs = TxSlice{}
	b, err = txs.MerkleRoot()
	if err != nil {
		t.Fatal(err)
	}
	if !EqualBytes(ZeroHash(), b) {
		t.Fatal("should be zero hash")
	}

}
