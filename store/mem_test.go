package store

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/txlog"
)

var testVn = &chord.Vnode{Id: []byte("test-vnode-id"), Host: "host"}

func prepStore() (*MemLoggedStore, txlog.Signator) {
	kp, _ := txlog.GenerateECDSAKeypair()
	return NewMemLoggedStore(testVn, kp), kp
}

func TestStoreNotFound(t *testing.T) {
	dst, _ := prepStore()

	if _, err := dst.GetBlock([]byte("foo")); err == nil {
		t.Fatal("should fail")
	}
}

func TestStore(t *testing.T) {
	dst, kp := prepStore()

	//dt := dst.TxLog
	ntx, _ := dst.NewTx([]byte("key"))
	rk := NewInodeFromData([]byte("key"), []byte("value"))
	fb := flatbuffers.NewBuilder(0)
	ofs := rk.Serialize(fb)
	fb.Finish(ofs)
	buf := fb.Bytes[fb.Head():]

	ntx.Data = append([]byte{TxTypeSet}, buf...)

	ntx.Sign(kp)
	if err := dst.AppendTx(ntx); err != nil {
		t.Fatal(err)
	}
	// give tx time to process
	<-time.After(50 * time.Millisecond)

	val, err := dst.Stat([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if len(val.Blocks) == 0 {
		t.Fatal("blockhash empty")
	}

	if _, err = dst.GetTx([]byte("key"), ntx.Hash()); err != nil {
		t.Fatal(err)
	}

	bsh1, err := dst.SetBlock([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	// cache
	bsh2, err := dst.SetBlock([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(bsh1, bsh2) {
		t.Fatal("double set failed")
	}

	blk, err := dst.GetBlock(val.Blocks[0])
	if err != nil {
		t.Fatal(err)
	}

	csh := sha256.Sum256([]byte("value"))
	vsh := sha256.Sum256(blk)

	if !reflect.DeepEqual(csh[:], vsh[:]) {
		t.Fatal("hash mismatch")
	}

	if !reflect.DeepEqual(csh[:], val.Blocks[0]) {
		t.Fatalf("hash mismatch")
	}

	ntx2, _ := dst.NewTx([]byte("key"))
	ntx2.Data = []byte{TxTypeDelete}
	ntx2.Sign(kp)
	dst.AppendTx(ntx2)
	// give tx time to process
	<-time.After(50 * time.Millisecond)

	if _, err = dst.Stat([]byte("key")); err == nil {
		t.Fatal("should fail")
	}

	rc, err := dst.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	if rc == nil {
		t.Fatal("should have snapshot data")
	}

	tst, _ := prepStore()
	if err = tst.Restore(rc); err != nil {
		t.Fatal(err)
	}
	rc.Close()

	if len(tst.txm) == 0 && len(tst.cad) == 0 {
		t.Fatal("restore failed")
	}

	if err = dst.DeleteBlock(bsh1); err != nil {
		t.Error(err)
	}
	if _, err = dst.GetBlock(bsh1); err == nil {
		t.Fatal("should fail")
	}
}

func TestInode(t *testing.T) {
	inode := NewInodeFromData([]byte("key"), []byte("data"))
	if _, err := json.Marshal(inode); err != nil {
		t.Fatal(err)
	}
	if len(inode.txroot) == 0 {
		t.Fatal("txroot not set")
	}

	if !txlog.EqualBytes(txlog.ZeroHash(), inode.TxRoot()) {
		t.Fatal("txroot should be zero")
	}
}
