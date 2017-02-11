package txlog

import (
	"fmt"
	"testing"
)

func prepTxStore(txcount int) *MemTxStore {
	//kp, _ := GenerateECDSAKeypair()
	st := NewMemTxStore()
	for i := 0; i < txcount; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		tx := NewTx(key, nil, []byte(fmt.Sprintf("data%d", i)))
		//tx.Sign(kp)
		st.Add(tx)
	}
	return st
}

func TestTxStoreMerkleRoot(t *testing.T) {
	st1 := prepTxStore(5)
	st2 := prepTxStore(5)

	mr1, er1 := st1.MerkleRoot([]byte("key0"))
	if er1 != nil {
		t.Fatal(er1)
	}
	mr2, er2 := st2.MerkleRoot([]byte("key0"))
	if er2 != nil {
		t.Fatal(er1)
	}

	if !EqualBytes(mr1, mr2) {
		t.Fatalf("merkle root mismatch: %x!=%x", mr1, mr2)
	}

	if EqualBytes(mr1, ZeroHash()) {
		t.Fatal("merkle should not be zero")
	}

	mr1, er1 = st1.MerkleRoot(nil)
	if er1 != nil {
		t.Fatal(er1)
	}
	mr2, er2 = st2.MerkleRoot(nil)
	if er2 != nil {
		t.Fatal(er1)
	}

	if !EqualBytes(mr1, mr2) {
		t.Fatalf("merkle root mismatch: %x!=%x", mr1, mr2)
	}

}

func TestTxStoreMerkle(t *testing.T) {
	st1 := prepTxStore(50)
	st2 := prepTxStore(34)

	kmt1, err := st1.keysMerkleTree()
	if err != nil {
		t.Fatal(err)
	}
	///t.Log(kmt1.Height())

	kmt2, err := st2.keysMerkleTree()
	if err != nil {
		t.Fatal(err)
	}

	if EqualBytes(kmt1.Root().Hash(), kmt2.Root().Hash()) {
		t.Fatal("root hashes should not match")
	}

	ts, err := st1.Transactions([]byte("key10"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) == 0 {
		t.Error("No transactions")
	}

	var cnt int
	st2.Iter(func(k []byte, kt *KeyTransactions) error {
		cnt++
		return nil
	})
	if cnt == 0 {
		t.Fatal("iter 0")
	}
	/*n := findLevel(&kmt1, &kmt2)
	if n < 0 {
		t.Fatal("not found")
	}

	//nodes := kmt1.GetNodesAtHeight(uint64(n))

	dumpTree(&kmt1)
	dumpTree(&kmt2)*/
}

/*func dumpTree(t *merkle.Tree) {
	fmt.Println("TREE")
	h := t.Height()
	for i := uint64(0); i < h; i++ {
		fmt.Printf("Level: %d\n", i)
		level := t.Levels[i]
		for _, l := range level {
			fmt.Printf("  %x\n", l.Hash)
		}
	}
}

// return the level no. from t1 where match was found
func findLevel(t1, t2 *merkle.Tree) int64 {
	h1 := t1.Height()
	slevel := uint64(h1 - t2.Height())

	j := 0
	for i := slevel; i < h1; i++ {

		if EqualBytes(t1.Levels[i][0].Hash, t2.Levels[j][0].Hash) {
			return int64(i)
		}

		j++
	}

	return -1
}*/
