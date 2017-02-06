package store

import (
	"compress/zlib"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/btcsuite/fastsha256"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/fbtypes"
	"github.com/ipkg/difuse/txlog"
)

// MemLoggedStore is a tx log back store
type MemLoggedStore struct {
	*MemDataStore

	txstore txlog.TxStore
	txl     *txlog.TxLog
}

// NewMemLoggedStore instantiates a new tx log back in memory store.
func NewMemLoggedStore(vn *chord.Vnode, kp txlog.Signator) *MemLoggedStore {
	mls := &MemLoggedStore{
		MemDataStore: NewMemDataStore(vn),
		txstore:      txlog.NewMemTxStore(),
	}

	mls.txl = txlog.NewTxLog(kp, mls.txstore, mls)
	go mls.txl.Start()

	return mls
}

// Apply a given transaction to the stable store
func (mem *MemLoggedStore) Apply(ktx *txlog.Tx) error {
	txType := ktx.Data[0]

	//log.Printf("Apply key='%s' vn=%s/%x type=%x size=%d", ktx.Key, mem.vn.Host, mem.vn.Id[:7], txType, len(ktx.Data[1:]))

	switch txType {
	case TxTypeSet:
		return mem.applySetKey(ktx.Key, ktx.Data[1:])

	case TxTypeDelete:
		return mem.applyDeleteKey(ktx.Key)

	default:
		return errInvalidTxType
	}
}

// setKey sets transaction based data
func (mem *MemLoggedStore) applySetKey(key, value []byte) error {
	rk := &Inode{}

	ind := fbtypes.GetRootAsInode(value, 0)
	rk.Deserialize(ind)

	// Set the merkle root of all tx's for this key. This is based on the local
	// store and should line up on every node if consistency is met.
	mr, err := mem.txstore.MerkleRoot(key)
	if err != nil {
		return err
	}
	rk.txroot = mr

	mem.tlock.Lock()
	mem.txm[string(key)] = rk
	mem.tlock.Unlock()

	return nil
}

// delete a key only leaving the underlying blocks intact.
func (mem *MemDataStore) applyDeleteKey(key []byte) error {
	k := string(key)

	_, ok := mem.txm[k]
	if !ok {
		return errKeyNotFound
	}

	mem.tlock.Lock()
	delete(mem.txm, k)
	mem.tlock.Unlock()
	return nil
}

// MerkleRootTx returns the merkle root of all transactions for a given key
func (mem *MemLoggedStore) MerkleRootTx(key []byte) ([]byte, error) {
	return mem.txstore.MerkleRoot(key)
}

// GetTx gets a transaction from the store
func (mem *MemLoggedStore) GetTx(key, txhash []byte) (*txlog.Tx, error) {
	return mem.txstore.Get(key, txhash)
}

// IterTx iterates over all transactions in the store.
func (mem *MemLoggedStore) IterTx(f func([]byte, *txlog.KeyTransactions) error) error {
	return mem.txstore.Iter(f)
}

// AppendTx appends/queues a transaction to the log
func (mem *MemLoggedStore) AppendTx(tx *txlog.Tx) error {
	return mem.txl.AppendTx(tx)
}

// NewTx creates a new transaction based on the previous hash from the log
func (mem *MemLoggedStore) NewTx(key []byte) (*txlog.Tx, error) {
	return mem.txl.NewTx(key)
}

// LastTx returns the last transaction in the log.
func (mem *MemLoggedStore) LastTx(key []byte) (*txlog.Tx, error) {
	return mem.txl.LastTx(key)
}

// MemDataStore is an in-memory datastore
type MemDataStore struct {
	// transactional store state
	tlock sync.Mutex
	txm   map[string]*Inode

	// content addressable store
	clock sync.RWMutex
	cad   map[string][]byte

	vn *chord.Vnode
}

func NewMemDataStore(vn *chord.Vnode) *MemDataStore {
	return &MemDataStore{
		txm: map[string]*Inode{},
		cad: map[string][]byte{},
		vn:  vn,
	}
}

// IterInodes iterates over all the inodes
func (ms *MemDataStore) IterInodes(f func([]byte, *Inode) error) error {
	var err error
	for k, v := range ms.txm {
		if e := f([]byte(k), v); e != nil {
			err = e
		}
	}
	return err
}

// Stat returns the inode for the given key/id
func (ms *MemDataStore) Stat(key []byte) (*Inode, error) {
	k := string(key)

	rk, ok := ms.txm[k]
	if ok {
		return rk, nil
	}

	return nil, errKeyNotFound
}

func (ms *MemDataStore) IterBlocks(f func(k, v []byte) error) error {
	var e error
	for k, v := range ms.cad {
		kb, err := hex.DecodeString(k)
		if err != nil {
			e = err
			continue
		}

		if err := f(kb, v); err != nil {
			return err
		}
	}
	return e
}

// GetBlock gets a block by it's content hash signified by key
func (ms *MemDataStore) GetBlock(key []byte) ([]byte, error) {
	k := fmt.Sprintf("%x", key)

	if v, ok := ms.cad[k]; ok {
		return v, nil
	}
	return nil, errBlockNotFound
}

// SetBlock sets the given value and returns the key hash.  This is used to directly
// set kv data and useful for content addressable storage.
func (ms *MemDataStore) SetBlock(value []byte) ([]byte, error) {
	sh := fastsha256.Sum256(value)
	k := fmt.Sprintf("%x", sh)

	ms.clock.Lock()
	defer ms.clock.Unlock()

	// return hash if we already have the block
	if _, ok := ms.cad[k]; ok {
		return sh[:], nil
	}

	ms.cad[k] = value
	return sh[:], nil
}

// DeleteBlock is used to directly delete content addressable data
func (ms *MemDataStore) DeleteBlock(key []byte) error {
	k := fmt.Sprintf("%x", key)

	ms.clock.Lock()
	defer ms.clock.Unlock()

	if _, ok := ms.cad[k]; ok {
		delete(ms.cad, k)
		return nil
	}
	return errBlockNotFound
}

func (ms *MemDataStore) Restore(r io.Reader) error {

	r, err := zlib.NewReader(r)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(r)

	var tc map[string][]byte
	if err = dec.Decode(&tc); err != nil {
		return err
	}

	ms.clock.Lock()
	for k, v := range tc {
		ms.cad[k] = v
	}
	ms.clock.Unlock()

	var tm map[string]*Inode
	if err = dec.Decode(&tm); err != nil {
		return err
	}

	ms.tlock.Lock()
	for k, v := range tm {
		hv, ok := ms.txm[k]
		if !ok {
			ms.txm[k] = v
			continue
		}
		log.Printf("TODO: check hash of '%v' and '%v'", hv, v)
	}
	ms.tlock.Unlock()

	log.Printf("Restored %s keys=%d objects=%d", ms.vn.String(), len(tm), len(tc))

	return nil
}

// Snapshot creates a snapshot in temp space encoding objects then the indoes and
// returns the handle to the snapshot.
func (ms *MemDataStore) Snapshot() (io.ReadCloser, error) {
	tfile, err := ioutil.TempFile("", ms.vn.String()+".")
	if err != nil {
		return nil, err
	}
	// compress the whole set
	wz := zlib.NewWriter(tfile)
	enc := gob.NewEncoder(wz)

	// Encode objects
	if len(ms.cad) > 0 {
		ms.clock.Lock()
		err = enc.Encode(ms.cad)
		ms.clock.Unlock()
	} else {
		err = enc.Encode(map[string][]byte{})
	}

	if err != nil {
		tfile.Close()
		os.Remove(tfile.Name())
		return nil, err
	}

	// Encode inodes
	if len(ms.txm) > 0 {
		ms.tlock.Lock()
		err = enc.Encode(ms.txm)
		ms.tlock.Lock()
	} else {
		err = enc.Encode(map[string]*Inode{})
	}

	if err != nil {
		tfile.Close()
		os.Remove(tfile.Name())
		return nil, err
	}

	log.Printf("Snapshotted: %s keys=%d objects=%d", ms.vn, len(ms.txm), len(ms.cad))
	// Close zlib
	if err = wz.Close(); err != nil {

		tfile.Close()
		os.Remove(tfile.Name())
		return nil, err
	}
	// close to flush,sync,persist
	tfile.Close()
	// open the snapshot and return
	return os.Open(tfile.Name())
}
