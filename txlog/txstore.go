package txlog

import (
	"fmt"
	"sort"
	"sync"

	merkle "github.com/ipkg/go-merkle"
)

// TxStore persists transactions to data store.
type TxStore interface {
	// New creates a new key with no tx's
	New(key []byte) error
	// Get key for the tx.  This is the object encompassing the tx's for a key.
	GetKey(key []byte) (*TxKey, error)

	Get(key []byte, txhash []byte) (*Tx, error)
	//First(key []byte) (*Tx, error)
	Last(key []byte) (*Tx, error)
	// Returns the merkle root of the transactions for a given key.  If key is nil the
	// merkle root of the complete store should be returned.
	MerkleRoot(key []byte) ([]byte, error)
	// Return all transactions for key from the given seek position.  If seek is nil,
	// all transactions are returned
	Transactions(key, seek []byte) (TxSlice, error)
	// Add a transaction to the store.
	Add(tx *Tx) error

	// Atomically return the mode
	Mode(key []byte) (KeyMode, error)
	// Atomically sets the key mode
	SetMode(key []byte, mode KeyMode) error
	// Iterate over each key - calling f on each key with the key and transaction
	// slice.
	Iter(f func(kt *KeyTransactions) error) error
}

// MemTxStore stores the transaction log
type MemTxStore struct {
	mu sync.RWMutex
	m  map[string]*KeyTransactions
}

// NewMemTxStore initializes a new transaction store
func NewMemTxStore() *MemTxStore {
	return &MemTxStore{
		m: map[string]*KeyTransactions{},
	}
}

// Mode return the mode for the key.
func (mts *MemTxStore) Mode(key []byte) (KeyMode, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if v, ok := mts.m[string(key)]; ok {
		return v.txkey.Mode(), nil
	}

	return -1, ErrKeyNotFound
}

// SetMode sets the mode on a given key.
func (mts *MemTxStore) SetMode(key []byte, m KeyMode) error {
	mts.mu.Lock()
	defer mts.mu.Unlock()

	if v, ok := mts.m[string(key)]; ok {
		return v.txkey.SetMode(m)
	}

	return ErrKeyNotFound
}

func (mts *MemTxStore) GetKey(key []byte) (*TxKey, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if k, ok := mts.m[string(key)]; ok {
		return k.txkey, nil
	}
	return nil, ErrKeyNotFound
}

// Iter iterates over key and associated transactions calling f
// with the key and slice of transactions as arguments.
func (mts *MemTxStore) Iter(f func(*KeyTransactions) error) error {
	//mts.mu.RLock()
	//defer mts.mu.RUnlock()

	var err error
	for _, v := range mts.m {
		if e := f(v); e != nil {
			err = e
		}
	}
	return err
}

// Last returns the last transaction for a key
func (mts *MemTxStore) Last(key []byte) (*Tx, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if txs, ok := mts.m[string(key)]; ok {
		if t := txs.Last(); t != nil {
			return t, nil
		}
	}

	return nil, ErrTxNotFound
}

// MerkleRoot returns the merkle root of the transaction log for a given key. If key is nil
// the merkle root for the whole transaction log is returned.
func (mts *MemTxStore) MerkleRoot(key []byte) ([]byte, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if key == nil {
		// Return merkle root of complete store
		return mts.storeMerkleRoot()
	} else if v, ok := mts.m[string(key)]; ok {
		// Return merkle root for the given key
		return v.txkey.MerkleRoot(), nil
	}

	return nil, ErrKeyNotFound
}

// Transactions returns all transactions for the key starting from the seek point.
func (mts *MemTxStore) Transactions(key, seek []byte) (TxSlice, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	v, ok := mts.m[string(key)]

	if !ok {
		return nil, ErrKeyNotFound
	}

	return v.Transactions(seek)
}

// New creates a new key in the tx store that is offline
func (mts *MemTxStore) New(key []byte) error {
	mts.mu.Lock()
	defer mts.mu.Unlock()

	k := string(key)

	if _, ok := mts.m[k]; ok {
		return fmt.Errorf("key already exists")
	}

	kt := NewKeyTransactions(key)
	kt.txkey.SetMode(OfflineKeyMode)
	mts.m[k] = kt
	return nil
}

// Add adds a transaction to the transaction store. If the key doesnt exist
// it will be created.
func (mts *MemTxStore) Add(tx *Tx) error {
	mts.mu.Lock()
	defer mts.mu.Unlock()

	k := string(tx.Key)

	txs, ok := mts.m[k]
	if !ok {
		txs = NewKeyTransactions(tx.Key)
	}

	if err := txs.AddTx(tx); err != nil {
		return err
	}

	mts.m[k] = txs
	return nil
}

// Get geta a transaction for a given key and associated hash
func (mts *MemTxStore) Get(key []byte, txhash []byte) (*Tx, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if txs, ok := mts.m[string(key)]; ok {
		for _, tx := range txs.TxSlice {
			if EqualBytes(tx.Hash(), txhash) {
				return tx, nil
			}
		}
	}

	return nil, ErrTxNotFound
}

func (mts *MemTxStore) keysMerkleTree() (*merkle.Tree, error) {
	ks := mts.getSortedKeys()
	keys := make([][]byte, len(ks))
	for i, v := range ks {
		keys[i] = []byte(v)
	}

	return merkle.GenerateTree(keys), nil
}

func (mts *MemTxStore) getSortedKeys() []string {
	keys := make([]string, len(mts.m))
	i := 0
	for k := range mts.m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// storeMerkleRoot returns the merkle root for the while txstore.
func (mts *MemTxStore) storeMerkleRoot() ([]byte, error) {
	keys := mts.getSortedKeys()
	klen := len(keys)
	if klen == 0 {
		return ZeroHash(), nil
	}

	mrs := make([][]byte, klen)
	for i, k := range keys {
		var err error
		if mrs[i], err = mts.m[k].MerkleRoot(); err != nil {
			return nil, err
		}
	}

	tree := merkle.GenerateTree(mrs)
	return tree.Root().Hash(), nil
}
