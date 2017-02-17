package txlog

import (
	"sort"
	"sync"

	merkle "github.com/ipkg/go-merkle"
)

// TxStore persists transactions to data store.
type TxStore interface {
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
	// Iterate over each key - calling f on each key with the key and transaction
	// slice.
	Iter(f func([]byte, *KeyTransactions) error) error
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

// Iter iterates over key and associated transactions calling f
// with the key and slice of transactions as arguments.
func (mts *MemTxStore) Iter(f func(k []byte, kt *KeyTransactions) error) error {
	var err error
	for k, v := range mts.m {
		if e := f([]byte(k), v); e != nil {
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
		if t := txs.txs.Last(); t != nil {
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
		return v.Root(), nil
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

// Add adds a transaction to the transaction store.
func (mts *MemTxStore) Add(tx *Tx) error {
	mts.mu.Lock()
	defer mts.mu.Unlock()

	txs, ok := mts.m[string(tx.Key)]
	if !ok {
		txs = NewKeyTransactions()
	}

	if err := txs.AddTx(tx); err != nil {
		return err
	}

	mts.m[string(tx.Key)] = txs
	return nil
}

// Get geta a transaction for a given key and associated hash
func (mts *MemTxStore) Get(key []byte, txhash []byte) (*Tx, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if txs, ok := mts.m[string(key)]; ok {
		for _, tx := range txs.txs {
			if EqualBytes(tx.Hash(), txhash) {
				return tx, nil
			}
		}
	}

	return nil, ErrNotFound
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
		if mrs[i], err = mts.m[k].txs.MerkleRoot(); err != nil {
			return nil, err
		}
	}

	tree := merkle.GenerateTree(mrs)
	return tree.Root().Hash(), nil
}
