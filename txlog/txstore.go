package txlog

import "sync"

// TxStore persists transactions to data store.
type TxStore interface {
	Get(key []byte, txhash []byte) (*Tx, error)
	GetAll(key []byte) ([]*Tx, error)
	First(key []byte) (*Tx, error)
	Last(key []byte) (*Tx, error)
	Length(key []byte) int
	// MerkleRoot of the transactions for the given key
	MerkleRoot(key []byte) ([]byte, error)

	Add(tx *Tx) error
	// Iterate over each key - TxSlice pair
	Iter(func([]byte, TxSlice) error) error
}

// MemTxStore stores the transaction log
type MemTxStore struct {
	mu sync.RWMutex
	m  map[string]TxSlice
}

// NewMemTxStore initializes a new transaction store
func NewMemTxStore() *MemTxStore {
	return &MemTxStore{
		m: map[string]TxSlice{},
	}
}

// Iter iterates over keys and associated transactions calling the specified function
// with the key and slice of transactions
func (mts *MemTxStore) Iter(f func(k []byte, txs TxSlice) error) error {
	var err error

	for k, v := range mts.m {
		if e := f([]byte(k), v); e != nil {
			err = e
		}
	}

	return err
}

// GetAll gets all transactions for a given key
func (mts *MemTxStore) GetAll(key []byte) ([]*Tx, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if v, ok := mts.m[string(key)]; ok {
		return v, nil
	}
	return nil, errNotFound
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

	return nil, errNotFound
}

// Length returns the number of transactions in the store for a given key.
func (mts *MemTxStore) Length(key []byte) int {
	if v, ok := mts.m[string(key)]; ok {
		return len(v)
	}
	return -1
}

// MerkleRoot returns the merkle root of the transaction log for a given key.
func (mts *MemTxStore) MerkleRoot(key []byte) ([]byte, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if v, ok := mts.m[string(key)]; ok {
		return v.MerkleRoot()
	}

	return nil, errNotFound
}

// First tx for a key
func (mts *MemTxStore) First(key []byte) (*Tx, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if txs, ok := mts.m[string(key)]; ok {
		if t := txs.First(); t != nil {
			return t, nil
		}
	}

	return nil, errNotFound
}

// Add adds a transaction the to the transaction store.
func (mts *MemTxStore) Add(tx *Tx) error {
	mts.mu.Lock()
	defer mts.mu.Unlock()

	txs, ok := mts.m[string(tx.Key)]
	if !ok {
		txs = TxSlice{}
	}

	//mts.ltx = tx
	txs = append(txs, tx)
	mts.m[string(tx.Key)] = txs
	return nil
}

/*// Update a transaction for a given key
func (mts *MemTxStore) Update(tx *Tx) error {
	mts.mu.Lock()
	defer mts.mu.Unlock()

	txs, ok := mts.m[string(tx.Key)]
	if !ok {
		return errNotFound
	}

	mts.ltx = tx
	txs = append(txs, tx)
	mts.m[string(tx.Key)] = txs

	return nil
}*/

// Get a tx for a given key by the hash
func (mts *MemTxStore) Get(key []byte, txhash []byte) (*Tx, error) {
	txs, ok := mts.m[string(key)]
	if !ok {
		return nil, errNotFound
	}
	for _, tx := range txs {
		if EqualBytes(tx.Hash(), txhash) {
			return tx, nil
		}
	}

	return nil, errNotFound
}
