package txlog

import "sync"

// TxStore persists transactions to data store.
type TxStore interface {
	Get(key []byte, txhash []byte) (*Tx, error)
	First(key []byte) (*Tx, error)
	Last(key []byte) (*Tx, error)

	// MerkleRoot of the transactions for the given key
	MerkleRoot(key []byte) ([]byte, error)

	Add(tx *Tx) error
	// Iterate over each key - TxSlice pair
	Iter(func([]byte, *KeyTransactions) error) error
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

// Iter iterates over keys and associated transactions calling the specified function
// with the key and slice of transactions
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
		if t := txs.Last(); t != nil {
			return t, nil
		}
	}

	return nil, errNotFound
}

// MerkleRoot returns the merkle root of the transaction log for a given key.
func (mts *MemTxStore) MerkleRoot(key []byte) ([]byte, error) {
	mts.mu.RLock()
	defer mts.mu.RUnlock()

	if v, ok := mts.m[string(key)]; ok {
		return v.Root(), nil
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
		txs = NewKeyTransactions()
	}

	if err := txs.AddTx(tx); err != nil {
		return err
	}

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
	for _, tx := range txs.TxSlice {
		if EqualBytes(tx.Hash(), txhash) {
			return tx, nil
		}
	}

	return nil, errNotFound
}
