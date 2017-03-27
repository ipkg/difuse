package txlog

import (
	"encoding/hex"
	"errors"
	"sync"
)

var (
	errTxNotFound      = errors.New("tx not found")
	ErrTxBlockNotFound = errors.New("tx block not found")
	ErrTxBlockExists   = errors.New("tx block exists")
)

// TxBlockStore implements a store for transaction blocks.
type TxBlockStore interface {
	// Create a key if it doesn't exist and set the mode
	Create(key []byte, mode TxBlockMode) error
	// Add tx block to store
	Set(tb *TxBlock) error
	// Get key for the tx.  This is the object encompassing the tx's for a key.
	Get(key []byte) (*TxBlock, error)
	// Atomically return the mode
	Mode(key []byte) (TxBlockMode, error)
	// Atomically sets the key mode
	SetMode(key []byte, mode TxBlockMode) error
	// Return a consistent snapshot
	Snapshot() (TxBlockStore, error)
	// Iterator
	Iter(func(*TxBlock) error) error
}

// TxStore implements a store interface for transactions.
type TxStore interface {
	Get(txhash []byte) (*Tx, error)
	Set(tx *Tx) error
}

// MemTxBlockStore implements an in-memory transaction block store
type MemTxBlockStore struct {
	mu sync.RWMutex
	m  map[string]*TxBlock
}

// NewMemTxBlockStore instantiates a new in-memory transaction block store.
func NewMemTxBlockStore() *MemTxBlockStore {
	return &MemTxBlockStore{m: make(map[string]*TxBlock)}
}

// Set sets the given tx block to the store.  If it already exists it returns an error.
func (m *MemTxBlockStore) Set(tb *TxBlock) error {
	k := string(tb.Key())

	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[k] = tb

	return nil
}

// Create creates a new tx block for the key if it doesn't exist and sets the mode on the block.
func (m *MemTxBlockStore) Create(key []byte, mode TxBlockMode) error {
	k := string(key)

	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.m[k]; ok {
		if err := v.SetMode(mode); err != nil {
			return err
		}

		m.m[k] = v
		return nil
	}

	txb := NewTxBlock(key)
	txb.mode = int32(mode)
	m.m[k] = txb

	return nil
}

// Get retrieves a transaction block by the given key.
func (m *MemTxBlockStore) Get(key []byte) (*TxBlock, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if k, ok := m.m[string(key)]; ok {
		return k, nil
	}
	return nil, ErrTxBlockNotFound
}

// Snapshot takes a snapshot of the current store.  It instantiates a new store copying in the existing
// transaction blocks.
func (m *MemTxBlockStore) Snapshot() (TxBlockStore, error) {
	nst := NewMemTxBlockStore()

	m.mu.RLock()
	for k, v := range m.m {
		nst.m[k] = v
	}
	m.mu.RUnlock()

	return nst, nil
}

// Iter iterates over all tx blocks.  This call does not perform any locking and should be used accordling.
func (m *MemTxBlockStore) Iter(f func(*TxBlock) error) error {
	for _, v := range m.m {
		if err := f(v); err != nil {
			return err
		}
	}
	return nil
}

// Mode retrieves the mode of the transaction block
func (m *MemTxBlockStore) Mode(key []byte) (TxBlockMode, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if v, ok := m.m[string(key)]; ok {
		return v.Mode(), nil
	}

	return TxBlockMode(-1), ErrTxBlockNotFound
}

// SetMode sets the mode of a block with the given key.
func (m *MemTxBlockStore) SetMode(key []byte, mode TxBlockMode) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if v, ok := m.m[string(key)]; ok {
		return v.SetMode(mode)
	}

	return ErrTxBlockNotFound
}

// MemTxStore implements an in-memory transaction store
type MemTxStore struct {
	mu sync.RWMutex
	m  map[string]*Tx
}

// NewMemTxStore instantiates a new in-memory transaction store.
func NewMemTxStore() *MemTxStore {
	return &MemTxStore{m: make(map[string]*Tx)}
}

// Get retrieves a transaction by it's id hash from the store.
func (m *MemTxStore) Get(txhash []byte) (*Tx, error) {
	h := hex.EncodeToString(txhash)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if tx, ok := m.m[h]; ok {
		return tx, nil
	}

	return nil, errTxNotFound
}

// Set sets a transaction on the store.  Transaction hash's are assumed to be unique as such, a check
// for the existence of the hash id is not performed.
func (m *MemTxStore) Set(tx *Tx) error {
	h := hex.EncodeToString(tx.Hash())

	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[h] = tx
	return nil
}
