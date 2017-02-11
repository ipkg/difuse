package txlog

// KeyTransactions holds all transactions for a given key
type KeyTransactions struct {
	txs  TxSlice
	root []byte
}

// NewKeyTransactions instances a new KeyTransactions to manages tx's for a key
func NewKeyTransactions() *KeyTransactions {
	return &KeyTransactions{
		txs:  TxSlice{},
		root: ZeroHash(),
	}
}

// Root returns the merkle root of all tx's
func (k *KeyTransactions) Root() []byte {
	return k.root
}

// Transactions returns all transactions
func (k *KeyTransactions) Transactions(seek []byte) (TxSlice, error) {
	if seek == nil {
		return k.txs, nil
	}

	for i, v := range k.txs {
		if EqualBytes(v.Hash(), seek) {
			return k.txs[i:], nil
		}
	}

	return nil, errNotFound
}

// AddTx adds a transaction for the key and updates the merkle root
func (k *KeyTransactions) AddTx(tx *Tx) (err error) {
	k.txs = append(k.txs, tx)
	k.root, err = k.txs.MerkleRoot()

	return
}
