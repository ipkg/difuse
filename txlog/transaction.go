package txlog

// KeyTransactions holds all transactions for a given key
type KeyTransactions struct {
	txkey *TxKey
	TxSlice
}

// NewKeyTransactions instances a new KeyTransactions to manages tx's for a key
func NewKeyTransactions(key []byte) *KeyTransactions {
	return &KeyTransactions{
		txkey:   NewTxKey(key),
		TxSlice: TxSlice{},
	}
}

func (k *KeyTransactions) SetMode(m KeyMode) error {
	return k.txkey.SetMode(m)
}

func (k *KeyTransactions) Key() []byte {
	return k.txkey.Key()
}

// Transactions returns all transactions beginning at the seek hash.  If seek is nil
// all transactions are returned.
func (k *KeyTransactions) Transactions(seek []byte) (TxSlice, error) {
	if seek == nil || IsZeroHash(seek) || len(k.TxSlice) == 0 {
		return k.TxSlice, nil
	} else if EqualBytes(k.Last().Hash(), seek) {
		return TxSlice{}, nil
	}

	for i, v := range k.TxSlice {
		if EqualBytes(v.Hash(), seek) {
			return k.TxSlice[i:], nil
		}
	}

	return nil, ErrTxNotFound
}

// AddTx adds a transaction for the key and updates the merkle root
func (k *KeyTransactions) AddTx(tx *Tx) (err error) {
	k.TxSlice = append(k.TxSlice, tx)
	k.txkey.AppendTx(tx.Hash())
	return
}

/*func (k *KeyTransactions) NewTx() (*Tx, error) {
	if atomic.LoadInt32(&k.mode) == TakeoverKeyMode {
		return nil, ErrInTakeOverMode
	}

	if ltx := k.Last(); ltx != nil {
		return NewTx(k.key, ltx.Hash(), nil), nil
	}
	return NewTx(k.key, ZeroHash(), nil), nil
}*/
