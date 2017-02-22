package txlog

type KeyMode int32

func (k KeyMode) String() string {
	switch k {
	case NormalKeyMode:
		return "normal"
	case TransitionKeyMode:
		return "transition"
	case TakeoverKeyMode:
		return "takeover"
	case OfflineKeyMode:
		return "offline"
	}
	return "unknown"
}

const (
	// NormalKeyMode is the normal mode of operation
	NormalKeyMode KeyMode = iota
	// TransitionKeyMode is the key is being transferred by a vnode.  This is set on the vnode
	// transitioning its keys to be taken over.
	TransitionKeyMode
	// TakeoverKeyMode is the key being taken-over by a vnode.  This mode is set on the vnode
	// receiving the keys.
	TakeoverKeyMode
	// OfflineKeyMode denotes that a key is offline and not usable.  This is set if a key
	// is not consistent.
	OfflineKeyMode
)

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
