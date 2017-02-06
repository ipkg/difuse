package txlog

import "github.com/btcsuite/fastsha256"

// Signator is used to sign a transaction
type Signator interface {
	Sign([]byte) (*Signature, error)
	PublicKey() PublicKey
	Verify(pubkey, signature, hash []byte) error
}

// KeyTransactions holds all transactions for a given key
type KeyTransactions struct {
	TxSlice
	root []byte
}

// NewKeyTransactions instances a new KeyTransactions to manages tx's for a key
func NewKeyTransactions() *KeyTransactions {
	return &KeyTransactions{
		TxSlice: TxSlice{},
		root:    ZeroHash(),
	}
}

// Root returns the merkle root of all tx's
func (k *KeyTransactions) Root() []byte {
	return k.root
}

// AddTx adds a transaction for the key and updates the merkle root
func (k *KeyTransactions) AddTx(tx *Tx) (err error) {
	k.TxSlice = append(k.TxSlice, tx)
	k.root, err = k.TxSlice.MerkleRoot()

	return
}

// TxHeader contains header info for a transaction.
type TxHeader struct {
	PrevHash    []byte
	Source      []byte // from pubkey
	Destination []byte // to pubkey
}

// Tx represents a single transaction
type Tx struct {
	*TxHeader
	Signature []byte
	Key       []byte
	Data      []byte
}

// NewTx given the previous tx hash, data and optional public keys
func NewTx(key, prevHash, data []byte) *Tx {
	return &Tx{
		Key: key,
		TxHeader: &TxHeader{
			PrevHash: prevHash,
		},
		Data: data,
	}
}

// DataHash of the tx data
func (tx *Tx) DataHash() []byte {
	s := fastsha256.Sum256(tx.Data)
	return s[:]
}

// bytesToGenHash returns the byte slice that should be used to generate the hash
func (tx *Tx) bytesToGenHash() []byte {
	// key + data hash + previous hash + src pub key + dst pub key
	return concat(tx.Key, tx.DataHash(), tx.PrevHash, tx.Source, tx.Destination)
}

// Hash of the whole Tx
func (tx *Tx) Hash() []byte {
	d := tx.bytesToGenHash()
	s := fastsha256.Sum256(d)
	return s[:]
}

// Sign transaction
func (tx *Tx) Sign(signer Signator) error {
	tx.Source = signer.PublicKey().Bytes()

	sig, err := signer.Sign(tx.Hash())
	if err == nil {
		tx.Signature = sig.Bytes()
	}

	return err
}

// VerifySignature of the transaction
func (tx *Tx) VerifySignature(verifier Signator) error {
	return verifier.Verify(tx.Source, tx.Signature, tx.Hash())
}
