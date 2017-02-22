package txlog

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/btcsuite/fastsha256"
)

// Signator is used to sign a transaction
type Signator interface {
	Sign([]byte) (*Signature, error)
	PublicKey() PublicKey
	Verify(pubkey, signature, hash []byte) error
}

// TxHeader contains header info for a transaction.
type TxHeader struct {
	PrevHash    []byte
	Source      []byte // from pubkey
	Destination []byte // to pubkey
	Timestamp   uint64 // Timestamp when tx was created
}

// Tx represents a single transaction
type Tx struct {
	*TxHeader
	Signature []byte
	Key       []byte
	Data      []byte
}

// MarshalJSON is a custom JSON marshaller for a tx.  It properly formats the hashes
// and includes everything except the tx data.
func (tx *Tx) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"prev":      hex.EncodeToString(tx.PrevHash),
		"id":        hex.EncodeToString(tx.Hash()),
		"key":       string(tx.Key),
		"timestamp": tx.Timestamp,
	})
}

// NewTx given the previous tx hash, data and optional public keys
func NewTx(key, prevHash, data []byte) *Tx {
	return &Tx{
		Key: key,
		TxHeader: &TxHeader{
			PrevHash:  prevHash,
			Timestamp: uint64(time.Now().UnixNano()),
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
