package types

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/btcsuite/fastsha256"
	"github.com/ipkg/difuse/keypairs"
	"github.com/ipkg/difuse/utils"
)

// Signator is used to sign a transaction
type Signator interface {
	Sign([]byte) (*keypairs.Signature, error)
	PublicKey() keypairs.PublicKey
	Verify(pubkey, signature, hash []byte) error
}

// NewTx given the previous tx hash, data and optional public keys
func NewTx(key, prevHash, data []byte) *Tx {
	tx := &Tx{
		Key: key,
		Header: &TxHeader{
			PrevHash:  prevHash,
			Timestamp: uint64(time.Now().UnixNano()),
		},
		Data: data,
	}
	if utils.IsZeroHash(prevHash) {
		tx.Header.Height = 1
	}
	return tx
}

// DataHash of the tx data
func (tx *Tx) DataHash() []byte {
	s := fastsha256.Sum256(tx.Data)
	return s[:]
}

// bytesToGenHash returns the byte slice that should be used to generate the hash
func (tx *Tx) bytesToGenHash() []byte {
	// key + data hash + previous hash + src pub key + dst pub key
	return concat(tx.Key, tx.DataHash(), tx.Header.PrevHash, tx.Header.Source, tx.Header.Destination)
}

// Hash of the whole Tx
func (tx *Tx) Hash() []byte {
	d := tx.bytesToGenHash()
	s := fastsha256.Sum256(d)
	return s[:]
}

// Sign transaction
func (tx *Tx) Sign(signer Signator) error {
	tx.Header.Source = signer.PublicKey().Bytes()

	sig, err := signer.Sign(tx.Hash())
	if err == nil {
		tx.Signature = sig.Bytes()
	}

	return err
}

// VerifySignature of the transaction
func (tx *Tx) VerifySignature(verifier Signator) error {
	return verifier.Verify(tx.Header.Source, tx.Signature, tx.Hash())
}

// MarshalJSON is a custom JSON marshaller.  It properly formats the hashes
// and includes everything except the transaction data.
func (tx *Tx) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"prev":      hex.EncodeToString(tx.Header.PrevHash),
		"id":        hex.EncodeToString(tx.Hash()),
		"key":       string(tx.Key),
		"timestamp": tx.Header.Timestamp,
		"height":    tx.Header.Height,
	})
}

func concat(pieces ...[]byte) []byte {
	sz := 0
	for _, p := range pieces {
		sz += len(p)
	}

	buf := make([]byte, sz)

	i := 0
	for _, p := range pieces {
		copy(buf[i:], p)
		i += len(p)
	}
	return buf
}
