package txlog

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"

	"github.com/tv42/base58"
)

const ecdsaKeySize = 28

// PublicKey represents a public key to obtain the byte encoding
type PublicKey interface {
	Bytes() []byte
}

// ECDSAKeypair is used to satisfy the keypair interface
type ECDSAKeypair struct {
	PrivateKey *ecdsa.PrivateKey
}

// GenerateECDSAKeypair generates a new keypair
func GenerateECDSAKeypair() (*ECDSAKeypair, error) {
	pk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err == nil {
		return &ECDSAKeypair{PrivateKey: pk}, nil
	}
	return nil, err
}

// Sign data returning the 2 signatures
func (ekp *ECDSAKeypair) Sign(data []byte) (*Signature, error) {
	sr, ss, err := ecdsa.Sign(rand.Reader, ekp.PrivateKey, data)
	if err == nil {
		//pub := ekp.PrivateKey.PublicKey
		//pk := base58.EncodeBig([]byte{}, joinBigInt(ecdsaKeySize, pub.X, pub.Y))
		//return &Signature{r: sr, s: ss}, pk, nil
		return &Signature{r: sr, s: ss}, nil
	}
	return nil, err
}

// PublicKey of the given private key
func (ekp *ECDSAKeypair) PublicKey() PublicKey {
	return ECDSAPublicKey(ekp.PrivateKey.PublicKey)
}

// Verify signature given the public key and data hash
func (ekp *ECDSAKeypair) Verify(pubkey, signature, hash []byte) error {
	sig, err := NewSignatureFromBytes(signature)
	if err == nil {
		err = sig.Verify(pubkey, hash)
	}

	return err
}

// ECDSAPublicKey satifsfies the PublicKey interface
type ECDSAPublicKey ecdsa.PublicKey

// Bytes encoded of the public key
func (pk ECDSAPublicKey) Bytes() []byte {
	x := pk.X
	y := pk.Y
	b := joinBigInt(ecdsaKeySize, x, y)
	return base58.EncodeBig([]byte{}, b)
}
