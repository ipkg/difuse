package txlog

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"math/big"

	"github.com/tv42/base58"
)

var (
	errSignatureVerify = fmt.Errorf("signature verification failed")
	errNotSigned       = fmt.Errorf("not signed")
	errAlreadySigned   = fmt.Errorf("already signed")
)

// Signature of a block or transaction
type Signature struct {
	r *big.Int
	s *big.Int
}

// NewSignatureFromBytes decodes signature bytes into a signature struct.
func NewSignatureFromBytes(b []byte) (*Signature, error) {
	if b == nil || len(b) == 0 {
		return nil, errNotSigned
	}

	bi, err := base58.DecodeToBig(b)
	if err == nil {
		pp := splitBigInt(bi, 2)
		return &Signature{r: pp[0], s: pp[1]}, nil
	}

	return nil, err
}

// Bytes of encoded signature
func (sig *Signature) Bytes() []byte {
	b := joinBigInt(ecdsaKeySize, sig.r, sig.s)
	return base58.EncodeBig([]byte{}, b)
}

// Verify data given the public key using the signature
func (sig *Signature) Verify(pubkey []byte, data []byte) error {
	pub, err := decodeECDSAPublicKeyBytes(pubkey)
	if err == nil {
		if ok := ecdsa.Verify(&pub, data, sig.r, sig.s); !ok {
			err = errSignatureVerify
		}
	}
	return err
}

func decodeECDSAPublicKeyBytes(pk []byte) (pub ecdsa.PublicKey, err error) {
	var b *big.Int
	if b, err = base58.DecodeToBig(pk); err == nil {
		sigg := splitBigInt(b, 2)
		pub = ecdsa.PublicKey{Curve: elliptic.P256(), X: sigg[0], Y: sigg[1]}
	}

	return
}
