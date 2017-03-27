package txlog

import (
	"fmt"

	"github.com/ipkg/difuse/utils"
)

type BlackHoleTransport struct{}

func (bht *BlackHoleTransport) ProposeTx(tx *Tx, opts utils.RequestOptions) (*utils.ResponseMeta, error) {
	return nil, fmt.Errorf("failed to connect. blackhole: %x", tx.Hash())
}
func (bht *BlackHoleTransport) GetTx(id []byte, opts utils.RequestOptions) (*Tx, *utils.ResponseMeta, error) {
	return nil, nil, fmt.Errorf("failed to connect. blackhole: %x", id)
}
