package txlog

import (
	"fmt"

	"github.com/ipkg/difuse/types"
)

type BlackHoleTransport struct{}

func (bht *BlackHoleTransport) ProposeTx(tx *types.Tx, opts types.RequestOptions) (*types.ResponseMeta, error) {
	return nil, fmt.Errorf("failed to connect. blackhole: %x", tx.Hash())
}
func (bht *BlackHoleTransport) GetTx(id []byte, opts types.RequestOptions) (*types.Tx, *types.ResponseMeta, error) {
	return nil, nil, fmt.Errorf("failed to connect. blackhole: %x", id)
}
