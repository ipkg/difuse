package difuse

import (
	"errors"

	"github.com/ipkg/difuse/keypairs"
	"github.com/ipkg/difuse/txlog"
	"github.com/ipkg/difuse/types"
)

var (
	errStoreNotFound = errors.New("store not found")
)

// VnodeStore is the actual store
type VnodeStore struct {
	tbstore txlog.TxBlockStore
	txstore txlog.TxStore
	txl     *txlog.TxLog
}

// NewVnodeStore instantiates a new VnodeStore.  If fsm is nil, it is set to the VnodeStore itself
// which implements a dummy fsm.
func NewVnodeStore(txstore txlog.TxStore, trans txlog.Transport, fsm txlog.FSM) *VnodeStore {
	vs := &VnodeStore{
		tbstore: txlog.NewMemTxBlockStore(),
		txstore: txstore,
	}

	kp, _ := keypairs.GenerateECDSAKeypair()
	vs.txl = txlog.NewTxLog(kp, vs.tbstore, vs.txstore, trans, fsm)
	vs.txl.Start()

	return vs
}

// QueueBlockReplay queues a TxBlock to the tx log to be replayed.
func (vs *VnodeStore) QueueBlockReplay(txb *types.TxBlock) {
	vs.txl.QueueBlockReplay(txb)
}

// GetTx gets a tx with the given id from the tx store.
func (vs *VnodeStore) GetTx(id []byte) (*types.Tx, error) {
	return vs.txstore.Get(id)
}

// GetTxBlock gets a TxBlock from the tx block store.
func (vs *VnodeStore) GetTxBlock(key []byte) (*types.TxBlock, error) {
	return vs.tbstore.Get(key)
}

// NewTx returns a new tx from the tx log.
func (vs *VnodeStore) NewTx(key []byte) (*types.Tx, error) {
	return vs.txl.NewTx(key)
}

// ProposeTx proposed the given tx to the tx log.
func (vs *VnodeStore) ProposeTx(tx *types.Tx) error {
	return vs.txl.ProposeTx(tx)
}
