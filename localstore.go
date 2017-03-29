package difuse

import (
	"github.com/ipkg/difuse/types"
	chord "github.com/ipkg/go-chord"
)

// localStore is a store containing all tx backed stores for the node.  This is used to address all
// stores for a node.
type localStore map[string]*VnodeStore

func (nls localStore) QueueBlockReplay(vn *chord.Vnode, txb *types.TxBlock) error {
	st, err := nls.getStore(vn)
	if err == nil {
		st.QueueBlockReplay(txb)
	}
	return err
}

func (nls localStore) GetTxBlock(vn *chord.Vnode, key []byte) (*types.TxBlock, error) {
	st, err := nls.getStore(vn)
	if err == nil {
		return st.GetTxBlock(key)
	}
	return nil, err
}

func (nls localStore) ProposeTx(vn *chord.Vnode, tx *types.Tx) error {
	st, err := nls.getStore(vn)
	if err == nil {
		return st.ProposeTx(tx)
	}
	return err
}

// GetTx gets a keyed tx from multiple vnodes based on the specified consistency.  All vnodes in the slice are assumed to be local vnodes
func (nls localStore) GetTx(vn *chord.Vnode, txhash []byte) (*types.Tx, error) {

	st, err := nls.getStore(vn)
	if err == nil {
		return st.GetTx(txhash)
	}

	return nil, err
}

func (nls localStore) NewTx(vn *chord.Vnode, key []byte) (*types.Tx, error) {

	st, err := nls.getStore(vn)
	if err == nil {
		return st.NewTx(key)
	}

	return nil, err
}

func (nls localStore) getStore(vn *chord.Vnode) (*VnodeStore, error) {
	if s, ok := nls[vn.String()]; ok {
		return s, nil
	}
	return nil, errStoreNotFound
}
