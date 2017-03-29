package difuse

import (
	"fmt"
	"log"
	"sync"

	"github.com/ipkg/difuse/types"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

// localTransport routes requests to local or remote based on the given vnodes.
type localTransport struct {
	host string

	lock  sync.Mutex
	local localStore

	remote Transport
}

func newLocalTransport(remote Transport) *localTransport {
	return &localTransport{remote: remote, local: make(localStore)}
}

func (lt *localTransport) ProposeTx(vn *chord.Vnode, tx *types.Tx) error {
	if vn.Host == lt.host {
		return lt.local.ProposeTx(vn, tx)
	}
	return lt.remote.ProposeTx(vn, tx)
}

func (lt *localTransport) GetTx(vn *chord.Vnode, txhash []byte) (*types.Tx, error) {
	if vn.Host == lt.host {
		return lt.local.GetTx(vn, txhash)
	}
	return lt.remote.GetTx(vn, txhash)
}

func (lt *localTransport) NewTx(vn *chord.Vnode, key []byte) (*types.Tx, error) {
	if vn.Host == lt.host {
		return lt.local.NewTx(vn, key)
	}
	return lt.remote.NewTx(vn, key)
}

func (lt *localTransport) GetTxBlock(vn *chord.Vnode, key []byte) (*types.TxBlock, error) {
	if vn.Host == lt.host {
		return lt.local.GetTxBlock(vn, key)
	}
	return lt.remote.GetTxBlock(vn, key)
}

func (lt *localTransport) TransferTxBlocks(local, remote *chord.Vnode) error {
	if local.Host == lt.host {
		return lt.remote.TransferTxBlocks(local, remote)
	}

	return fmt.Errorf("source vnode must be local: %s!=%s", utils.ShortVnodeID(local), lt.host)
}

func (lt *localTransport) RegisterTakeoverQ(ch chan<- *TakeoverReq) {
	lt.remote.RegisterTakeoverQ(ch)
}

// RegisterVnode registers a datastore for a vnode.
func (lt *localTransport) Register(vn *chord.Vnode, vs *VnodeStore) {
	log.Printf("action=register entity=store vn=%s", utils.ShortVnodeID(vn))

	lt.lock.Lock()
	lt.host = vn.Host
	lt.local[vn.String()] = vs
	lt.lock.Unlock()

	lt.remote.Register(vn, vs)
}

/*func (lt *localTransport) RegisterTxTransport(txt txlog.Transport) {
	log.Printf("action=register entity=tx-transport")

	lt.lock.Lock()
	for k := range lt.local {
		lt.local[k].RegisterTxTransport(txt)
	}
	lt.lock.Unlock()
}*/
