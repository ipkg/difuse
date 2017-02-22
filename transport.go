package difuse

import (
	"encoding/json"
	"sync"

	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
	chord "github.com/ipkg/go-chord"
)

// VnodeResponse contains responses from multiple vnodes.
type VnodeResponse struct {
	Id   []byte // vnode id
	Data interface{}
	Err  error
}

// MarshalJSON is a custom json encoder to handler errors and formatting.
func (vr *VnodeResponse) MarshalJSON() ([]byte, error) {
	o := map[string]interface{}{
		"vnode": string(vr.Id),
	}

	if vr.Data != nil {
		o["data"] = vr.Data
	}

	if vr.Err != nil {
		o["error"] = vr.Err.Error()
	}

	return json.Marshal(o)
}

// ResponseMeta contains response metadata
type ResponseMeta struct {
	// Vnode that executed/responded.  In the case of writes this will be the leader
	// vnode. For reads it will be the node that performed the acual read
	Vnode *chord.Vnode
}

// localTransport routes requests to local or remote based on the given vnodes.
type localTransport struct {
	host string

	lock  sync.Mutex
	local localStore

	remote Transport

	cs ConsistentStore
}

func newLocalTransport(remote Transport, cs ConsistentStore) *localTransport {
	return &localTransport{remote: remote, local: make(localStore), cs: cs}
}

func (lt *localTransport) Stat(key []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.Stat(key, options, vl...)
	}
	return lt.remote.Stat(key, options, vl...)
}

func (lt *localTransport) SetInode(host string, inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	if lt.host == host {
		return lt.cs.SetInode(inode, options)
	}
	return lt.remote.SetInode(host, inode, options)
}

func (lt *localTransport) DeleteInode(host string, inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	if lt.host == host {
		return lt.cs.DeleteInode(inode, options)
	}
	return lt.remote.DeleteInode(host, inode, options)
}

func (lt *localTransport) SetBlock(data []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.SetBlock(data, options, vl...)
	}
	return lt.remote.SetBlock(data, options, vl...)
}

func (lt *localTransport) GetBlock(hash []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.GetBlock(hash, options, vl...)
	}
	return lt.remote.GetBlock(hash, options, vl...)
}

func (lt *localTransport) DeleteBlock(hash []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.DeleteBlock(hash, options, vl...)
	}
	return lt.remote.DeleteBlock(hash, options, vl...)
}

func (lt *localTransport) ReplicateBlocks(src, dst *chord.Vnode) error {
	return lt.remote.ReplicateBlocks(src, dst)
}

func (lt *localTransport) AppendTx(tx *txlog.Tx, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.AppendTx(tx, options, vl...)
	}
	return lt.remote.AppendTx(tx, options, vl...)
}

func (lt *localTransport) ProposeTx(tx *txlog.Tx, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.ProposeTx(tx, options, vl...)
	}
	return lt.remote.ProposeTx(tx, options, vl...)
}

func (lt *localTransport) Transactions(vn *chord.Vnode, key, seek []byte) (txlog.TxSlice, error) {
	if vn.Host == lt.host {
		return lt.local.Transactions(vn, key, seek)
	}
	return lt.remote.Transactions(vn, key, seek)
}

func (lt *localTransport) GetTx(vn *chord.Vnode, key, txhash []byte) (*txlog.Tx, error) {
	if vn.Host == lt.host {
		return lt.local.GetTx(vn, key, txhash)
	}
	return lt.remote.GetTx(vn, key, txhash)
}

func (lt *localTransport) LastTx(vn *chord.Vnode, key []byte) (*txlog.Tx, error) {
	if vn.Host == lt.host {
		return lt.local.LastTx(vn, key)
	}
	return lt.remote.LastTx(vn, key)
}

func (lt *localTransport) MerkleRootTx(vn *chord.Vnode, key []byte) ([]byte, error) {
	if vn.Host == lt.host {
		return lt.local.MerkleRootTx(vn, key)
	}
	return lt.remote.MerkleRootTx(vn, key)
}

func (lt *localTransport) NewTx(key []byte, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.NewTx(key, vl...)
	}
	return lt.remote.NewTx(key, vl...)
}

func (lt *localTransport) GetTxKey(vn *chord.Vnode, key []byte) (*txlog.TxKey, error) {
	if vn.Host == lt.host {
		return lt.local.GetTxKey(vn, key)
	}
	return lt.remote.GetTxKey(vn, key)
}

func (lt *localTransport) SetMode(vn *chord.Vnode, key []byte, mode txlog.KeyMode) error {
	if vn.Host == lt.host {
		return lt.local.SetMode(vn, key, mode)
	}
	return lt.remote.SetModeTxKey(vn, key, mode)
}

func (lt *localTransport) TransferTxKeys(src, dst *chord.Vnode) error {
	return lt.remote.TransferTxKeys(src, dst)
}

// set request for remote on leader
//Set(peer string, key, value []byte, options ...RequestOptions) error
//Delete(peer string, key []byte, options ...RequestOptions) error
func (lt *localTransport) LookupLeader(host string, key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error) {
	if lt.host == host {
		return lt.cs.LookupLeader(key)
	}
	return lt.remote.LookupLeader(host, key)
}

// RegisterVnode registers a datastore for a vnode.
func (lt *localTransport) RegisterVnode(vn *chord.Vnode, vs VnodeStore) {
	lt.lock.Lock()
	lt.host = vn.Host
	lt.local[vn.String()] = vs
	lt.lock.Unlock()

	lt.remote.RegisterVnode(vn, vs)
}

func (lt *localTransport) Register(cs ConsistentStore) {
	lt.cs = cs
	lt.remote.Register(cs)
}

type consistentTransport struct {
	ring      *chord.Ring
	transport Transport
}

func (c *consistentTransport) LastTx(key []byte, opts RequestOptions) (*txlog.Tx, error) {
	_, vs, err := c.ring.Lookup(opts.NetSize, key)
	if err != nil {
		return nil, err
	}

	for _, vn := range vs {
		tx, er := c.transport.LastTx(vn, key)
		if er != nil {
			err = er
			continue
		}
		return tx, nil
	}

	return nil, err
}

func (c *consistentTransport) GetTx(key, txhash []byte, opts RequestOptions) (*txlog.Tx, error) {
	_, vs, err := c.ring.Lookup(opts.NetSize, key)
	if err != nil {
		return nil, err
	}

	for _, vn := range vs {
		tx, er := c.transport.GetTx(vn, key, txhash)
		if er != nil {
			err = er
			continue
		}
		return tx, nil
	}

	return nil, err
}
