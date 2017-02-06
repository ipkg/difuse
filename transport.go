package difuse

import (
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

func (lt *localTransport) GetTx(key, txhash []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.GetTx(key, txhash, options, vl...)
	}
	return lt.remote.GetTx(key, txhash, options, vl...)
}

func (lt *localTransport) LastTx(key []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.LastTx(key, options, vl...)
	}
	return lt.remote.LastTx(key, options, vl...)
}

func (lt *localTransport) MerkleRootTx(key []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.MerkleRootTx(key, options, vl...)
	}
	return lt.remote.MerkleRootTx(key, options, vl...)
}

func (lt *localTransport) NewTx(key []byte, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.NewTx(key, vl...)
	}
	return lt.remote.NewTx(key, vl...)
}

// ReplicateTx replicats transactions from a local vnode to a remote one. i.e.
// pushes tx's to the specified remote vnode.
func (lt *localTransport) ReplicateTx(src, dst *chord.Vnode) error {
	return lt.remote.ReplicateTx(src, dst)
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
