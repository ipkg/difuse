package difuse

import (
	"sync"

	chord "github.com/euforia/go-chord"
	"github.com/ipkg/difuse/txlog"
)

// VnodeResponse contains responses from multiple vnodes.
type VnodeResponse struct {
	Id   []byte // vnode id
	Data interface{}
	Err  error
}

// localTransport routes requests to local or remote based on the given vnodes.
type localTransport struct {
	host string

	lock  sync.Mutex
	local localStore

	remote Transport

	elector LeaderElector
}

func newLocalTransport(remote Transport, elector LeaderElector) *localTransport {
	return &localTransport{remote: remote, local: make(localStore), elector: elector}
}

func (lt *localTransport) Stat(key []byte, options *RequestOptions, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	if vl[0].Host == lt.host {
		return lt.local.Stat(key, options, vl...)
	}
	return lt.remote.Stat(key, options, vl...)
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
		return lt.elector.LookupLeader(key)
	}
	return lt.remote.LookupLeader(host, key)
}

// Register registers a datastore for a vnode.
func (lt *localTransport) Register(vn *chord.Vnode, vs VnodeStore) {
	lt.lock.Lock()
	lt.host = vn.Host
	lt.local[vn.String()] = vs
	lt.lock.Unlock()

	lt.remote.Register(vn, vs)
}

func (lt *localTransport) RegisterElector(elector LeaderElector) {
	lt.elector = elector
	lt.remote.RegisterElector(elector)
}
