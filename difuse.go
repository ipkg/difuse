package difuse

import (
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/fastsha256"
	flatbuffers "github.com/google/flatbuffers/go"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

const (
	errInvalidDataType = "invalid data type: %#v"
)

var (
	// ErrNotLeader is error not leader
	ErrNotLeader = errors.New("not leader")
)

// VnodeStore implements an actual persistent store.
type VnodeStore interface {
	AppendTx(tx *txlog.Tx) error
	GetTx(key []byte, txhash []byte) (*txlog.Tx, error)
	NewTx(key []byte) (*txlog.Tx, error)
	LastTx(key []byte) (*txlog.Tx, error)
	IterTx(func([]byte, *txlog.KeyTransactions) error) error
	// MerkleRoot of all transactions for the given key
	MerkleRootTx(key []byte) ([]byte, error)

	// Iterate over all inodes in the store.
	IterInodes(func([]byte, *store.Inode) error) error
	Stat(key []byte) (*store.Inode, error)

	// This sets the value without the use of transactions returning the hash of the data
	// as the key.
	SetBlock(data []byte) ([]byte, error)
	GetBlock(hash []byte) ([]byte, error)
	DeleteBlock(hash []byte) error
	// Iterate over all blocks in the store.
	IterBlocks(func([]byte, []byte) error) error

	Snapshot() (io.ReadCloser, error)
	Restore(io.Reader) error
}

// Transport is the transport interface for various rpc calls
type Transport interface {
	// Stat returns the inode entries from the specified vnodes for the given key
	Stat(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	// Set the inode on the given host returning the leader vnode or error
	SetInode(string, *store.Inode, *RequestOptions) (*chord.Vnode, error)
	// Delete the inode on the given host returning the leader vnode or error
	DeleteInode(string, *store.Inode, *RequestOptions) (*chord.Vnode, error)

	// Block data is directly on the vnode. This is used when the transaction log is not
	// needed.  Data set using this call should be stored seperately from the transactional
	// data in terms of physicality.
	SetBlock([]byte, *RequestOptions, ...*chord.Vnode) ([]*VnodeResponse, error)
	GetBlock([]byte, *RequestOptions, ...*chord.Vnode) ([]*VnodeResponse, error)
	DeleteBlock([]byte, *RequestOptions, ...*chord.Vnode) ([]*VnodeResponse, error)
	// Replicate blocks from the local vnode to the remote one.
	ReplicateBlocks(src, dst *chord.Vnode) error

	AppendTx(tx *txlog.Tx, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	GetTx(key, txhash []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	LastTx(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	MerkleRootTx(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	NewTx(key []byte, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	// Replicate transactions from the local vnode to the remote one.
	ReplicateTx(src, dst *chord.Vnode) error

	// Lookup the leader for the given key on the given host returning the leader, an ordered list of
	// other vnodes as well as a host-to-vnode map.
	LookupLeader(host string, key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error)

	// RegisterVnode registers a datastore for a vnode.
	RegisterVnode(*chord.Vnode, VnodeStore)
	Register(ConsistentStore)
}

// ConsistentStore implements consistent store methods using the underlying ring methods.
type ConsistentStore interface {
	LookupLeader(key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error)
	// SetInode sets the given inode returning the leader for the inode and error
	SetInode(inode *store.Inode, options *RequestOptions) (*chord.Vnode, error)
	// DeleteInode deletes the given inode returning the leader for the inode and error
	DeleteInode(inode *store.Inode, options *RequestOptions) (*chord.Vnode, error)
}

// Difuse is the core engine
type Difuse struct {
	signator txlog.Signator

	ring   *chord.Ring
	config *Config

	transport Transport
}

// NewDifuse instantiates a new Difuse instance, generating a new keypair and setting the
// given remote transport.
func NewDifuse(conf *Config, trans Transport) *Difuse {
	sig, _ := txlog.GenerateECDSAKeypair()
	slt := &Difuse{
		config:   conf,
		signator: sig,
	}

	slt.transport = newLocalTransport(trans, slt)
	return slt
}

// RegisterRing registers the chord ring to the difuse instance.
func (s *Difuse) RegisterRing(ring *chord.Ring) {
	s.ring = ring
	s.transport.Register(s)
}

// Get retrieves a the given key.  It first gets the inode then retrieves the underlying
// blocks
func (s *Difuse) Get(key []byte, options ...RequestOptions) ([]byte, *ResponseMeta, error) {
	inode, meta, err := s.Stat(key, options...)
	if err != nil {
		return nil, meta, err
	}

	out := make([]byte, 0, inode.Size)
	for _, bh := range inode.Blocks {

		bd, err := s.GetBlock(bh, options...)
		if err != nil {
			return nil, meta, err
		}

		out = append(out, bd...)
	}
	return out, meta, nil
}

// Delete deletes an inode associated to the given key based on provided options. Returns
// the leader vnode and error
func (s *Difuse) Delete(key []byte, options ...RequestOptions) (*store.Inode, *ResponseMeta, error) {
	inode, _, err := s.Stat(key, options...)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &ResponseMeta{}
	if len(options) > 0 {
		rmeta.Vnode, err = s.DeleteInode(inode, &options[0])
	} else {
		rmeta.Vnode, err = s.DeleteInode(inode, nil)
	}

	return inode, rmeta, err
}

// Set sets a key to the given value.  It first sets the underlying block data then
// sets the inode.  Returns the leader vnode and error
func (s *Difuse) Set(key, value []byte, options ...RequestOptions) (*ResponseMeta, error) {
	hsh, err := s.SetBlock(value, options...)
	if err != nil {
		return nil, err
	}

	inode := store.NewInode(key)
	inode.Size = int64(len(value))
	inode.Blocks = [][]byte{hsh}

	rmeta := &ResponseMeta{}
	if len(options) > 0 {
		rmeta.Vnode, err = s.SetInode(inode, &options[0])
	} else {
		rmeta.Vnode, err = s.SetInode(inode, nil)
	}

	return rmeta, err
}

// DeleteInode deletes the given inode.  It only deletes the inode and not the underlying data.
// It returns the leader and error
func (s *Difuse) DeleteInode(inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	var opts *RequestOptions
	if options != nil {
		opts = options
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLeader}
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(inode.Serialize(fb))

	lvn, err := s.appendTx(store.TxTypeDelete, inode.Id, fb.Bytes[fb.Head():], opts)
	if err == ErrNotLeader {
		// TODO: Redirect to leader
		return s.transport.DeleteInode(lvn.Host, inode, opts)
	}

	return lvn, err
}

// SetInode takes the given inode, creates a set tx and submits it based on the
// given consistency level. It returns the leader and error
func (s *Difuse) SetInode(inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	var opts *RequestOptions
	if options != nil {
		opts = options
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLeader}
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(inode.Serialize(fb))

	lvn, err := s.appendTx(store.TxTypeSet, inode.Id, fb.Bytes[fb.Head():], opts)
	if err == ErrNotLeader {
		// TODO: Redirect to leader
		return s.transport.SetInode(lvn.Host, inode, opts)
	}

	return lvn, err
}

// Stat returns the inode entry for the key. By default it uses the leader consistency
func (s *Difuse) Stat(key []byte, options ...RequestOptions) (*store.Inode, *ResponseMeta, error) {
	var opts *RequestOptions
	if len(options) > 0 {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLeader}
	}

	rmeta := &ResponseMeta{}

	switch opts.Consistency {
	case ConsistencyLeader:
		l, _, _, err := s.LookupLeader(key)
		if err != nil {
			return nil, nil, err
		}
		rmeta.Vnode = l

		resp, err := s.transport.Stat(key, opts, l)
		if err != nil {
			return nil, rmeta, err
		}

		if resp[0].Err != nil {
			return nil, rmeta, resp[0].Err
		}

		if ind, ok := resp[0].Data.(*store.Inode); ok {
			return ind, rmeta, nil
		}
		return nil, rmeta, fmt.Errorf(errInvalidDataType, resp[0].Data)

	case ConsistencyLazy:
		ccfg := s.config.Chord

		vl, err := s.ring.Lookup(ccfg.NumSuccessors, key)
		if err != nil {
			return nil, nil, err
		}

		vnb := vnodesByHost(vl)
		// Try local vnodes first
		if vns, ok := vnb[ccfg.Hostname]; ok {
			resp, er := s.transport.Stat(key, opts, vns...)
			if er == nil {
				for i, rsp := range resp {
					if rsp.Err == nil {
						if ind, ok := rsp.Data.(*store.Inode); ok {
							rmeta.Vnode = vns[i]
							return ind, rmeta, nil
						}
					}
				}
			}
			// Remove local host as we've already visited it.
			delete(vnb, ccfg.Hostname)
		}

		// try the remainder
		for _, vns := range vnb {
			resp, er := s.transport.Stat(key, opts, vns...)
			if er != nil {
				err = er
				continue
			}

			for i, rsp := range resp {
				if rsp.Err != nil {
					err = rsp.Err
					continue
				}

				if ind, ok := rsp.Data.(*store.Inode); ok {
					rmeta.Vnode = vns[i]
					return ind, rmeta, nil
				}
				err = fmt.Errorf(errInvalidDataType, rsp.Data)
			}
		}

		return nil, rmeta, err
	}

	return nil, rmeta, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}

// GetBlock gets a block based on the provided options
func (s *Difuse) GetBlock(hash []byte, options ...RequestOptions) ([]byte, error) {
	var opts *RequestOptions
	if len(options) > 0 {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLazy}
	}

	switch opts.Consistency {
	case ConsistencyLazy:
		vns, err := s.ring.Lookup(s.config.Chord.NumSuccessors, hash)
		if err != nil {
			return nil, err
		}

		vm := vnodesByHost(vns)
		for _, vl := range vm {
			resp, er := s.transport.GetBlock(hash, opts, vl...)
			if er != nil {
				return nil, err
			}
			// check all responses
			for _, v := range resp {
				if v.Err == nil {
					if val, ok := v.Data.([]byte); ok {
						return val, nil
					}
					err = fmt.Errorf(errInvalidDataType, v.Data)
				} else {
					err = v.Err
				}
			}
		}

		return nil, err

	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}

// SetBlock sets the block data on all vnodes based on the options returning the hash key
// for the data or an error
func (s *Difuse) SetBlock(data []byte, options ...RequestOptions) ([]byte, error) {
	var opts *RequestOptions
	if len(options) > 0 {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyAll}
	}

	sh := fastsha256.Sum256(data)

	switch opts.Consistency {
	case ConsistencyAll:

		vns, err := s.ring.Lookup(s.config.Chord.NumSuccessors, sh[:])
		if err != nil {
			return nil, err
		}
		vm := vnodesByHost(vns)

		//out := make([]*VnodeResponse, 0)
		for _, vl := range vm {
			resp, er := s.transport.SetBlock(data, opts, vl...)
			if er != nil {
				return nil, err
			}
			// check all responses
			for _, v := range resp {
				if v.Err != nil {
					return nil, v.Err
				}
			}
		}
		return sh[:], nil
	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}

// DeleteBlock deletes the block from all vnodes based on the specified consistency.
func (s *Difuse) DeleteBlock(hash []byte, options ...RequestOptions) error {
	return fmt.Errorf("TBI")
}

// LookupLeader does a lookup on the key and returns the leader for the key, vnodes
// used to compute the leader
func (s *Difuse) LookupLeader(key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error) {
	vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, nil, nil, err
	}

	l, vm, err := s.keyleader(key, vs)
	return l, vs, vm, err
}
