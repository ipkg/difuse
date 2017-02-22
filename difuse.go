package difuse

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/btcsuite/fastsha256"
	flatbuffers "github.com/google/flatbuffers/go"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

const (
	errInvalidDataType = "invalid data type: %#v"

	replicationQSize = 128
)

var (
	// ErrNotLeader is error not leader
	ErrNotLeader    = errors.New("not leader")
	errNoLocalVnode = errors.New("no local vnode")
)

// VnodeStore implements an actual persistent store.
type VnodeStore interface {
	// Return vnode for this store
	Vnode() *chord.Vnode

	CreateTxKey(key []byte) error
	GetTxKey(key []byte) (*txlog.TxKey, error)

	ProposeTx(tx *txlog.Tx) error
	AppendTx(tx *txlog.Tx) error
	GetTx(key []byte, txhash []byte) (*txlog.Tx, error)
	NewTx(key []byte) (*txlog.Tx, error)

	LastTx(key []byte) (*txlog.Tx, error)
	IterTx(func(keytxs *txlog.KeyTransactions) error) error

	// MerkleRoot of all transactions for the given key
	MerkleRootTx(key []byte) ([]byte, error)
	// Transactions returns transactions starting from the seek point.  If seek is
	// nil or a zero-hash, all transactions are returned.
	Transactions(key, seek []byte) (txlog.TxSlice, error)

	// SetMode
	SetMode(key []byte, mode txlog.KeyMode) error
	Mode(key []byte) (txlog.KeyMode, error)

	// Iterate over all inodes in the store.
	IterInodes(func(key []byte, inode *store.Inode) error) error
	// Return the inode for the given key or error
	Stat(key []byte) (*store.Inode, error)
	InodeCount() int64

	// Sets the value without the use of transactions returning the hash of the data
	// as the key.
	SetBlock(data []byte) ([]byte, error)
	// Gets block by its hash key
	GetBlock(hash []byte) ([]byte, error)
	// Deletes a block by hash key
	DeleteBlock(hash []byte) error
	// Iterate over all blocks in the store.
	IterBlocks(func([]byte, []byte) error) error
	// Returns the no. of blocks in the store.
	BlockCount() int64

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
	ProposeTx(tx *txlog.Tx, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	GetTx(key, txhash []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)

	NewTx(key []byte, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	Transactions(key, seek []byte, vs ...*chord.Vnode) (txlog.TxSlice, error)

	// Transfer keys from the local vnode to the remote one.
	TransferKeys(src, dst *chord.Vnode) error

	LastTx(vn *chord.Vnode, key []byte) (*txlog.Tx, error)
	MerkleRootTx(vn *chord.Vnode, key []byte) ([]byte, error)
	GetTxKey(vn *chord.Vnode, key []byte) (*txlog.TxKey, error)

	// Lookup the leader for the given key on the given host returning the leader, an ordered list of
	// other vnodes as well as a host-to-vnode map.
	LookupLeader(host string, key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error)

	// RegisterVnode registers a datastore for a vnode.
	RegisterVnode(*chord.Vnode, VnodeStore)
	Register(ConsistentStore)
	RegisterTransferQ(chan<- *ReplRequest)
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

	transport *localTransport

	// keys pushed out from local to remote
	replOut chan *ReplRequest

	// keys pulled from remote to local i.e. transfer request
	replIn chan *ReplRequest

	//repl *replicator
}

// NewDifuse instantiates a new Difuse instance, generating a new keypair and setting the
// given remote transport.
func NewDifuse(conf *Config, trans Transport) *Difuse {
	sig, _ := txlog.GenerateECDSAKeypair()
	slt := &Difuse{
		config:   conf,
		signator: sig,
		replOut:  make(chan *ReplRequest, replicationQSize),
		replIn:   make(chan *ReplRequest, replicationQSize),
		//repl:     newReplicator(),
	}

	slt.transport = newLocalTransport(trans, slt)

	//slt.repl.config = slt.config
	//slt.repl.trans = slt.transport

	trans.RegisterTransferQ(slt.replIn)
	go slt.startOutboundReplication()
	go slt.startInboundReplication()

	return slt
}

/*// Creates all directories all the way upto root.  This is used to update a directory
// entry, creating required parent directories along the way, starting at parentPath.
func (s *Difuse) createParentPath(parentPath []byte, child []byte) error {

	opts := &RequestOptions{Consistency: ConsistencyAll}
	inode, _, err := s.Stat(parentPath)
	if err == nil {
		if inode.Type != store.DirInodeType {
			return fmt.Errorf("not a directory inode: %s", parentPath)
		}

		// Update child if not present
		if inode.ContainsBlock(child) {
			return nil
		}
		inode.Blocks = append(inode.Blocks, child)
		//_, err = s.SetInode(f, opts)
		//return err
	} else {
		inode = store.NewDirInode(parentPath)
		inode.Blocks = append(inode.Blocks, child)
	}

	if _, err = s.SetInode(inode, opts); err != nil {
		return err
	}

	skey := string(parentPath)
	if skey == "/" {
		return nil
	}

	// Call on parent
	pp := strings.Split(skey, "/")
	var parent string
	if len(pp) == 2 {
		parent = "/"
	} else {
		parent = strings.Join(pp[:len(pp)-1], "/")
	}

	if err = s.createParentPath([]byte(parent), []byte(pp[len(pp)-1])); err != nil {
		return err
	}

	return nil
}*/

// RegisterRing registers the chord ring to the difuse instance.
func (s *Difuse) RegisterRing(ring *chord.Ring) {
	s.ring = ring
	//s.repl.ring = ring
	s.transport.Register(s)
}

// Get retrieves a the given key whose inode type is assumed to be key.
func (s *Difuse) Get(key []byte, options ...RequestOptions) ([]byte, *ResponseMeta, error) {
	inode, meta, err := s.Stat(key, options...)
	if err != nil {
		return nil, meta, err
	}

	return inode.Blocks[0], meta, nil
}

// Delete deletes an inode associated to the given key based on provided options. Returns
// the leader vnode and error
func (s *Difuse) Delete(key []byte, options ...RequestOptions) (*store.Inode, *ResponseMeta, error) {
	inode, _, err := s.Stat(key, options...)
	if err != nil {
		return nil, nil, err
	}

	//log.Printf("DELETING INODE %#v", inode)

	rmeta := &ResponseMeta{}
	if len(options) > 0 {
		rmeta.Vnode, err = s.DeleteInode(inode, &options[0])
	} else {
		rmeta.Vnode, err = s.DeleteInode(inode, nil)
	}

	return inode, rmeta, err
}

// Set sets a key to the given value.  It uses a key type inode.
func (s *Difuse) Set(key, value []byte, options ...RequestOptions) (*ResponseMeta, error) {

	inode := store.NewKeyInodeWithValue(key, value)

	var (
		rmeta = &ResponseMeta{}
		err   error
	)
	if len(options) > 0 {
		rmeta.Vnode, err = s.SetInode(inode, &options[0])
	} else {
		rmeta.Vnode, err = s.SetInode(inode, nil)
	}

	return rmeta, err
}

// DeleteInode deletes the given inode.  It only deletes the inode and not the underlying block data.
// It returns the leader and error
func (s *Difuse) DeleteInode(inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	var opts *RequestOptions
	if options != nil {
		opts = options
	} else {
		opts = &RequestOptions{Consistency: ConsistencyAll}
	}

	//log.Printf("DELETE INODE %#v", inode)

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(inode.Serialize(fb))

	/*lvn, err := s.appendTx(store.TxTypeDelete, inode.Id, fb.Bytes[fb.Head():], opts)
	if err == ErrNotLeader {
		// Redirect to leader
		return s.transport.DeleteInode(lvn.Host, inode, opts)
	}

	return lvn, err*/
	vn, err := s.proposeTx(store.TxTypeDelete, inode.Id, fb.Bytes[fb.Head():], opts)
	if err != nil {
		if err == ErrNotLeader {
			return s.transport.DeleteInode(vn.Host, inode, opts)
		} else if err.Error() == "previous hash" {
			log.Println("TODO: check", string(inode.Id))
		}
	}

	return vn, nil
}

// SetInode takes the given inode, creates a set tx and submits it based on the
// given consistency level. It returns the leader and error
func (s *Difuse) SetInode(inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	var opts *RequestOptions
	if options != nil {
		opts = options
	} else {
		opts = &RequestOptions{Consistency: ConsistencyAll}
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(inode.Serialize(fb))

	/*lvn, err := s.appendTx(store.TxTypeSet, inode.Id, fb.Bytes[fb.Head():], opts)
	if err == ErrNotLeader {
		//  Redirect to leader
		return s.transport.SetInode(lvn.Host, inode, opts)
	}
	return lvn, err*/

	vn, err := s.proposeTx(store.TxTypeSet, inode.Id, fb.Bytes[fb.Head():], opts)
	if err != nil {
		if err == ErrNotLeader {
			return s.transport.SetInode(vn.Host, inode, opts)
		} else if err.Error() == "previous hash" {
			log.Println("TODO: check", string(inode.Id))
		}
	}

	return vn, nil
}

/*func (s *Difuse) statLocal(key []byte, opts *RequestOptions, vns []*chord.Vnode) (*store.Inode, *ResponseMeta, error) {

	resp, err := s.transport.Stat(key, opts, vns...)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &ResponseMeta{}

	for i, rsp := range resp {
		if rsp.Err != nil {
			err = rsp.Err
			continue
		}

		if ind, ok := rsp.Data.(*store.Inode); ok {
			rmeta.Vnode = vns[i]
			return ind, rmeta, nil
		}
	}
	return nil, rmeta, err
}*/

func parseStatResponse(resp []*VnodeResponse, vns []*chord.Vnode) (*store.Inode, *ResponseMeta, error) {
	var (
		rmeta = &ResponseMeta{}
		err   error
	)

	for i, rsp := range resp {
		if rsp.Err != nil {
			err = rsp.Err
			continue
		}

		if ind, ok := rsp.Data.(*store.Inode); ok {
			rmeta.Vnode = vns[i]
			return ind, rmeta, nil
		} else {
			err = fmt.Errorf(errInvalidDataType, rsp.Data)
		}
	}

	return nil, rmeta, err
}

// Stat returns the inode entry for the key. By default it uses the leader consistency
func (s *Difuse) Stat(key []byte, options ...RequestOptions) (*store.Inode, *ResponseMeta, error) {
	var opts *RequestOptions
	if len(options) > 0 {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyAll}
	}

	ccfg := s.config.Chord

	_, vl, err := s.ring.Lookup(ccfg.NumSuccessors, key)
	if err != nil {
		return nil, nil, err
	}
	vnb := vnodesByHost(vl)

	// Try local vnodes first
	if vns, ok := vnb[ccfg.Hostname]; ok {
		resp, er := s.transport.Stat(key, opts, vns...)
		if er == nil {
			inode, rmeta, e := parseStatResponse(resp, vns)
			if e == nil {
				return inode, rmeta, nil
			} else {
				err = e
			}
		} else {
			err = er
		}
		// Remove local host as we've already visited it.
		delete(vnb, ccfg.Hostname)
	}

	if len(vnb) == 0 {
		return nil, nil, err
	}

	rmeta := &ResponseMeta{}
	// try the remainder
	for _, vns := range vnb {
		resp, er := s.transport.Stat(key, opts, vns...)
		if er != nil {
			err = er
			continue
		}

		var inode *store.Inode
		if inode, rmeta, er = parseStatResponse(resp, vns); er != nil {
			err = er
			continue
		}

		return inode, rmeta, nil
	}

	return nil, rmeta, err
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
		_, vns, err := s.ring.Lookup(s.config.Chord.NumSuccessors, hash)
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

		_, vns, err := s.ring.Lookup(s.config.Chord.NumSuccessors, sh[:])
		if err != nil {
			return nil, err
		}
		vm := vnodesByHost(vns)

		//out := make([]*VnodeResponse, 0)
		for _, vl := range vm {
			resp, er := s.transport.SetBlock(data, opts, vl...)
			if er != nil {
				//return nil, err
				err = er
				continue
			}
			// check all responses
			for _, v := range resp {
				if v.Err != nil {
					//return nil, v.Err
					err = v.Err
					continue
				}
			}
		}

		return sh[:], err
	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}

// DeleteBlock deletes the block from all vnodes based on the specified consistency.
func (s *Difuse) DeleteBlock(hash []byte, options ...RequestOptions) error {
	var opts *RequestOptions
	if len(options) > 0 {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyAll}
	}

	switch opts.Consistency {
	case ConsistencyAll:

		_, vns, err := s.ring.Lookup(s.config.Chord.NumSuccessors, hash)
		if err != nil {
			return err
		}
		vm := vnodesByHost(vns)

		for _, vl := range vm {
			resp, er := s.transport.DeleteBlock(hash, opts, vl...)
			if er != nil {
				//return err
				err = er
				continue
			}
			// check all responses
			for _, v := range resp {
				if v.Err != nil {
					//return v.Err
					err = v.Err
					continue
				}
			}
		}

		return err
	}

	return fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}

// LookupLeader does a lookup on the key and returns the leader for the key, vnodes
// used to compute the leader
func (s *Difuse) LookupLeader(key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error) {
	_, vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, nil, nil, err
	}

	return vs[0], vs, vnodesByHost(vs), nil
	//l, vm, err := s.keyleader(key, vs)
	//return l, vs, vm, err
}

func (s *Difuse) isLocalVnode(vn *chord.Vnode) bool {
	return s.config.Chord.Hostname == vn.Host
}
