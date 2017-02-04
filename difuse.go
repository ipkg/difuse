package difuse

import (
	"fmt"
	"io"
	"log"

	"github.com/btcsuite/fastsha256"
	chord "github.com/euforia/go-chord"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

const (
	errInvalidDataType = "invalid data type: %#v"
	errNotLeader       = "not leader: %s"
)

// VnodeStore implements an actual persistent store.
type VnodeStore interface {
	AppendTx(tx *txlog.Tx) error
	GetTx(key []byte, txhash []byte) (*txlog.Tx, error)
	NewTx(key []byte) (*txlog.Tx, error)
	LastTx(key []byte) (*txlog.Tx, error)
	IterTx(func([]byte, txlog.TxSlice) error) error
	// MerkleRoot of transactions for the given key
	MerkleRoot(key []byte) ([]byte, error)
	// Iterate over all inodes in the store.
	IterInodes(func([]byte, *store.Inode) error) error
	Stat(key []byte) (*store.Inode, error)
	// This sets the value without any transactions returning the key hash. This is used to set
	// content addressable data.
	SetBlock(data []byte) ([]byte, error)
	GetBlock(hash []byte) ([]byte, error)
	DeleteBlock(hash []byte) error
	// Block iteration
	IterBlocks(func([]byte, []byte) error) error

	Snapshot() (io.ReadCloser, error)
	Restore(io.Reader) error
}

// Transport is the transport interface for various rpc calls
type Transport interface {
	// Stat returns the inode entries from the specified vnodes for the given key
	Stat(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)

	// Block data is directly on the vnode. This is used when the transaction log is not
	// needed.  Data set using this call should be stored seperately from the transactional
	// data and is stored as content addressable.
	SetBlock([]byte, *RequestOptions, ...*chord.Vnode) ([]*VnodeResponse, error)
	GetBlock([]byte, *RequestOptions, ...*chord.Vnode) ([]*VnodeResponse, error)
	DeleteBlock([]byte, *RequestOptions, ...*chord.Vnode) ([]*VnodeResponse, error)
	ReplicateBlocks(src, dst *chord.Vnode) error

	AppendTx(tx *txlog.Tx, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	GetTx(key, txhash []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	LastTx(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	NewTx(key []byte, vs ...*chord.Vnode) ([]*VnodeResponse, error)
	ReplicateTx(src, dst *chord.Vnode) error

	LookupLeader(host string, key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error)

	// Register registers a datastore for a vnode.
	Register(*chord.Vnode, VnodeStore)
	RegisterElector(elector LeaderElector)
}

// LeaderElector implements the leader election algorithm.
type LeaderElector interface {
	LookupLeader(key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error)
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
	s.transport.RegisterElector(s)
}

// isLeader returns whether this host owns the provided leader vnode
func (s *Difuse) isLeader(vn *chord.Vnode) bool {
	return s.config.Chord.Hostname == vn.Host
}

// Delete deletes an inode associated to the given key based on provided options.
func (s *Difuse) Delete(key []byte, options ...RequestOptions) error {
	inode, err := s.Stat(key, options...)
	if err != nil {
		return err
	}

	return s.DeleteInode(inode, options...)
}

// Get retrieves a the given key.  It first gets the inode then retrieves the underlying
// blocks
func (s *Difuse) Get(key []byte, options ...RequestOptions) ([]byte, error) {
	inode, err := s.Stat(key, options...)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, inode.Size)
	for _, bh := range inode.Blocks {

		bd, err := s.GetBlock(bh, options...)
		if err != nil {
			return nil, err
		}

		out = append(out, bd...)
	}
	return out, nil
}

// Set sets a key to the given value.  It first sets the underlying block data then
// sets the inode.
func (s *Difuse) Set(key, value []byte, options ...RequestOptions) error {
	hsh, err := s.SetBlock(value, options...)
	if err != nil {
		return err
	}

	inode := store.NewInode(key)
	inode.Size = int64(len(value))
	inode.Blocks = [][]byte{hsh}

	return s.SetInode(inode, options...)
}

func (s *Difuse) appendTx(txtype byte, id, data []byte, opts *RequestOptions) error {

	switch opts.Consistency {
	case ConsistencyLeader:
		l, _, vm, err := s.LookupLeader(id)
		if err != nil {
			return err
		}

		if !s.isLeader(l) {
			return fmt.Errorf(errNotLeader, shortID(l))
		}
		// Get new tx from leader
		rsp, err := s.transport.NewTx(id, l)
		if err != nil {
			return err
		}
		if rsp[0].Err != nil {
			return rsp[0].Err
		}
		tx, ok := rsp[0].Data.(*txlog.Tx)
		if !ok {
			return fmt.Errorf(errInvalidDataType, tx)
		}

		tx.Data = append([]byte{txtype}, data...)
		if err = tx.Sign(s.signator); err != nil {
			return err
		}
		// Append the new tx
		vns := vm[l.Host]
		resp, err := s.transport.AppendTx(tx, opts, vns...)
		if err != nil {
			return err
		}
		if resp[0].Err != nil {
			return resp[0].Err
		}

		delete(vm, l.Host)

		go func(vmap map[string][]*chord.Vnode, ktx *txlog.Tx, options RequestOptions) {
			for _, vns := range vmap {
				resp, err := s.transport.AppendTx(ktx, &options, vns...)
				if err != nil {
					log.Println("ERR", err)
					continue
				}

				if resp[0].Err != nil {
					log.Println("ERR", resp[0].Err)
				}
			}
		}(vm, tx, *opts)

		return nil
	}

	return fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)

}

// DeleteInode deletes the given inode.  It only deletes the inode and not the underlying data.
func (s *Difuse) DeleteInode(inode *store.Inode, options ...RequestOptions) error {
	var opts *RequestOptions
	if options != nil {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLeader}
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(inode.Serialize(fb))
	return s.appendTx(store.TxTypeSet, inode.Id, fb.Bytes[fb.Head():], opts)
}

// SetInode takes the given inode, creates a set tx and submits it based on the
// given consistency level.
func (s *Difuse) SetInode(inode *store.Inode, options ...RequestOptions) error {
	var opts *RequestOptions
	if options != nil {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLeader}
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(inode.Serialize(fb))
	return s.appendTx(store.TxTypeSet, inode.Id, fb.Bytes[fb.Head():], opts)
}

// Stat returns the inode entry for the key. By default it uses the leader consistency
func (s *Difuse) Stat(key []byte, options ...RequestOptions) (*store.Inode, error) {
	var opts *RequestOptions
	if len(options) > 0 {
		opts = &options[0]
	} else {
		opts = &RequestOptions{Consistency: ConsistencyLeader}
	}

	switch opts.Consistency {
	case ConsistencyLeader:
		l, _, _, err := s.LookupLeader(key)
		if err != nil {
			return nil, err
		}

		resp, err := s.transport.Stat(key, opts, l)
		if err != nil {
			return nil, err
		}

		if resp[0].Err != nil {
			return nil, resp[0].Err
		}

		if ind, ok := resp[0].Data.(*store.Inode); ok {
			return ind, nil
		}
		return nil, fmt.Errorf(errInvalidDataType, resp[0].Data)

	case ConsistencyLazy:
		ccfg := s.config.Chord

		vl, err := s.ring.Lookup(ccfg.NumSuccessors, key)
		if err != nil {
			return nil, err
		}

		vnb := vnodesByHost(vl)
		// Try local vnodes first
		if vns, ok := vnb[ccfg.Hostname]; ok {
			resp, er := s.transport.Stat(key, opts, vns...)
			if er == nil && resp[0].Err == nil {
				if ind, ok := resp[0].Data.(*store.Inode); ok {
					return ind, nil
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

			if resp[0].Err != nil {
				err = resp[0].Err
				continue
			}

			if ind, ok := resp[0].Data.(*store.Inode); ok {
				return ind, nil
			}
			err = fmt.Errorf(errInvalidDataType, resp[0].Data)
		}

		return nil, err
	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
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

// SetBlock sets the block data based on theh options returning the hash key
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

func (s *Difuse) keyleader(key []byte, vs []*chord.Vnode) (lvn *chord.Vnode, vm map[string][]*chord.Vnode, err error) {
	vm = vnodesByHost(vs)

	quorum := (len(vs) / 2) + 1
	lm := make(map[string][]*VnodeResponse)
	var i int

	// Get the last tx for the first n vnodes satisfying quorum.  Traverse input
	// vnode slice to maintain order
	for _, vn := range vs {
		if i > quorum {
			break
		}
		// Already visited this host and all of its vnoes
		if _, ok := lm[vn.Host]; ok {
			continue
		}
		// Get the last tx for all vn's on the host
		vns := vm[vn.Host]
		resp, e := s.transport.LastTx(key, nil, vns...)
		if e != nil {
			continue
		}
		// Add tx's to the host's response list
		lm[vn.Host] = resp
		i++
	}

	// Count votes for each tx.
	txvote := make(map[string][]string)
	for host, vl := range lm {
		for _, vr := range vl {
			var hk string
			if vr.Err != nil {
				hk = "00000000000000000000000000000000"
			} else {
				tx := vr.Data.(*txlog.Tx)
				hk = fmt.Sprintf("%x", tx.Hash())
			}

			if _, ok := txvote[hk]; !ok {
				txvote[hk] = []string{host}
			} else {
				txvote[hk] = append(txvote[hk], host)
			}
		}
	}

	// Get tx with max votes
	leaderTx, _ := maxVotes(txvote)
	// Get all hosts with this tx hash
	candidates := txvote[leaderTx]
	// Get first vn in the supplied list and candidates to elect as leader
	for _, v := range vs {
		if hasCandidate(candidates, v.Host) {
			lvn = v
			return
		}
	}

	err = fmt.Errorf("could not find leader vnode: %s", key)
	return
}
