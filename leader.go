package difuse

import (
	"fmt"
	"log"

	"github.com/ipkg/difuse/txlog"
	chord "github.com/ipkg/go-chord"
)

// isLeader returns whether this host owns the provided vnode
func (s *Difuse) isLeader(vn *chord.Vnode) bool {
	return s.config.Chord.Hostname == vn.Host
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
		//resp, e := s.transport.LastTx(key, nil, vns...)
		resp, e := s.transport.MerkleRootTx(key, nil, vns...)
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
				//tx := vr.Data.(*txlog.Tx)
				//hk = fmt.Sprintf("%x", tx.Hash())
				mr := vr.Data.([]byte)
				hk = fmt.Sprintf("%x", mr)
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

// appendTx appends a transaction to the log based on the consistency.  If this node is not the leader
// for the key the leader vnode and a not leader error is returned otherwise just the leader vnode
// is returned.
func (s *Difuse) appendTx(txtype byte, key, data []byte, opts *RequestOptions) (*chord.Vnode, error) {

	l, _, vm, err := s.LookupLeader(key)
	if err != nil {
		return nil, err
	}

	// If we are not the leader return the leader and a not-leader error
	if !s.isLeader(l) {
		return l, ErrNotLeader
	}

	switch opts.Consistency {
	case ConsistencyLeader:

		// Get new tx from leader
		rsp, err := s.transport.NewTx(key, l)
		if err != nil {
			return l, err
		}
		if rsp[0].Err != nil {
			return l, rsp[0].Err
		}
		tx, ok := rsp[0].Data.(*txlog.Tx)
		if !ok {
			return l, fmt.Errorf(errInvalidDataType, tx)
		}

		tx.Data = append([]byte{txtype}, data...)
		if err = tx.Sign(s.signator); err != nil {
			return l, err
		}
		// Append the new tx
		vns := vm[l.Host]
		resp, err := s.transport.AppendTx(tx, opts, vns...)
		if err != nil {
			return l, err
		}
		if resp[0].Err != nil {
			return l, resp[0].Err
		}

		delete(vm, l.Host)

		go func(vmap map[string][]*chord.Vnode, ktx *txlog.Tx, options RequestOptions) {
			for _, vns := range vmap {
				resp, err := s.transport.AppendTx(ktx, &options, vns...)
				if err != nil {
					log.Printf("ERR key=%s msg='%v'", ktx.Key, err)
					continue
				}
				for _, rsp := range resp {
					if rsp.Err != nil {
						log.Printf("ERR key=%s vn=%x msg='%v'", ktx.Key, rsp.Id[:8], resp[0].Err)
					}
				}
			}
		}(vm, tx, *opts)

		return l, nil
	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}
