package difuse

import (
	"fmt"

	chord "github.com/ipkg/go-chord"
)

// isLeader returns whether this host owns the provided vnode
func (s *Difuse) isLeader(vn *chord.Vnode) bool {
	return s.config.Chord.Hostname == vn.Host
}

func (s *Difuse) keyleader(key []byte, vs []*chord.Vnode) (lvn *chord.Vnode, vm map[string][]*chord.Vnode, err error) {
	vm = vnodesByHost(vs)

	//quorum := (len(vs) / 2) + 1
	lm := make(map[string][]*VnodeResponse)
	//var i int

	// Get the last tx for the first n vnodes satisfying quorum.  Traverse input
	// vnode slice to maintain order
	for _, vn := range vs {
		/*if i > quorum {
			break
		}*/

		// Already visited this host and all of its vnoes
		if _, ok := lm[vn.Host]; ok {
			continue
		}

		// Get the last tx for all vn's on the host
		// TODO: check we have the vnodes for the host
		vns := vm[vn.Host]
		//resp, e := s.transport.LastTx(key, nil, vns...)
		resp, e := s.transport.MerkleRootTx(key, nil, vns...)
		if e != nil {
			continue
		}
		// Add tx's to the host's response list
		lm[vn.Host] = resp
		//i++
	}

	// Count votes for each tx.
	txvote := make(map[string][]string)
	for host, vl := range lm {
		for _, vr := range vl {
			var hk string
			if vr.Err != nil {
				hk = zeroHashString
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
