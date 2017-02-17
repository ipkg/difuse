package difuse

import (
	"fmt"
	"log"

	"github.com/ipkg/difuse/txlog"
	chord "github.com/ipkg/go-chord"
)

/*type vnodeInode struct {
	v *chord.Vnode
	i *store.Inode
}*/

/*func (s *Difuse) reconcile(tx *txlog.Tx) error {
	l, _, vm, err := s.LookupLeader(tx.Key)
	if err != nil {
		return nil, err
	}

	// Get local vnodes for this key
	vns, ok := vm[s.config.Chord.Hostname]
	if !ok || len(vns) == 0 {
		return nil, fmt.Errorf("no local vnodes: '%s'", tx.Key)
	}

	rltx, err := s.transport.LastTx(tx.Key, &RequestOptions{Consistency: ConsistencyLeader}, l)
	if err != nil {
		return err
	}

	if rltx[0].Err != nil {
		return rltx[0].Err
	}

	ltx := rltx[0].Data.(*txlog.Tx)

	resp, err := s.transport.LastTx(key, &RequestOptions{Consistency: ConsistencyAll}, vns...)
	if err != nil {
		return err
	}

	for _, r := range resp {
		if r.Err == nil {
            tx := r.Data.(*txlog.Tx)
    		if txlog.EqualBytes(tx.Hash(), ltx.Hash()) {
    			continue
    		}
		}




	}

}*/

// RepairLocalVnodes repairs the key for all local vnodes.  It finds the leader for
// the key and submits replications requests for all local vnodes for the key. If an
// error occurs the vnodes scoped for the repair are returned otherwise the exact
// vnodes for which requests were submitted are returned.
func (s *Difuse) RepairLocalVnodes(key []byte) ([]*chord.Vnode, error) {
	// Get leader and vnodes
	l, _, vm, err := s.LookupLeader(key)
	if err != nil {
		return nil, err
	}

	// Get local vnodes for this key
	vns, ok := vm[s.config.Chord.Hostname]
	if !ok || len(vns) == 0 {
		return nil, fmt.Errorf("no local vnodes: '%s'", key)
	}

	opts := &RequestOptions{Consistency: ConsistencyAll}

	// Get leader inode
	//lresp, err := s.transport.Stat(key, opts, l)
	lresp, err := s.transport.LastTx(key, opts, l)
	if err != nil {
		return vns, err
	}
	if lresp[0].Err != nil {
		return vns, lresp[0].Err
	}

	leaderLtx := lresp[0].Data.(*txlog.Tx)

	// Get inode from all local vnodes
	resp, err := s.transport.LastTx(key, opts, vns...)
	if err != nil {
		return vns, err
	}

	// actual request submitted
	out := []*chord.Vnode{}

	for _, r := range resp {

		if r.Err == nil {
			vtx := r.Data.(*txlog.Tx)
			if txlog.EqualBytes(vtx.Hash(), leaderLtx.Hash()) {
				continue
			}
		}

		vn := &chord.Vnode{Id: r.Id, Host: s.config.Chord.Hostname}
		out = append(out, vn)

		log.Printf("SUBMITTING %s", key)
		// If vnode contains errors or merkle roots do not match, submit replication request.
		s.replOut <- &ReplRequest{
			Src: l,
			Dst: vn,
			Key: key,
		}
	}

	delete(vm, s.config.Chord.Hostname)

	return out, nil

	/*resps := []*vnodeInode{}


	// Get inodes from all vnodes
	for h, v := range vm {
		// get inodes
		ind, err := s.transport.Stat(key, opts, v...)
		if err != nil {
			continue
		}

		for _, r := range ind {
			if r.Err != nil {
				// TODO: need to check when key is not found.
				//s.replQ <- &ReplRequest{
				//	Src: l,
				//	Dst: &chord.Vnode{Id: r.Id, Host: h},
				//	Key: key,
				//}
				//continue
			}

			resps = append(resps, &vnodeInode{
				v: &chord.Vnode{Id: r.Id, Host: h},
				i: r.Data.(*store.Inode),
			})

		}
	}

	// find leader inode
	var leaderInode *store.Inode
	for _, r := range resps {
		// set leader data
		if txlog.EqualBytes(r.v.Id, l.Id) {
			leaderInode = r.i
			break
		}
	}

	if leaderInode == nil {
		return fmt.Errorf("could not get leader data")
	}

	// submit mis matching
	for _, resp := range resps {
		if !txlog.EqualBytes(leaderInode.TxRoot(), resp.i.TxRoot()) {
			s.replQ <- &ReplRequest{Src: l, Dst: resp.v, Key: key}
		}
	}*/

}

// Updates local log for the key from remote, catching
// up the tx log to the last tx per the remote.
func (s *Difuse) startInboundReplication() {
	for t := range s.replIn {

		//log.Printf("Queue transfer: key=%s src=%s dst=%s", t.Key, ShortVnodeID(t.Src), ShortVnodeID(t.Dst))
		localst, err := s.transport.local.GetStore(t.Dst.Id)
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		lltx, err := localst.LastTx(t.Key)

		var seek []byte
		if err != nil {
			//log.Println(&err, &txlog.ErrTxNotFound)
			if err != txlog.ErrTxNotFound {
				//if err.Error() != txlog.ErrTxNotFound.Error() {
				log.Println("ERR", err)
				continue
			}
		} else {
			seek = lltx.Hash()
		}

		// get transactions from source vnode
		txs, err := s.transport.Transactions(t.Key, seek, t.Src)
		if err != nil {
			log.Printf("ERR key=%s seek=%x vnode=%s msg='%v'", t.Key, seek[:8], ShortVnodeID(t.Src), err)
			continue
		}
		// append tx to local vnode store
		for _, tx := range txs {
			if err = localst.AppendTx(tx); err != nil {
				log.Println("ERR", err)
			}
		}

	}
}

// Replicate tx's out to other vnodes.  This only replicates keys that this node is
// the leader for.  It assumes the provided source is local and the leader.
func (s *Difuse) startOutboundReplication() {

	for req := range s.replOut {
		opts := &RequestOptions{Consistency: ConsistencyAll}

		// Get last tx for src
		sr, err := s.transport.LastTx(req.Key, opts, req.Src)
		if err != nil {
			log.Println("ERR", err, req.Src)
			continue
		}
		if sr[0].Err != nil {
			log.Println("ERR", sr[0].Err, req.Src)
			continue
		}
		stx := sr[0].Data.(*txlog.Tx)

		// Get last tx for dest
		dr, err := s.transport.LastTx(req.Key, opts, req.Dst)
		if err != nil {
			log.Println("ERR", err, req.Dst)
			continue
		}
		// determine seek position.
		var seek []byte
		if dr[0].Err != nil {
			seek = txlog.ZeroHash()
		} else {
			t := dr[0].Data.(*txlog.Tx)
			seek = t.Hash()
		}

		if txlog.EqualBytes(stx.Hash(), seek) {
			continue
		}

		//log.Printf("key=%s seek=%x scr=%s dst=%s", req.Key, seek[:8], ShortVnodeID(req.Src), ShortVnodeID(req.Dst))

		st, err := s.transport.local.GetStore(req.Src.Id)
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		// get tx's from local store starting at seek to replicate out
		txs, err := st.Transactions(req.Key, seek)
		if err != nil {
			//log.Println("ERR", err)
			log.Printf("ERR key=%s seek=%x vnode=%s msg='%v'", req.Key, seek[:8], ShortVnodeID(req.Src), err)
			continue
		}
		// Append all tx's to dest. vnode
		for _, tx := range txs {
			if _, e := s.transport.AppendTx(tx, opts, req.Dst); e != nil {
				log.Println("ERR", e)
			}
		}

	}
}
