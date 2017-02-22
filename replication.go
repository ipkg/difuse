package difuse

import (
	"log"

	"github.com/ipkg/difuse/txlog"
)

/*type replicator struct {
	config *Config
	ring   *chord.Ring
	trans  *localTransport

	ql           sync.RWMutex
	q            map[string][]*chord.Vnode // vnodes to check
	qLastUpdated time.Time                 // time last replication request was q'd

	tmr    *time.Timer
	tmrInt time.Duration
}

func newReplicator() *replicator {
	repl := &replicator{
		qLastUpdated: time.Now(),
		q:            make(map[string][]*chord.Vnode),
		tmrInt:       40 * time.Second,
	}
	repl.tmr = time.AfterFunc(repl.tmrInt, repl.startReplication)
	repl.tmr.Stop()
	return repl
}

func (r *replicator) startReplication() {
	log.Println("STARTING")

	r.ql.Lock()
	defer r.ql.Unlock()

	for _, v := range r.q {
		st, err := r.trans.local.GetStore(v[0].Id)
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		log.Printf("INF action=check entity=vnode vnode=%x", ShortVnodeID(v[0]))

		go r.replicateTx(st)
	}
	r.q = nil
}

// q replication of keys from src to dst.  src must be a local vnode
func (r *replicator) qVnodeCheck(vn *chord.Vnode) error {
	if !r.isLocalVnode(vn) {
		return errNoLocalVnode
	}

	r.tmr.Reset(r.tmrInt)

	r.ql.Lock()
	defer r.ql.Unlock()
	defer func() { r.qLastUpdated = time.Now() }()

	v, ok := r.q[vn.String()]
	if !ok {
		r.q[vn.String()] = []*chord.Vnode{vn}
		return nil
	}

	r.q[vn.String()] = append(v, vn)
	return nil
}

func (r *replicator) isLocalVnode(vn *chord.Vnode) bool {
	return r.config.Chord.Hostname == vn.Host
}*/

// transfer all keys from src for which dst is the leader for.
/*func (s *Difuse) transferLeaderKeys(src, dst *chord.Vnode) error {

srcSt, err := s.transport.local.GetStore(src.Id)
if err != nil {
	return err
}

var cnt int
err = srcSt.IterTx(func(keytxs *txlog.KeyTransactions) error {

	key := keytxs.Key()
	opts := &RequestOptions{Consistency: ConsistencyAll}

	sh := fastsha256.Sum256(key)
	// does not belong on dest if greate than dst id
	if bytes.Compare(sh[:], dst.Id) >= 0 {
		return nil
	}

	srcLtx := keytxs.Last()
	if srcLtx == nil {
		return nil
	}*/
//log.Printf("TRANSFER %s", key)

/*_, vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
if err != nil {
	return err
}
lvn := vs[0]*/

//if !txlog.EqualBytes(lvn.Id, dst.Id) {
//	return nil
//}

//log.Printf("IS LEADER %s", dst)

// get last tx from source
/*lrtx, err := s.transport.LastTx(key, opts, src)
if err != nil {
	log.Printf("WRN key=%s msg='%v'", key, err)
	return err
}
if lrtx[0].Err != nil {
	log.Printf("WRN key=%s msg='%v'", key, lrtx[0].Err)
	return lrtx[0].Err
}
lltx := lrtx[0].Data.(*txlog.Tx)*/

// The dst vnode is leader for the key.  Transfer to vnode.

//for _, vn := range vs[1:] {
// get last transaction from destination
/*var seek []byte
		rtx, err := s.transport.LastTx(dst, key)
		if err == nil {
			//log.Printf("WRN key=%s msg='%v'", key, err)
			//return err
			//if rtx[0].Err == nil {
			//log.Printf("WRN key=%s msg='%v'", key, rtx[0].Err)
			//return rtx[0].Err
			//ltx := rtx[0].Data.(*txlog.Tx)
			seek = rtx.Hash()
			//}
		}

		// keys are in-sync
		if seek != nil && txlog.EqualBytes(srcLtx.Hash(), seek) {
			return nil
		}

		// get all local transactions
		txs, err := keytxs.Transactions(seek)
		if err != nil {
			log.Printf("WRN key=%s msg='%v'", key, err)
			return err
		}

		log.Printf("INF action=transfer key=%s txcount=%d", key, len(txs))

		// send tx's
		for _, tx := range txs {
			if _, err = s.transport.AppendTx(tx, opts, dst); err != nil {
				log.Printf("WRN key=%s tx=%x msg='%v'", key, tx.Hash()[:8], err)
			}
		}
		//}
		cnt++
		return err
	})

	log.Printf("INF action=replicated src=%s dst=%s count=%d", ShortVnodeID(src), ShortVnodeID(dst), cnt)
	return err
}*/

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
/*func (s *Difuse) RepairLocalVnodes(key []byte) (*chord.Vnode, error) {
	// Get leader and vnodes
	l, vl, _, err := s.LookupLeader(key)
	if err != nil {
		return nil, err
	}

    m:=map[string]*txlog.TxKey{}

    for _,vn:=range vl {
        tk,err:=s.transport.GetTxKey(vn, key)
        if err!=nil {
            continue
        }
        m[vn.String()]= tk
    }

    // IN RPGORESS


	// Get local vnodes for this key
	//vns, ok := vm[s.config.Chord.Hostname]
	//if !ok || len(vns) == 0 {
	//	return nil, fmt.Errorf("no local vnodes: '%s'", key)
	//}

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
}*/

// Updates local tx log for the key from remote, catching up the tx log to the last tx per the remote.
func (s *Difuse) startInboundReplication() {
	for t := range s.replIn {

		//log.Printf("Queue transfer: key=%s src=%s dst=%s", t.Key, ShortVnodeID(t.Src), ShortVnodeID(t.Dst))
		localst, err := s.transport.local.GetStore(t.Dst.Id)
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		//m, e := localst.Mode(t.Key)
		//log.Println("key mode", m, e)

		lltx, err := localst.LastTx(t.Key)
		// Get seek position of local store
		var seek []byte
		if err != nil {
			if err != txlog.ErrTxNotFound {
				log.Println("ERR", err)
				continue
			}
		} else {
			seek = lltx.Hash()
		}

		// Get transactions from source vnode until none are left.  This is due to the
		// fact that during transition other tx's could have come in.
		for {

			txs, er := s.transport.Transactions(t.Key, seek, t.Src)
			if er != nil {
				log.Printf("ERR action=replicate-in key=%s seek=%x src=%s dst=%s msg='%v'", t.Key, seek, ShortVnodeID(t.Src), ShortVnodeID(t.Dst), er)
				//s.replOut <- &ReplRequest{Src: t.Dst, Dst: t.Src, Key: t.Key}
				//err = er
				break
			}

			if len(txs) == 0 {
				break
			}

			// append tx to local vnode store
			for _, tx := range txs {
				if er = localst.AppendTx(tx); er != nil {
					log.Printf("ERR action=replicate-in key=%s tx=%x msg='%v'", tx.Key, tx.Hash()[:8], er)
					//err = er
				}
			}
			log.Printf("INF action=replicated-in key=%s txcount=%d", t.Key, len(txs))

			seek = txs.Last().Hash()
		}

		// TODO: need to re-visit

		// Check merkle root of both keys to check if in-sync
		lmr, err1 := localst.MerkleRootTx(t.Key)
		if err1 != nil {
			log.Println("ERR", err1)
			continue
		}
		rmr, err2 := s.transport.MerkleRootTx(t.Src, t.Key)
		if err2 != nil {
			log.Println("ERR", err2)
			continue
		}
		// Update mode if merkles are in sync
		if txlog.EqualBytes(lmr, rmr) {
			if e := localst.SetMode(t.Key, txlog.NormalKeyMode); e != nil {
				log.Println("ERR", e)
			}
			continue
		}

		//log.Printf("key=%s src=%s dst=%s msg='merkle mismatch'", t.Key, ShortVnodeID(t.Src), ShortVnodeID(t.Dst))
		s.replOut <- &ReplRequest{Src: t.Dst, Dst: t.Src, Key: t.Key}
		//
		// TODO:
		//
		//stx,err:=s.transport.LastTx(t.Key, nil, t.Src)
		//dtx,err:=s.transport.LastTx(t.Key, nil, t.Dst)

	}
}

// Replicate tx's out to other vnodes.  This only replicates keys that this node is
// the leader for.  It assumes the provided source is local and the leader.
func (s *Difuse) startOutboundReplication() {

	for req := range s.replOut {

		localst, err := s.transport.local.GetStore(req.Src.Id)
		if err != nil {
			log.Printf("ERR msg='%v'", err)
			continue
		}

		sltx, err := localst.LastTx(req.Key)
		if err != nil {
			log.Printf("ERR key=%s vnode=%s msg='%v'", req.Key, req.Src, err)
			continue
		}

		var seek []byte
		if dltx, er := s.transport.LastTx(req.Dst, req.Key); er == nil {
			seek = dltx.Hash()
		}

		if txlog.EqualBytes(sltx.Hash(), seek) {
			continue
		}

		txs, err := localst.Transactions(req.Key, seek)
		if err != nil {
			log.Printf("ERR key=%s src=%s dst=%s msg='%v'", req.Key, ShortVnodeID(req.Src), ShortVnodeID(req.Dst), err)
			continue
		}

		opts := &RequestOptions{Consistency: ConsistencyAll}
		for _, tx := range txs {
			if _, e := s.transport.AppendTx(tx, opts, req.Dst); e != nil {
				log.Println("ERR", e)
			}
		}

		m1, err1 := localst.LastTx(req.Key)
		if err1 != nil {
			log.Println("ERR", err1)
			continue
		}

		m2, err2 := s.transport.LastTx(req.Dst, req.Key)
		if err2 != nil {
			log.Println("ERR", err2)
			continue
		}

		if !txlog.EqualBytes(m1.Hash(), m2.Hash()) {
			log.Printf("WRN key=%s src=%s/%x dst=%s/%x msg='last tx mismatch'",
				req.Key, ShortVnodeID(req.Src), m1.Hash(), ShortVnodeID(req.Dst), m2.Hash())
			continue
		}

		log.Printf("INF action=replicate-out key=%s src=%s dst=%s", req.Key, req.Src, req.Dst)

		//log.Printf("action=replicate status=ok key=%s src=%s dst=%s", req.Key, ShortVnodeID(req.Src), ShortVnodeID(req.Dst))
		if e := localst.SetMode(req.Key, txlog.NormalKeyMode); e != nil {
			log.Println("ERR", e)
		}

		/*opts := &RequestOptions{Consistency: ConsistencyAll}

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
		}*/

	}
}
