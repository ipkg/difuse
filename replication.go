package difuse

import (
	"log"

	"github.com/ipkg/difuse/txlog"
)

/*func (s *Difuse) foo(tx *txlog.Tx) error {
	// last tx of the vnode in question
	lhash := txlog.ZeroHash()
	ltx, err := s.transport.LastTx(vn, tx.Key)
	if err == nil {
		lhash = ltx.Hash()
	}

	opts := &RequestOptions{Consistency: ConsistencyAll}

	txlist := []*txlog.Tx{tx}

	for {

		ptx, er := s.transport.GetTx(tx.Key, tx.PrevHash, opts, vn)
		if er != nil {
			err = er
			break
		}

		txlist = append(txlist, ptx)

		if txlog.EqualBytes(ltx.Hash(), ptx.PrevHash) {
			break
		}

		if isZeroHash(ptx.PrevHash) {
			err = fmt.Errorf("tx does belong to the chain")
			break
		}

	}

	if err != nil {
		return err
	}

	for i := len(txlist) - 1; i >= 0; i-- {
		resp, err := s.transport.AppendTx(txlist[i], opts, vn)
		if err != nil {
			log.Println("ERR", err)
			break
		}
	}

}*/

// Updates local tx log for the key from remote, catching up the tx log to the last tx per the remote.
// The destination vnode must be local.
func (s *Difuse) startInboundReplication() {
	for req := range s.replIn {

		if !s.isLocalVnode(req.Dst) {
			log.Println("ERR vnode is not local:", ShortVnodeID(req.Dst))
			continue
		}

		// Get seek position of local store
		var seek []byte
		lltx, err := s.transport.LastTx(req.Dst, req.Key)
		if err != nil {
			if err != txlog.ErrTxNotFound {
				log.Println("ERR", err)
				continue
			}
		} else {
			seek = lltx.Hash()
		}

		// Get transactions from source vnode until none are left.  This is due to the
		// fact that during transition other tx's could have come in.  All errors are
		// ignored as there is a final check at the end of the loop regardless of whether
		// we perform error checking.
		for {

			txs, er := s.transport.Transactions(req.Src, req.Key, seek)
			if er != nil {
				//log.Printf("ERR action=replicate-in key=%s seek=%x src=%s dst=%s msg='%v'", req.Key, seek, ShortVnodeID(req.Src), ShortVnodeID(req.Dst), er)
				//s.replOut <- &ReplRequest{Src: t.Dst, Dst: t.Src, Key: t.Key}
				//err = er
				break
			}

			if len(txs) == 0 {
				break
			}

			// append tx to local vnode store
			opts := &RequestOptions{Consistency: ConsistencyAll}
			c := 0
			for _, tx := range txs {
				if _, er = s.transport.AppendTx(tx, opts, req.Dst); er != nil {
					//log.Printf("ERR action=replicate-in key=%s tx=%x msg='%v'", tx.Key, tx.Hash()[:8], er)
					//err = er
					continue
				}
				c++
			}
			log.Printf("INF action=replicated-in key=%s txcount=%d", req.Key, c)

			seek = txs.Last().Hash()
		}

		// Check last tx of both keys to check if in-sync.
		// get local last tx
		lmr, err1 := s.transport.LastTx(req.Dst, req.Key)
		if err1 != nil {
			log.Println("ERR", err1)
			continue
		}
		// get remote last tx
		rmr, err2 := s.transport.LastTx(req.Src, req.Key)
		if err2 != nil {
			log.Println("ERR", err2)
			continue
		}

		if !txlog.EqualBytes(lmr.Hash(), rmr.Hash()) {
			// TODO: check which side's tx's contain the other sides tx
			s.replOut <- &ReplRequest{Src: req.Dst, Dst: req.Src, Key: req.Key}
		}

		// TODO: RE-VISIT

		// Update mode if last tx's are in sync

		// set local
		e := s.transport.SetMode(req.Dst, req.Key, txlog.NormalKeyMode)
		if e != nil {
			log.Println("ERR", e)
		}
		// set remote
		if e = s.transport.SetMode(req.Src, req.Key, txlog.NormalKeyMode); e != nil {
			log.Println("ERR", e)
		}

	}
}

// Replicate tx's out to other vnodes.  This only replicates keys that this node is
// the leader for.  It assumes the provided source is local and the leader.
func (s *Difuse) startOutboundReplication() {

	for req := range s.replOut {
		// should never be here
		if !s.isLocalVnode(req.Src) {
			log.Println("ERR vnode is not local:", ShortVnodeID(req.Src))
			continue
		}

		// local last tx
		sltx, err := s.transport.LastTx(req.Src, req.Key)
		if err != nil {
			log.Printf("ERR key=%s vnode=%s msg='%v'", req.Key, req.Src, err)
			continue
		}

		// seek position of remote
		var seek []byte
		if dltx, er := s.transport.LastTx(req.Dst, req.Key); er == nil {
			seek = dltx.Hash()
		}

		// log is up-to-date
		if txlog.EqualBytes(sltx.Hash(), seek) {
			continue
		}

		// TODO: check if the src tx chain is longer than the dst

		// get tx's from local src starting at the seek point
		txs, err := s.transport.Transactions(req.Src, req.Key, seek)
		if err != nil {
			log.Printf("ERR action=replicate-out key=%s src=%s dst=%s msg='%v'", req.Key, ShortVnodeID(req.Src), ShortVnodeID(req.Dst), err)
			continue
		}

		// append tx's to the dst vnode
		opts := &RequestOptions{Consistency: ConsistencyAll}
		for _, tx := range txs {
			if _, e := s.transport.AppendTx(tx, opts, req.Dst); e != nil {
				log.Println("ERR", e)
			}
		}

		// Compare the src and dst to check if they match.
		m1, err1 := s.transport.LastTx(req.Src, req.Key)
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

		log.Printf("INF action=replicate-out key=%s src=%s dst=%s", req.Key, ShortVnodeID(req.Src), ShortVnodeID(req.Dst))

		// TODO: RE-VISIT

		// set key mode back to normal if everything checks out (local)
		if err1 = s.transport.SetMode(req.Src, req.Key, txlog.NormalKeyMode); err1 != nil {
			log.Println("ERR", err1)
		}

		// set key mode back to normal for remote.
		if err2 = s.transport.SetMode(req.Dst, req.Key, txlog.NormalKeyMode); err2 != nil {
			log.Println("ERR", err1)
		}
	}
}
