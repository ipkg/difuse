package difuse

import (
	"fmt"
	"log"

	"github.com/ipkg/difuse/txlog"
	chord "github.com/ipkg/go-chord"
)

// BroadcastTx broadcasts the transaction to a set of peers minus the source.
func (s *Difuse) BroadcastTx(tx *txlog.Tx, src *chord.Vnode) error {
	_, vl, err := s.ring.Lookup(s.config.Chord.NumSuccessors, tx.Key)
	if err != nil {
		return err
	}

	opts := &RequestOptions{Consistency: ConsistencyAll}

	rch := make(chan *VnodeResponse, 3)

	for _, v := range vl {
		//log.Printf("Broadcast src=%s dst=%s", ShortVnodeID(src), ShortVnodeID(v))
		go func(vn *chord.Vnode) {
			resp, er := s.transport.ProposeTx(tx, opts, vn)
			if er != nil {
				rch <- &VnodeResponse{Id: []byte(ShortVnodeID(vn)), Err: err}
				return
			}

			// Submit key check as vn may be out of sync
			if resp[0].Err != nil && resp[0].Err.Error() == "previous hash" {
				resp[0].Err = nil
				s.replOut <- &ReplRequest{Src: src, Dst: vn, Key: tx.Key}
			}

			resp[0].Id = []byte(ShortVnodeID(vn))
			rch <- resp[0]
		}(v)
	}

	var (
		c      int
		quorum = (s.config.Chord.NumSuccessors / 2) + 1
		l      = len(vl) - 1
	)

	for i := 0; i < l; i++ {
		if c == quorum {
			break
		}

		resp := <-rch
		if resp.Err == nil || resp.Err.Error() == "tx exists" {
			c++
			continue
		}

		log.Printf("ERR vnode=%s key=%s msg='%v'", resp.Id, tx.Key, resp.Err)
		err = resp.Err

		//if c == l {
		//err = nil
		//	break
		//}

		/*select {
		/*case <-okCh:
			//c++
		case er := <-errCh:
			log.Printf("ERR vnode=%s key=%s msg='%v'", ShortVnodeID(src), tx.Key, er)
			err = er
		}*/

	}

	return err
}

// shared function for delete and set tx
func (s *Difuse) proposeTx(txtype byte, key, data []byte, opts *RequestOptions) (*chord.Vnode, error) {
	/*_, vl, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, err
	}*/
	l, _, _, err := s.LookupLeader(key)
	if err != nil {
		return nil, err
	}

	if !s.isLocalVnode(l) {
		return l, ErrNotLeader
	}

	//vm := vnodesByHost(vl)
	//lvns, ok := vm[s.config.Chord.Hostname]
	//if !ok {
	//	return vl[0], errNoLocalVnode
	//}
	//vn := lvns[0]

	rsp, err := s.transport.NewTx(key, l)
	if err != nil {
		return l, err
	} else if rsp[0].Err != nil {
		return l, rsp[0].Err
	}

	tx := rsp[0].Data.(*txlog.Tx)
	tx.Data = append([]byte{txtype}, data...)
	if err = tx.Sign(s.signator); err != nil {
		return l, err
	}

	var prsp []*VnodeResponse
	if prsp, err = s.transport.ProposeTx(tx, nil, l); err == nil {
		if prsp[0].Err != nil {
			err = prsp[0].Err
		}
	}

	return l, err
}

// appendTx appends a transaction to the log based on the consistency.  If this node is not the leader
// for the key, the leader vnode and error are returned otherwise just the leader vnode. This always
// processes leader first then remainder based on consistency
func (s *Difuse) appendTx(txtype byte, key, data []byte, opts *RequestOptions) (*chord.Vnode, error) {

	l, _, vm, err := s.LookupLeader(key)
	if err != nil {
		return nil, err
	}

	// If we are not the leader ie we do not have the vnode locally return the leader and a not-leader error
	if !s.isLocalVnode(l) {
		return l, ErrNotLeader
	}

	// Get new tx from leader
	rsp, err := s.transport.NewTx(key, l)
	if err != nil {
		return l, err
	} else if rsp[0].Err != nil {
		return l, rsp[0].Err
	}

	tx, _ := rsp[0].Data.(*txlog.Tx)
	tx.Data = append([]byte{txtype}, data...)
	if err = tx.Sign(s.signator); err != nil {
		return l, err
	}

	// Append the new tx on all vnodes on the leader host
	vns := vm[l.Host]
	resp, err := s.transport.AppendTx(tx, opts, vns...)
	if err != nil {
		return l, err
	}
	// Check errors
	for _, rp := range resp {
		if rp.Err != nil {
			return l, rp.Err
		}
	}

	delete(vm, l.Host)

	switch opts.Consistency {

	/*case ConsistencyQuorum:
	quorum := (len(vl) / 2) + 1
	i := 1
	for _, vns := range vm {
		if i >= quorum {
			go func(ktx *txlog.Tx, o RequestOptions, vs []*chord.Vnode) {
				r, e := s.transport.AppendTx(ktx, &o, vs...)
				if e != nil {
					//go s.RepairLocalVnodes(tx.Key)
					log.Printf("ERR key=%s %v", key, e)
					return
				}
				for _, rr := range r {
					if rr.Err != nil {
						log.Printf("ERR key=%s %v", key, rr.Err)
					}
				}

			}(tx, *opts, vns)
		} else {
			r, e := s.transport.AppendTx(tx, opts, vns...)
			if e != nil {
				err = e
				log.Printf("ERR key=%s %v", key, e)
				//go s.RepairLocalVnodes(tx.Key)
				continue
			}
			for _, rr := range r {
				if rr.Err != nil {
					err = rr.Err
					log.Printf("ERR key=%s %v", key, rr.Err)
				}
			}

			i++
		}
	}

	return l, err*/

	/*case ConsistencyLeader:

	go func(vmap map[string][]*chord.Vnode, ktx *txlog.Tx, options RequestOptions) {
		for _, vns := range vmap {
			s.transport.AppendTx(ktx, &options, vns...)
		}

	}(vm, tx, *opts)

	return l, nil*/

	case ConsistencyAll:
		for h, vns := range vm {

			resp, e := s.transport.AppendTx(tx, opts, vns...)
			if e != nil {
				log.Printf("ERR key=%s tx=%x msg='%v'", tx.Key, tx.Hash()[:8], e)
				err = e
				continue
			}

			for _, r := range resp {
				if r.Err == nil {
					continue
				}
				log.Printf("ERR vnode=%s key=%s tx=%x msg='%v'", ShortVnodeID(&chord.Vnode{Id: r.Id, Host: h}), tx.Key, tx.Hash()[:8], r.Err)

				//if r.Err == txlog.ErrPrevHash {
				//log.Println("SUBMITTING")
				//s.replOut <- &ReplRequest{Src: l, Dst: &chord.Vnode{Id: r.Id, Host: h}, Key: key}
				//	continue
				//}

				err = r.Err
			}

		}
		return l, err
	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}
