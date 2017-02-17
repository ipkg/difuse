package difuse

import (
	"fmt"
	"log"

	"github.com/ipkg/difuse/txlog"
	chord "github.com/ipkg/go-chord"
)

func (s *Difuse) BroadcastTx(tx *txlog.Tx, src *chord.Vnode) error {
	_, vl, err := s.ring.Lookup(s.config.Chord.NumSuccessors, tx.Key)
	if err != nil {
		return err
	}

	// remove supplied source from list
	var fl []*chord.Vnode
	for i, v := range vl {
		if txlog.EqualBytes(v.Id, src.Id) {
			fl = append(vl[:i], vl[i+1:]...)
			break
		}
	}
	if fl == nil {
		return fmt.Errorf("key %s does not belong src vn %s", tx.Key, ShortVnodeID(src))
	}

	//log.Printf("action=broadcast key=%s tx=%x src=%s net=%d", tx.Key, tx.Hash()[:8], ShortVnodeID(src), len(fl))

	//l := len(fl) - 1
	//ch := make(chan *VnodeResponse, l+1)
	opts := &RequestOptions{Consistency: ConsistencyAll}

	/*go func(l []*chord.Vnode) {

	        for _, v := range l {
				if _, er := s.transport.ProposeTx(tx, opts, v); er != nil {
					log.Println("ERR", er)
				}
			}
		}(fl)*/

	okCh := make(chan bool)
	errCh := make(chan error)

	for _, v := range fl {
		go func(vn *chord.Vnode) {
			if _, er := s.transport.ProposeTx(tx, opts, vn); er != nil {
				errCh <- er
				//log.Println("ERR", er)
			} else {
				okCh <- true
			}
		}(v)
	}

	var c, t int
	quorum := (len(fl) / 2) + 1
	l := len(fl)
	for {
		if c == quorum {
			err = nil
			break
		} else if t == l {
			break
		}

		select {
		case <-okCh:
			c++
		case er := <-errCh:
			log.Println("ERR", er)
			err = er
		}

		t++
	}
	return err

	/*i := 0
	for resp := range ch {
		err = mergeErrors(err, resp.Err)
		i++
		if i == l {
			break
		}
	}

	return err*/
	//return nil
}

func (s *Difuse) proposeTx(txtype byte, key, data []byte, opts *RequestOptions) (*chord.Vnode, error) {
	_, vl, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, err
	}

	vm := vnodesByHost(vl)
	lvns, ok := vm[s.config.Chord.Hostname]
	if !ok {
		return vl[0], errNoLocalVnode
	}
	vn := lvns[0]

	rsp, err := s.transport.NewTx(key, vn)
	if err != nil {
		return vn, err
	} else if rsp[0].Err != nil {
		return vn, rsp[0].Err
	}

	tx := rsp[0].Data.(*txlog.Tx)
	tx.Data = append([]byte{txtype}, data...)
	if err = tx.Sign(s.signator); err != nil {
		return vn, err
	}

	_, err = s.transport.ProposeTx(tx, nil, vn)
	return vn, err
}

// appendTx appends a transaction to the log based on the consistency.  If this node is not the leader
// for the key, the leader vnode and error are returned otherwise just the leader vnode. This always
// processes leader first then remainder based on consistency
func (s *Difuse) appendTx_DEPRECATE(txtype byte, key, data []byte, opts *RequestOptions) (*chord.Vnode, error) {

	l, _, vm, err := s.LookupLeader(key)
	if err != nil {
		return nil, err
	}

	// If we are not the leader return the leader and a not-leader error
	if !s.isLeader(l) {
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
		for host, vns := range vm {

			resp, e := s.transport.AppendTx(tx, opts, vns...)
			if e != nil {
				err = e
				continue
			}

			for _, r := range resp {
				if r.Err == nil {
					continue
				}
				// queue replication/check
				s.replOut <- &ReplRequest{Src: l, Dst: &chord.Vnode{Id: r.Id, Host: host}, Key: key}
			}

		}
		return l, err
	}

	return nil, fmt.Errorf(errInvalidConsistencyLevel, opts.Consistency)
}
