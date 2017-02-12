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

// Queue a key replication request on the given vnode.  The leader for the key is used
// as the source vnode and the provided as destination.
func (s *Difuse) requestVnodeKeyRepair(dst *chord.Vnode, key []byte) error {
	// Get leader and vnodes
	l, _, vm, err := s.LookupLeader(key)
	if err != nil {
		return err
	}

	// Make sure key belongs to vnode
	vns, ok := vm[dst.Host]
	if !ok {
		return fmt.Errorf("key '%s' does not belong to %s", key, ShortVnodeID(dst))
	}
	dstFound := false
	for _, v := range vns {
		if txlog.EqualBytes(v.Id, dst.Id) {
			dstFound = true
		}
	}
	if !dstFound {
		return fmt.Errorf("key '%s' does not belong to %s", key, ShortVnodeID(dst))
	}

	s.replQ <- &ReplRequest{Src: l, Dst: dst, Key: key}

	/*resps := []*vnodeInode{}
	opts := &RequestOptions{Consistency: ConsistencyAll}

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

	return nil
}

func (s *Difuse) startReplEngine() {

	for req := range s.replQ {

		st, err := s.transport.local.GetStore(req.Dst.Id)
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		// Get last tx from store as our seek point to get any tx's we may not have.
		var seek []byte
		ltx, err := st.LastTx(req.Key)
		if err == nil {
			seek = ltx.Hash()
		}

		// Replicate transactions from remote to the vnode store. Error intentionally
		// not caught, as it is inconsiquential
		s.transport.ReplicateTransactions(req.Key, seek, req.Src, req.Dst)
	}
}
