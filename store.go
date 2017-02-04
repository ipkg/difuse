package difuse

import (
	"fmt"

	chord "github.com/euforia/go-chord"
	"github.com/ipkg/difuse/txlog"
)

// localStore is a store containing all tx backed stores for the node.  It also
// manages the consistency level of each operation.
type localStore map[string]VnodeStore

// GetStore returns a store for the given id.  If not found it returns nil
func (nls localStore) GetStore(id []byte) VnodeStore {
	if s, ok := nls[fmt.Sprintf("%x", id)]; ok {
		return s
	}
	return nil
}

/*// Snapshot snapshots a vnode store returning a Reader
func (nls localStore) Snapshot(vn *chord.Vnode) (io.ReadCloser, error) {
	st := nls.GetStore(vn.Id)
	if st == nil {
		return nil, fmt.Errorf("target vn not found: %s", vn.String())
	}

	return st.Snapshot()
}

// Restore data to the local vnode from the reader
func (nls localStore) Restore(vn *chord.Vnode, r io.Reader) error {
	st := nls.GetStore(vn.Id)
	if st == nil {
		return fmt.Errorf("target vn not found: %s", vn.String())
	}

	return st.Restore(r)
}*/

// AppendTx appends a keyed tx to the given vnodes.  All vnodes in the slice are assumed to be local vnodes.
// TODO: support consistency level
func (nls localStore) AppendTx(tx *txlog.Tx, opts *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {
	resp := make([]*VnodeResponse, len(vs))

	for i, vn := range vs {
		r := &VnodeResponse{Id: vn.Id, Data: []byte{}}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Err = store.AppendTx(tx)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	return resp, nil
}

// Stat gets a key from the local stores based on the consistency level.  All vnodes in the slice are assumed to be local vnodes
func (nls localStore) Stat(key []byte, opts *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {
	/*opts := DefaultRequestOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	var resp []*VnodeResponse

	switch opts.Consistency {
	case ConsistencyQuorum:
		q := (len(ids) / 2) + 1
		resp = make([]*VnodeResponse, q)

		for i := 0; i < q; i++ {
			id := ids[i].Id
			r := &VnodeResponse{Id: id}
			if store := nls.GetStore(id); store != nil {
				r.Data, r.Err = store.Stat(key)

			} else {
				r.Err = fmt.Errorf("target vn not found: %x", id)
			}
			resp[i] = r
		}

	case ConsistencyAll:*/
	resp := make([]*VnodeResponse, len(vs))

	for i, vn := range vs {
		r := &VnodeResponse{Id: vn.Id}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Data, r.Err = store.Stat(key)

		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	/*case ConsistencyLeader:
		// leader only
		r := &VnodeResponse{}
		if store := nls.GetStore(ids[0].Id); store != nil {
			r.Data, r.Err = store.Stat(key)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", ids[0].Id)
		}
		resp = []*VnodeResponse{r}

	default:
		return nil, errInvalidConsistencyLevel
	}*/

	return resp, nil
}

// GetTx gets a keyed tx from multiple vnodes based on the specified consistency.  All vnodes in the slice are assumed to be local vnodes
func (nls localStore) GetTx(key, txhash []byte, opts *RequestOptions, ids ...*chord.Vnode) ([]*VnodeResponse, error) {
	/*opts := DefaultRequestOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	var resp []*VnodeResponse

	switch opts.Consistency {
	case ConsistencyLeader:
		// leader only
		r := &VnodeResponse{}
		if store := nls.GetStore(ids[0].Id); store != nil {
			r.Data, r.Err = store.GetTx(key, txhash)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", ids[0].Id)
		}
		resp = []*VnodeResponse{r}

	case ConsistencyQuorum:
		q := (len(ids) / 2) + 1
		resp = make([]*VnodeResponse, q)

		for i := 0; i < q; i++ {
			id := ids[i].Id
			r := &VnodeResponse{Id: id}
			if store := nls.GetStore(id); store != nil {
				r.Data, r.Err = store.GetTx(key, txhash)

			} else {
				r.Err = fmt.Errorf("target vn not found: %x", id)
			}
			resp[i] = r
		}

	case ConsistencyAll:*/
	resp := make([]*VnodeResponse, len(ids))
	for i, vn := range ids {
		r := &VnodeResponse{Id: vn.Id}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Data, r.Err = store.GetTx(key, txhash)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	/*default:
		return nil, errInvalidConsistencyLevel
	}*/

	return resp, nil
}

// LastTx gets the last tx for a given key from multiple vnodes based on the specified consistency.  All vnodes in the
// slice are assumed to be local vnodes
func (nls localStore) LastTx(key []byte, opts *RequestOptions, ids ...*chord.Vnode) ([]*VnodeResponse, error) {
	/*opts := DefaultRequestOptions()
	if len(options) > 0 {
		opts = options[0]
	}

	var resp []*VnodeResponse

	switch opts.Consistency {
	case ConsistencyLeader:
		// leader only
		r := &VnodeResponse{}
		if store := nls.GetStore(ids[0].Id); store != nil {
			r.Data, r.Err = store.LastTx(key)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", ids[0].Id)
		}
		resp = []*VnodeResponse{r}

	case ConsistencyQuorum:
		q := (len(ids) / 2) + 1
		resp = make([]*VnodeResponse, q)

		for i := 0; i < q; i++ {
			id := ids[i].Id
			r := &VnodeResponse{Id: id}
			if store := nls.GetStore(id); store != nil {
				r.Data, r.Err = store.LastTx(key)

			} else {
				r.Err = fmt.Errorf("target vn not found: %x", id)
			}
			resp[i] = r
		}

	case ConsistencyAll:*/
	resp := make([]*VnodeResponse, len(ids))
	for i, vn := range ids {
		r := &VnodeResponse{Id: vn.Id}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Data, r.Err = store.LastTx(key)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	/*default:
		return nil, errInvalidConsistencyLevel
	}*/

	return resp, nil
}

func (nls localStore) NewTx(key []byte, vl ...*chord.Vnode) ([]*VnodeResponse, error) {
	resp := make([]*VnodeResponse, len(vl))
	for i, vn := range vl {
		r := &VnodeResponse{Id: vn.Id}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Data, r.Err = store.NewTx(key)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	return resp, nil
}

// GetBlock gets block data with the given key from multiple vnodes.  All vnodes in the slice are assumed to be local vnodes
func (nls localStore) GetBlock(key []byte, opts *RequestOptions, ids ...*chord.Vnode) ([]*VnodeResponse, error) {
	resp := make([]*VnodeResponse, len(ids))

	for i, vn := range ids {
		r := &VnodeResponse{Id: vn.Id}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Data, r.Err = store.GetBlock(key)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	return resp, nil
}

// DeleteBlock deletes the block data on vnodes given their ids.  All vnodes in the slice are assumed to be local vnodes
func (nls localStore) DeleteBlock(blk []byte, opts *RequestOptions, ids ...*chord.Vnode) ([]*VnodeResponse, error) {
	resp := make([]*VnodeResponse, len(ids))

	for i, vn := range ids {
		r := &VnodeResponse{Id: vn.Id, Data: []byte{}}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Err = store.DeleteBlock(blk)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	return resp, nil
}

// SetBlock sets the block data on vnodes given their ids.  All vnodes in the slice are assumed to be local vnodes.
// TODO: support consistency level
func (nls localStore) SetBlock(blk []byte, opts *RequestOptions, ids ...*chord.Vnode) ([]*VnodeResponse, error) {
	resp := make([]*VnodeResponse, len(ids))

	for i, vn := range ids {
		r := &VnodeResponse{Id: vn.Id}
		if store := nls.GetStore(vn.Id); store != nil {
			r.Data, r.Err = store.SetBlock(blk)
		} else {
			r.Err = fmt.Errorf("target vn not found: %x", vn.Id)
		}
		resp[i] = r
	}

	return resp, nil
}
