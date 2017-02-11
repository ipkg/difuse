package difuse

import "fmt"

// LocateLastTx locates all last transactions for the given key from each vnode.
func (s *Difuse) LocateLastTx(key []byte) ([]*VnodeResponse, error) {
	vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, err
	}

	vbh := vnodesByHost(vs)
	out := []*VnodeResponse{}

	for h, v := range vbh {
		ltx, er := s.transport.LastTx(key, &RequestOptions{Consistency: ConsistencyAll}, v...)
		if er != nil {
			err = er
			continue
		}

		for i, t := range ltx {
			ltx[i].Id = []byte(fmt.Sprintf("%s/%x", h, t.Id))
		}

		out = append(out, ltx...)
	}
	return out, err
}

// LocateInode locates all inodes for the given key from each vnode.
func (s *Difuse) LocateInode(key []byte) ([]*VnodeResponse, error) {
	vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, err
	}

	vbh := vnodesByHost(vs)
	out := []*VnodeResponse{}

	for h, v := range vbh {
		ltx, er := s.transport.Stat(key, &RequestOptions{Consistency: ConsistencyAll}, v...)
		if er != nil {
			err = er
			continue
		}

		for i, t := range ltx {
			ltx[i].Id = []byte(fmt.Sprintf("%s/%x", h, t.Id))
		}

		out = append(out, ltx...)
	}
	return out, err
}
