package difuse

// LocateLastTx locates all last transactions for the given key from each vnode.
func (s *Difuse) LocateLastTx(key []byte) ([]byte, []*VnodeResponse, error) {
	khash, vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, nil, err
	}

	//vbh := vnodesByHost(vs)
	out := []*VnodeResponse{}

	for _, v := range vs {
		ltx, er := s.transport.LastTx(key, &RequestOptions{Consistency: ConsistencyAll}, v)
		if er != nil {
			err = er
			continue
		}

		//for i, t := range ltx {
		ltx[0].Id = []byte(ShortVnodeID(v))
		//}

		out = append(out, ltx[0])
	}
	return khash, out, err
}

// LocateInode locates all inodes for the given key from each vnode.
func (s *Difuse) LocateInode(key []byte) ([]byte, []*VnodeResponse, error) {
	khash, vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, nil, err
	}

	//vbh := vnodesByHost(vs)
	out := []*VnodeResponse{}

	for _, v := range vs {
		ltx, er := s.transport.Stat(key, &RequestOptions{Consistency: ConsistencyAll}, v)
		if er != nil {
			err = er
			continue
		}

		//for i, t := range ltx {
		ltx[0].Id = []byte(ShortVnodeID(v))
		//}

		out = append(out, ltx...)
	}
	return khash, out, err
}

func (s *Difuse) LocateBlock(key []byte) ([]byte, []*VnodeResponse, error) {
	khash, vs, err := s.ring.Lookup(s.config.Chord.NumSuccessors, key)
	if err != nil {
		return nil, nil, err
	}

	//vbh := vnodesByHost(vs)
	out := []*VnodeResponse{}

	for _, v := range vs {
		ltx, er := s.transport.GetBlock(key, &RequestOptions{Consistency: ConsistencyAll}, v)
		if er != nil {
			err = er
			continue
		}

		//for i, t := range ltx {
		ltx[0].Id = []byte(ShortVnodeID(v))
		//}

		out = append(out, ltx...)
	}
	return khash, out, err
}
