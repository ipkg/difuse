package txlog

func concat(pieces ...[]byte) []byte {
	sz := 0
	for _, p := range pieces {
		sz += len(p)
	}

	buf := make([]byte, sz)

	i := 0
	for _, p := range pieces {
		copy(buf[i:], p)
		i += len(p)
	}
	return buf
}
