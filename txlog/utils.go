package txlog

import "math/big"

// ZeroHash returns a 32 byte hash with all zeros
func ZeroHash() []byte {
	return make([]byte, 32)
}

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

func joinBigInt(expectedLen int, bigs ...*big.Int) *big.Int {
	bs := []byte{}
	for i, b := range bigs {
		by := b.Bytes()
		dif := expectedLen - len(by)
		if dif > 0 && i != 0 {
			by = append(arrayOfBytes(dif, 0), by...)
		}
		bs = append(bs, by...)
	}

	b := new(big.Int).SetBytes(bs)
	return b
}

func splitBigInt(b *big.Int, parts int) []*big.Int {
	bs := b.Bytes()
	if len(bs)%2 != 0 {
		bs = append([]byte{0}, bs...)
	}

	l := len(bs) / parts
	as := make([]*big.Int, parts)

	for i := range as {
		as[i] = new(big.Int).SetBytes(bs[i*l : (i+1)*l])
	}

	return as

}

// create an array filled with b
func arrayOfBytes(i int, b byte) (p []byte) {
	for i != 0 {
		p = append(p, b)
		i--
	}
	return
}

// EqualBytes returns whether 2 byte slices have the same data
func EqualBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func IsZeroHash(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
