package keypairs

import "math/big"

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
