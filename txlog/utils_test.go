package txlog

import "testing"

func Test_concat(t *testing.T) {
	arr := [][]byte{
		[]byte("block"),
		[]byte("chain"),
		[]byte("concat"),
		[]byte("example"),
		[]byte("four"),
	}

	s := concat(arr...)
	if string(s) != "blockchainconcatexamplefour" {
		t.Fatalf("mismatch %s", s)
	}
}
