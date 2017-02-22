package txlog

import (
	"encoding/json"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ipkg/difuse/gentypes"
	merkle "github.com/ipkg/go-merkle"
)

type TxKey struct {
	key []byte

	mode KeyMode
	txs  [][]byte

	// used when key is unstable.
	root []byte
}

func NewTxKey(key []byte) *TxKey {
	return &TxKey{
		key:  key,
		mode: NormalKeyMode,
		root: ZeroHash(),
		txs:  make([][]byte, 0),
	}
}

func (t *TxKey) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"key":  string(t.key),
		"mode": t.mode.String(),
	}

	s := make([]string, len(t.txs))
	for i, v := range t.txs {
		s[i] = fmt.Sprintf("%x", v)
	}
	m["tx"] = s

	return json.Marshal(m)
}

func (t *TxKey) Key() []byte {
	return t.key
}

func (t *TxKey) MerkleRoot() []byte {
	return merkle.GenerateTree(t.txs).Root().Hash()
}

func (t *TxKey) AppendTx(id []byte) {
	t.txs = append(t.txs, id)
}

// Mode returns the current mode of the key
func (t *TxKey) Mode() KeyMode {
	return t.mode
}

// SetMode sets the mode of the key
func (t *TxKey) SetMode(m KeyMode) (err error) {

	switch m {
	case NormalKeyMode, TransitionKeyMode, TakeoverKeyMode, OfflineKeyMode:
		t.mode = m

	default:
		err = errUnknownMode

	}
	return
}

func (t *TxKey) Deserialize(obj *gentypes.TxKey) {
	t.key = obj.KeyBytes()
	t.mode = KeyMode(obj.Mode())
	t.root = obj.RootBytes()

	l := obj.TxLength()
	bh := make([][]byte, l)
	for i := 0; i < l; i++ {
		var bs gentypes.ByteSlice
		obj.Tx(&bs, i)
		// deserialize flatbuffer in reverse order to get the actual order
		bh[l-i-1] = bs.BBytes()
	}

	t.txs = bh
}

func (t *TxKey) Serialize(fb *flatbuffers.Builder) flatbuffers.UOffsetT {
	obh := make([]flatbuffers.UOffsetT, len(t.txs))
	for i, v := range t.txs {
		bhp := fb.CreateByteString(v)
		gentypes.ByteSliceStart(fb)
		gentypes.ByteSliceAddB(fb, bhp)
		obh[i] = gentypes.ByteSliceEnd(fb)
	}

	gentypes.TxKeyStartTxVector(fb, len(t.txs))
	for _, v := range obh {
		fb.PrependUOffsetT(v)
	}
	bh := fb.EndVector(len(t.txs))

	kp := fb.CreateByteString(t.key)
	rp := fb.CreateByteString(t.root)

	gentypes.TxKeyStart(fb)
	gentypes.TxKeyAddKey(fb, kp)
	gentypes.TxKeyAddMode(fb, int32(t.mode))
	gentypes.TxKeyAddRoot(fb, rp)
	gentypes.TxKeyAddTx(fb, bh)
	return gentypes.TxKeyEnd(fb)
}
