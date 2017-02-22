package txlog

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ipkg/difuse/gentypes"
	merkle "github.com/ipkg/go-merkle"
)

// KeyMode represents states a key can have.
type KeyMode int32

func (k KeyMode) String() string {
	switch k {
	case NormalKeyMode:
		return "normal"
	case TransitionKeyMode:
		return "transition"
	case TakeoverKeyMode:
		return "takeover"
	case OfflineKeyMode:
		return "offline"
	}
	return "unknown"
}

const (
	// NormalKeyMode is the normal mode of operation
	NormalKeyMode KeyMode = iota
	// TransitionKeyMode is the key is being transferred by a vnode.  This is set on the vnode
	// transitioning its keys to be taken over.
	TransitionKeyMode
	// TakeoverKeyMode is the key being taken-over by a vnode.  This mode is set on the vnode
	// receiving the keys.
	TakeoverKeyMode
	// OfflineKeyMode denotes that a key is offline and not usable.  This is set if a key
	// is not consistent.
	OfflineKeyMode
)

type TxKey struct {
	key []byte

	mode int32
	txs  [][]byte

	// used when key is unstable.
	root []byte
}

func NewTxKey(key []byte) *TxKey {
	return &TxKey{
		key:  key,
		mode: int32(NormalKeyMode),
		root: ZeroHash(),
		txs:  make([][]byte, 0),
	}
}

func (t *TxKey) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"key":  string(t.key),
		"mode": KeyMode(t.mode).String(),
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
	km := atomic.LoadInt32(&t.mode)
	return KeyMode(km)
}

func (t *TxKey) Degraded() bool {
	return !(t.Mode() == NormalKeyMode)
}

// SetMode sets the mode of the key
func (t *TxKey) SetMode(m KeyMode) (err error) {

	switch m {
	case NormalKeyMode, TransitionKeyMode, TakeoverKeyMode, OfflineKeyMode:
		im := int32(m)
		atomic.StoreInt32(&t.mode, im)

	default:
		err = errUnknownMode

	}
	return
}

func (t *TxKey) Deserialize(obj *gentypes.TxKey) {
	t.key = obj.KeyBytes()
	t.mode = obj.Mode()
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

// Serialize serializes the txkey into the flatbuffer
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
