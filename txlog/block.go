package txlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ipkg/difuse/gentypes"
	"github.com/ipkg/difuse/utils"
	merkle "github.com/ipkg/go-merkle"
)

// TxBlockMode represents transaction block states
type TxBlockMode int32

func (k TxBlockMode) String() string {
	switch k {
	case NormalTxBlockMode:
		return "normal"
	case TransitionTxBlockMode:
		return "transition"
	case TakeoverTxBlockMode:
		return "takeover"
	case OfflineTxBlockMode:
		return "offline"
	}
	return "unknown"
}

const (
	// NormalTxBlockMode is the normal mode of operation
	NormalTxBlockMode TxBlockMode = iota
	// TransitionTxBlockMode is the key is being transferred by a vnode.  This is set on the vnode
	// transitioning its keys to be taken over.
	TransitionTxBlockMode
	// TakeoverTxBlockMode is the key being taken-over by a vnode.  This mode is set on the vnode
	// receiving the keys.
	TakeoverTxBlockMode
	// OfflineTxBlockMode denotes that a key is offline and not usable.  This is set if a key
	// is not consistent.
	OfflineTxBlockMode
)

var errUnknownMode = errors.New("unknown mode")

// TxBlock holds all transactions for a given key.
type TxBlock struct {
	key []byte

	mode int32    // mode of the block
	txs  [][]byte // list of tx id's
	root []byte   // merkle root of tx id's
}

// NewTxBlock instantiates a new empty TxBlock.
func NewTxBlock(key []byte) *TxBlock {
	return &TxBlock{
		key:  key,
		mode: int32(NormalTxBlockMode),
		root: make([]byte, 32),
		txs:  make([][]byte, 0),
	}
}

// TxIds returns all transaction id's in this block.
func (t *TxBlock) TxIds() [][]byte {
	return t.txs
}

// LastTx returns the last transaction id in the block.
func (t *TxBlock) LastTx() []byte {
	l := len(t.txs)
	if l > 0 {
		return t.txs[l-1]
	}
	// There is none
	return nil
}

// ContainsTx returns true if the block contains the tx hash id.
func (t *TxBlock) ContainsTx(h []byte) bool {
	for _, th := range t.txs {
		if utils.EqualBytes(h, th) {
			return true
		}
	}
	return false
}

// Key returns the key for the block
func (t *TxBlock) Key() []byte {
	return t.key
}

// MerkleRoot returns the merkle root of all transactions in the block.
func (t *TxBlock) MerkleRoot() []byte {
	return t.root
}

// AppendTx appends a transaction id to the block and re-calculates the merkle root.
func (t *TxBlock) AppendTx(id []byte) {
	t.txs = append(t.txs, id)
	t.root = merkle.GenerateTree(t.txs).Root().Hash()
}

// Degraded returns whether the block is degraded and the mode.
func (t *TxBlock) Degraded() (TxBlockMode, bool) {
	m := t.Mode()
	return m, !(m == NormalTxBlockMode)
}

// Mode returns the current mode of the key
func (t *TxBlock) Mode() TxBlockMode {
	km := atomic.LoadInt32(&t.mode)
	return TxBlockMode(km)
}

// SetMode sets the mode of the key
func (t *TxBlock) SetMode(m TxBlockMode) (err error) {

	switch m {
	case TransitionTxBlockMode, TakeoverTxBlockMode:
		if ok := atomic.CompareAndSwapInt32(&t.mode, int32(NormalTxBlockMode), int32(m)); !ok {
			atomic.CompareAndSwapInt32(&t.mode, int32(OfflineTxBlockMode), int32(m))
		}

	case NormalTxBlockMode, OfflineTxBlockMode:
		atomic.StoreInt32(&t.mode, int32(m))

	default:
		err = errUnknownMode

	}
	return
}

// Deserialize deserializes the flatbuffer object
func (t *TxBlock) Deserialize(obj *gentypes.TxBlock) {
	t.key = obj.KeyBytes()
	t.mode = obj.Mode()
	t.root = obj.RootBytes()

	l := obj.TxsLength()
	bh := make([][]byte, l)
	for i := 0; i < l; i++ {
		var bs gentypes.ByteSlice
		obj.Txs(&bs, i)
		// deserialize flatbuffer in reverse order to get the actual order
		bh[l-i-1] = bs.BBytes()
	}

	t.txs = bh
}

// Serialize serializes the txkey into the flatbuffer
func (t *TxBlock) Serialize(fb *flatbuffers.Builder) flatbuffers.UOffsetT {
	obh := make([]flatbuffers.UOffsetT, len(t.txs))
	for i, v := range t.txs {
		bhp := fb.CreateByteString(v)
		gentypes.ByteSliceStart(fb)
		gentypes.ByteSliceAddB(fb, bhp)
		obh[i] = gentypes.ByteSliceEnd(fb)
	}

	gentypes.TxBlockStartTxsVector(fb, len(t.txs))
	for _, v := range obh {
		fb.PrependUOffsetT(v)
	}
	bh := fb.EndVector(len(t.txs))

	kp := fb.CreateByteString(t.key)
	rp := fb.CreateByteString(t.root)

	gentypes.TxBlockStart(fb)
	gentypes.TxBlockAddKey(fb, kp)
	gentypes.TxBlockAddMode(fb, int32(t.mode))
	gentypes.TxBlockAddRoot(fb, rp)
	gentypes.TxBlockAddTxs(fb, bh)
	return gentypes.TxBlockEnd(fb)
}

// MarshalJSON custom marshal TxBlock to be human readable.
func (t *TxBlock) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"key":  string(t.key),
		"mode": TxBlockMode(t.mode).String(),
		"root": fmt.Sprintf("%x", t.root),
	}

	s := make([]string, len(t.txs))
	for i, v := range t.txs {
		s[i] = fmt.Sprintf("%x", v)
	}
	m["tx"] = s

	return json.Marshal(m)
}
