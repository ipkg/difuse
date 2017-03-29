package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ipkg/difuse/utils"
	merkle "github.com/ipkg/go-merkle"
)

var errUnknownMode = errors.New("unknown mode")

// NewTxBlock instantiates a new empty TxBlock.
func NewTxBlock(key []byte) *TxBlock {
	return &TxBlock{
		Key:  key,
		Mode: int32(TxBlockMode_NORMAL),
		Root: make([]byte, 32),
		TXs:  make([][]byte, 0),
	}
}

// LastTx returns the last transaction id in the block.
func (t *TxBlock) LastTx() []byte {
	l := len(t.TXs)
	if l > 0 {
		return t.TXs[l-1]
	}
	// There is none
	return nil
}

// ContainsTx returns true if the block contains the tx hash id.
func (t *TxBlock) ContainsTx(h []byte) bool {
	for _, th := range t.TXs {
		if utils.EqualBytes(h, th) {
			return true
		}
	}
	return false
}

// AppendTx appends a transaction id to the block and re-calculates the merkle root.
func (t *TxBlock) AppendTx(id []byte) {
	t.TXs = append(t.TXs, id)
	t.Root = merkle.GenerateTree(t.TXs).Root().Hash()
}

// Degraded returns whether the block is degraded and the mode.
func (t *TxBlock) Degraded() (TxBlockMode, bool) {
	m := t.ReadMode()
	return m, !(m == TxBlockMode_NORMAL)
}

// ReadMode atomically returns the current mode of the key
func (t *TxBlock) ReadMode() TxBlockMode {
	km := atomic.LoadInt32(&t.Mode)
	return TxBlockMode(km)
}

// SetMode sets the mode of the key
func (t *TxBlock) SetMode(m TxBlockMode) (err error) {

	switch m {
	case TxBlockMode_TRANSITION, TxBlockMode_TAKEOVER:
		if ok := atomic.CompareAndSwapInt32(&t.Mode, int32(TxBlockMode_NORMAL), int32(m)); !ok {
			atomic.CompareAndSwapInt32(&t.Mode, int32(TxBlockMode_OFFLINE), int32(m))
		}

	case TxBlockMode_NORMAL, TxBlockMode_OFFLINE:
		atomic.StoreInt32(&t.Mode, int32(m))

	default:
		err = errUnknownMode

	}
	return
}

// MarshalJSON custom marshal TxBlock to be human readable.
func (t *TxBlock) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"key":  string(t.Key),
		"mode": TxBlockMode(t.Mode).String(),
		"root": fmt.Sprintf("%x", t.Root),
	}

	s := make([]string, len(t.TXs))
	for i, v := range t.TXs {
		s[i] = fmt.Sprintf("%x", v)
	}
	m["tx"] = s

	return json.Marshal(m)
}
