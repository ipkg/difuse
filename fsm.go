package difuse

import (
	"log"

	"github.com/ipkg/difuse/txlog"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

type DummyFSM struct {
	vn *chord.Vnode
}

func (d *DummyFSM) New(vn *chord.Vnode) txlog.FSM {
	return &DummyFSM{vn: vn}
}

// Vnode returns the vnode for this store.  It satisfies the FSM interface
func (d *DummyFSM) Vnode() *chord.Vnode {
	return d.vn
}

// Apply is a placeholder to satisfy the FSM interface if one is not provided.
func (d *DummyFSM) Apply(tx *txlog.Tx) error {
	log.Printf("INF vn=%s action=fsm-apply key=%s tx=%x", utils.ShortVnodeID(d.vn), tx.Key, tx.Hash()[:8])
	return nil
}

/*// BoltFSM
type BoltFSM struct {
	vn *chord.Vnode
}

func (b *BoltFSM) Apply(tx *txlog.Tx) error {

}

func (b *BoltFSM) Vnode() *chord.Vnode {
	return b.vn
}*/
