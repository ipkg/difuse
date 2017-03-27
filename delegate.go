package difuse

import (
	"log"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/utils"
)

// Init is the chord delegate
func (s *Difuse) Init(vn *chord.Vnode) {
	//vns := NewVnodeStore(vn)
	//s.transport.Register(vn, vns)
}

// NewPredecessor is called when a new predecessor is found
func (s *Difuse) NewPredecessor(local, remoteNew, remotePrev *chord.Vnode) {
	log.Printf("INF action=predecessor src=%s dst=%s", utils.ShortVnodeID(local), utils.ShortVnodeID(remoteNew))

	// Skip local
	if local.Host == remoteNew.Host {
		return
	}
	//log.Printf("Stores: %d", len(s.transport.local))

	s.tm.transferq <- &transferReq{src: local, dst: remoteNew}

	/*if err := s.transport.TransferTxBlocks(local, remoteNew); err != nil {
		log.Printf("ERR action=transfer src=%s dst=%s msg='%v'", utils.ShortVnodeID(local), utils.ShortVnodeID(remoteNew), err)
	} else {
		log.Printf("INF action=transfer src=%s dst=%s", utils.ShortVnodeID(local), utils.ShortVnodeID(remoteNew))
	}*/

	//s.tm.queue(&transferReq{src: local, dst: remoteNew})
}

// Leaving is called when local node is leaving the ring
func (s *Difuse) Leaving(local, pred, succ *chord.Vnode) {
	//log.Printf("DBG [chord] Leaving local=%s succ=%s", shortID(local), shortID(succ))
}

// PredecessorLeaving is called when a predecessor leaves
func (s *Difuse) PredecessorLeaving(local, remote *chord.Vnode) {
	//log.Printf("DBG [chord] PredecessorLeaving local=%s remote=%s", shortID(local), shortID(remote))
}

// SuccessorLeaving is called when a successor leaves
func (s *Difuse) SuccessorLeaving(local, remote *chord.Vnode) {
	//log.Printf("DBG [chord] SuccessorLeaving local=%s remote=%s", shortID(local), shortID(remote))
}

// Shutdown is called when the node is shutting down
func (s *Difuse) Shutdown() {
	log.Println("[chord] Shutdown")
}
