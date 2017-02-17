package difuse

import (
	"log"

	"github.com/ipkg/difuse/store"
	chord "github.com/ipkg/go-chord"
)

// Init initializes a log backed datastore for the given vnode
func (s *Difuse) Init(local *chord.Vnode) {
	vstore := store.NewMemLoggedStore(local, s.signator, s)
	s.transport.RegisterVnode(local, vstore)
}

// NewPredecessor is called when a new predecessor is found
func (s *Difuse) NewPredecessor(local, remoteNew, remotePrev *chord.Vnode) {
	// skip local
	if local.Host == remoteNew.Host {
		return
	}

	// TODO: queue rather than running right away
	/*if err := s.transport.TransferKeys(local, remoteNew); err != nil {
		log.Printf("action=transfer status=failed src=%s dst=%s msg='%v'", ShortVnodeID(local), ShortVnodeID(remoteNew), err)
	}

	if err := s.transport.ReplicateBlocks(local, remoteNew); err != nil {
		log.Printf("action=replicate-blocks status=failed msg='%v'", err)
	}*/

	log.Printf("INF action=transfer src=%s dst=%s", ShortVnodeID(local), ShortVnodeID(remoteNew))
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
