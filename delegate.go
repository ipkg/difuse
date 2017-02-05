package difuse

import (
	"log"

	"github.com/ipkg/difuse/store"
	chord "github.com/ipkg/go-chord"
)

// Init initializes a log backed datastore for the given vnode
func (s *Difuse) Init(local *chord.Vnode) {
	vstore := store.NewMemLoggedStore(local, s.signator)
	s.transport.Register(local, vstore)
}

// NewPredecessor is called when a new predecessor is found
func (s *Difuse) NewPredecessor(local, remoteNew, remotePrev *chord.Vnode) {
	// skip local
	if local.Host == remoteNew.Host {
		return
	}

	// TODO: check if we have remoteNew.Host key
	// TODO: queue clone rather than running it now.
	if err := s.transport.ReplicateBlocks(local, remoteNew); err != nil {
		log.Printf("ERR Block replication failed: %s --> %s %v", shortID(local), shortID(remoteNew), err)
	}

	// TODO: queue rather than running right away
	if err := s.transport.ReplicateTx(local, remoteNew); err != nil {
		log.Printf("action=replicate status=failed src=%s dst=%s msg='%v'", shortID(local), shortID(remoteNew), err)
	} else {
		log.Printf("action=replicate status=ok src=%s dst=%s", shortID(local), shortID(remoteNew))
	}
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
