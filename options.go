package difuse

import chord "github.com/ipkg/go-chord"

// ConsistencyLevel holds the consistency configuration for an operation
type ConsistencyLevel uint8

const (
	// ConsistencyLeader ensures consistency on the leader only
	ConsistencyLeader ConsistencyLevel = iota
	// ConsistencyQuorum ensures consistency on majority of nodes
	ConsistencyQuorum
	// ConsistencyAll ensures consistency on all nodes
	ConsistencyAll
	// ConsistencyLazy picks the first available node preferring local first.
	ConsistencyLazy
)

const (
	errInvalidConsistencyLevel = "invalid consistency level: %d"
)

// RequestOptions for a given operation.
type RequestOptions struct {
	Consistency ConsistencyLevel
	NetSize     int // The number of peers to use for the operation.
}

// ReplRequest is a replication request.  It contains the source to destination vnode
// and the key that will be replicated.
type ReplRequest struct {
	Src *chord.Vnode
	Dst *chord.Vnode
	Key []byte
}
