package difuse

import chord "github.com/ipkg/go-chord"

// ConsistencyLevel holds the consistency configuration for an operation
type ConsistencyLevel uint8

/*func (cl ConsistencyLevel) String() string {
	switch cl {
	case ConsistencyLeader:
		return "leader"

	case ConsistencyQuorum:
		return "quorum"

	case ConsistencyAll:
		return "all"

	default:
		return fmt.Sprintf("%d", cl)
	}
}*/

const (
	// ConsistencyLeader ensures consistency on the leader only
	ConsistencyLeader ConsistencyLevel = iota
	// ConsistencyQuorum ensures consistency on majority of nodes
	ConsistencyQuorum
	// ConsistencyAll ensures consistency on all nodes
	ConsistencyAll
	// ConsistencyLazy picks the first available node
	ConsistencyLazy
)

const (
	errInvalidConsistencyLevel = "invalid consistency level: %d"
)

// RequestOptions for a given operation.
type RequestOptions struct {
	Consistency ConsistencyLevel
}

// DefaultRequestOptions sets the consistency to leader only.
//func DefaultRequestOptions() RequestOptions {
//	return RequestOptions{Consistency: ConsistencyLeader}
//}

/*// Serialize serializes the struct to the given flatbuffer returning the offset
func (ro *RequestOptions) Serialize(fb *flatbuffers.Builder) flatbuffers.UOffsetT {
	fbtypes.RequestOptionsStart(fb)
	fbtypes.RequestOptionsAddConsistency(fb, int8(ro.Consistency))
	return fbtypes.RequestOptionsEnd(fb)
}*/

type ReplRequest struct {
	Src *chord.Vnode
	Dst *chord.Vnode
	Key []byte
}
