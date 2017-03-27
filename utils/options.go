package utils

import chord "github.com/ipkg/go-chord"

const (
	// ConsistencyLazy represents the first available replica
	ConsistencyLazy uint8 = iota
	// ConsistencyAll represents all replicas
	ConsistencyAll
)

// RequestOptions are request options
type RequestOptions struct {
	Consistency uint8
	PeerSetSize int          // lookup size
	PeerSetKey  []byte       // lookup key when needed as opions
	Source      *chord.Vnode // source vnode making the request (call dependent)
}

// ResponseMeta contains response metadata where applicable
type ResponseMeta struct {
	Vnode   *chord.Vnode
	KeyHash []byte // hash of the key used for it's placement
}
