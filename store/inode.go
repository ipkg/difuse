package store

import (
	"encoding/json"
	"fmt"

	"github.com/btcsuite/fastsha256"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ipkg/difuse/gentypes"
	"github.com/ipkg/difuse/txlog"
)

type InodeType byte

func (i InodeType) String() string {
	var s string
	switch i {
	case FileInodeType:
		s = "file"
	case DirInodeType:
		s = "dir"
	case KeyInodeType:
		s = "key"
	default:
		s = "unknown"
	}
	return s
}

const (
	// KeyInodeType is an node that represents a key
	KeyInodeType InodeType = iota
	// FileInodeType is an node that represents a filie
	FileInodeType
	// DirInodeType is an node that represents a directory
	DirInodeType
)

// Inode represents a single unit of data around the ring.
type Inode struct {
	Id []byte
	// total size of the data
	Size int64
	// Inline data indicates that the data in Blocks is the actual
	// data rather than addresses to the data.
	Inline bool
	// Type of inode
	Type InodeType
	// This holds the address to physical data.  The address can be of any type
	// i.e. hash, key, url etc..
	Blocks [][]byte

	// merkle root of transactions made against this inode
	txroot []byte
}

// NewInode instantiates a new inode with the given id.  This is an empty inode with
// no data.
func NewInode(id []byte) *Inode {
	return &Inode{
		Id:     id,
		Type:   FileInodeType,
		Blocks: make([][]byte, 0),
		txroot: txlog.ZeroHash(),
	}
}

// NewInodeFromData instantiates a new inode setting the hash from the provided data
// to blocks
func NewInodeFromData(key, data []byte) *Inode {
	rk := NewInode(key)
	rk.Size = int64(len(data))

	sh := fastsha256.Sum256(data)
	rk.Blocks = [][]byte{sh[:]}
	return rk
}

// TxRoot returns the merkle root of all transactions performed on this vnode.
func (r *Inode) TxRoot() []byte {
	return r.txroot
}

// MarshalJSON is for user legibility
func (r *Inode) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"size":   r.Size,
		"key":    string(r.Id),
		"inline": r.Inline,
		"type":   r.Type.String(),
		"txroot": fmt.Sprintf("%x", r.txroot),
	}

	bhs := make([]string, len(r.Blocks))
	for i, v := range r.Blocks {
		bhs[i] = fmt.Sprintf("%x", v)
	}
	m["blocks"] = bhs

	return json.Marshal(m)
}

// Deserialize deserializes the flatbuffer object into a Inode
func (r *Inode) Deserialize(ind *gentypes.Inode) {

	r.Id = ind.IdBytes()
	r.Size = ind.Size()
	r.Type = InodeType(ind.Type())
	r.Inline = (ind.Inline() == byte(1))
	r.txroot = ind.RootBytes()

	l := ind.BlocksLength()
	bh := make([][]byte, l)
	for i := 0; i < l; i++ {
		var obj gentypes.ByteSlice
		ind.Blocks(&obj, i)
		// deserialize flatbuffer in reverse order to get the actual order
		bh[l-i-1] = obj.BBytes()
	}

	r.Blocks = bh
}

// Serialize serializes the struct into the flatbuffer returning the offset.
func (r *Inode) Serialize(fb *flatbuffers.Builder) flatbuffers.UOffsetT {
	obh := make([]flatbuffers.UOffsetT, len(r.Blocks))

	for i, v := range r.Blocks {
		bhp := fb.CreateByteString(v)
		gentypes.ByteSliceStart(fb)
		gentypes.ByteSliceAddB(fb, bhp)
		obh[i] = gentypes.ByteSliceEnd(fb)
	}

	gentypes.InodeStartBlocksVector(fb, len(r.Blocks))
	for _, v := range obh {
		fb.PrependUOffsetT(v)
	}
	bh := fb.EndVector(len(r.Blocks))
	kp := fb.CreateByteString(r.Id)
	rp := fb.CreateByteString(r.txroot)

	gentypes.InodeStart(fb)
	gentypes.InodeAddId(fb, kp)
	gentypes.InodeAddBlocks(fb, bh)
	gentypes.InodeAddSize(fb, r.Size)
	gentypes.InodeAddType(fb, int8(r.Type))
	gentypes.InodeAddRoot(fb, rp)
	if r.Inline {
		gentypes.InodeAddInline(fb, byte(1))
	}
	return gentypes.InodeEnd(fb)
}
