package difuse

import (
	"fmt"
	"io"
	"log"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/fbtypes"
	"github.com/ipkg/difuse/netrpc"
	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

type outConn struct {
	host   string
	conn   *grpc.ClientConn
	client netrpc.DifuseRPCClient
}

// NetTransport is the net rpc transport
type NetTransport struct {
	lock  sync.Mutex
	local localStore

	clock sync.Mutex
	out   map[string]*outConn
	// consistent storage interface
	cs ConsistentStore
}

func NewNetTransport() *NetTransport {
	return &NetTransport{
		local: make(localStore),
		out:   make(map[string]*outConn),
	}
}

func (t *NetTransport) getConn(host string) (*outConn, error) {
	if v, ok := t.out[host]; ok {
		return v, nil
	}

	conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithCodec(&chord.PayloadCodec{}))
	if err != nil {
		return nil, err
	}

	oc := &outConn{
		host:   host,
		conn:   conn,
		client: netrpc.NewDifuseRPCClient(conn),
	}

	t.clock.Lock()
	t.out[host] = oc
	t.clock.Unlock()

	return oc, nil

}

// reapConn closes and removes the conn from out mem pool.  This should be called
// when connections go bad.
func (t *NetTransport) reapConn(conn *outConn) {
	conn.conn.Close()

	t.clock.Lock()
	if _, ok := t.out[conn.host]; ok {
		delete(t.out, conn.host)
	}
	t.clock.Unlock()
}

// SetInode sets the given inode returning the leader for the inode and error
func (t *NetTransport) SetInode(host string, inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	out, err := t.getConn(host)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	ofs := inode.Serialize(fb)
	fb.Finish(ofs)
	payload := &chord.Payload{Data: fb.Bytes[fb.Head():]}

	resp, err := out.client.SetInodeServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return chord.DeserializeVnodeErr(resp.Data)
}

// DeleteInode deletes the given inode returning the leader for the inode and error
func (t *NetTransport) DeleteInode(host string, inode *store.Inode, options *RequestOptions) (*chord.Vnode, error) {
	out, err := t.getConn(host)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	ofs := inode.Serialize(fb)
	fb.Finish(ofs)
	payload := &chord.Payload{Data: fb.Bytes[fb.Head():]}

	resp, err := out.client.DeleteInodeServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return chord.DeserializeVnodeErr(resp.Data)
}

// Stat makes a stat request to the provided vnodes.  All vnodes per request should be long to the same host.
// This is to allow the same query to be run on multiple vnodes on a single host.
func (t *NetTransport) Stat(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {
	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.StatServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdInodeErrList(resp.Data), nil
}

// SetBlock sets blocks on the remote vnodes.   All vnodes per request should be long to the same host.
// This is to allow the same query to be run on multiple vnodes on a single host.
func (t *NetTransport) SetBlock(blkdata []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(blkdata, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.SetBlockServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) GetBlock(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.GetBlockServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) DeleteBlock(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.DeleteBlockServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) MerkleRootTx(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.MerkleRootTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) AppendTx(tx *txlog.Tx, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsTx(tx, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.AppendTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) GetTx(key, txhash []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsTwoByteSlices(key, txhash, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.GetTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdTxErrList(resp.Data), nil
}

func (t *NetTransport) LastTx(key []byte, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {
	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.LastTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdTxErrList(resp.Data), nil
}

// NewTx is a stub to satisfy the Transport interface as transactions cannot be created
// remotely
func (t *NetTransport) NewTx(key []byte, vs ...*chord.Vnode) ([]*VnodeResponse, error) {
	return nil, fmt.Errorf("cannot create new transactions remotely")
}

// LookupLeader looks up the leader for a key on the given host
func (t *NetTransport) LookupLeader(host string, key []byte) (*chord.Vnode, []*chord.Vnode, map[string][]*chord.Vnode, error) {
	out, err := t.getConn(host)
	if err != nil {
		return nil, nil, nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	p := serializeByteSlice(fb, key)
	fb.Finish(p)
	payload := &chord.Payload{Data: fb.Bytes[fb.Head():]}

	resp, err := out.client.LookupLeaderServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, nil, nil, err
	}

	vl, err := chord.DeserializeVnodeListErr(resp.Data)
	if err != nil {
		return nil, nil, nil, err
	}

	// re-generate map from vnode slice locally to save on bandwidth
	vm := vnodesByHost(vl[1:])
	return vl[0], vl[1:], vm, nil
}

// ReplicateTx replicates the last tx for each key from the local vnode to the remote vnode.
func (t *NetTransport) ReplicateTx(local, remote *chord.Vnode) error {
	// Get local store
	st, err := t.local.GetStore(local.Id)
	if err != nil {
		return err
	}
	// Get remote conn
	out, err := t.getConn(remote.Host)
	if err != nil {
		return err
	}

	stream, err := out.client.ReplicateTxServe(context.Background())
	if err != nil {
		t.reapConn(out)
		return err
	}

	// Buffer is built here for efficiency
	fb := flatbuffers.NewBuilder(0)

	// Build vnode id flatbuffer.
	fb.Finish(serializeByteSlice(fb, remote.Id))

	// Send vnode id
	req := &chord.Payload{Data: fb.Bytes[fb.Head():]}
	if err = stream.SendMsg(req); err != nil {
		return err
	}

	// TODO:
	// recieve merkle

	var cnt int
	err = st.IterTx(func(k []byte, kts *txlog.KeyTransactions) error {
		// TODO:
		// calcuate diff based on merkle and send delta

		// Send transactions
		for _, tx := range kts.TxSlice {

			fb.Reset()
			fb.Finish(serializeTx(fb, tx))
			pl := &chord.Payload{Data: fb.Bytes[fb.Head():]}

			if er := stream.Send(pl); er != nil {
				log.Println("ERR replicating tx:", er)
			}
		}

		cnt++
		return nil
	})

	if err != nil {
		return err
	}

	log.Printf("action=replicated count=%d src=%s dst=%s", cnt, shortID(local), shortID(remote))

	_, err = stream.CloseAndRecv()
	return err
}

// ReplicateBlocks starts cloning blocks from local vnode to remote vnode.
func (t *NetTransport) ReplicateBlocks(local, remote *chord.Vnode) error {
	// Get local store
	st, err := t.local.GetStore(local.Id)
	if err != nil {
		return err
	}
	// Get remote conn
	out, err := t.getConn(remote.Host)
	if err != nil {
		return err
	}
	// Get client
	stream, err := out.client.ReplicateBlocksServe(context.Background())
	if err != nil {
		t.reapConn(out)
		return err
	}

	// Buffer is built here for efficiency
	fb := flatbuffers.NewBuilder(0)

	// build vnode id flatbuffer
	fb.Finish(serializeByteSlice(fb, remote.Id))
	req := &chord.Payload{Data: fb.Bytes[fb.Head():]}
	// Send vnode id
	if err = stream.SendMsg(req); err != nil {
		return err
	}

	// TODO: send missing blocks only

	// Send blocks
	err = st.IterBlocks(func(h []byte, data []byte) error {
		fb.Reset()
		fb.Finish(serializeByteSlice(fb, data))
		blk := &chord.Payload{Data: fb.Bytes[fb.Head():]}
		return stream.Send(blk)
	})

	if err != nil {
		return err
	}

	_, err = stream.CloseAndRecv()
	return err
}

// RegisterVnode registers a store to a vnode.
func (t *NetTransport) RegisterVnode(vn *chord.Vnode, store VnodeStore) {
	key := vn.String()
	t.lock.Lock()
	t.local[key] = store
	t.lock.Unlock()
}

// Register registers a consistent store to the transport
func (t *NetTransport) Register(cs ConsistentStore) {
	t.cs = cs
}

// GetTxServe serves a GetTx request
func (t *NetTransport) GetTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, key, txhash := deserializeVnodeIdsTwoByteSlices(in.Data)
	rsp, _ := t.local.GetTx(key, txhash, nil, vns...)

	data := serializeVnodeIdTxErrList(rsp)
	return &chord.Payload{Data: data}, nil
}

// LastTxServe serves a LastTx request
func (t *NetTransport) LastTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, k := deserializeVnodeIdsBytes(in.Data)
	txs, _ := t.local.LastTx(k, nil, vns...)

	data := serializeVnodeIdTxErrList(txs)
	return &chord.Payload{Data: data}, nil
}

// MerkleRootTxServe serves a MerkleRootTx requeset
func (t *NetTransport) MerkleRootTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vs, key := deserializeVnodeIdsBytes(in.Data)
	rsps, _ := t.local.MerkleRootTx(key, nil, vs...)

	data := serializeVnodeIdBytesErrList(rsps)
	return &chord.Payload{Data: data}, nil
}

// AppendTxServe serves an AppendTx request
func (t *NetTransport) AppendTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, tx := deserializeVnodeIdsTx(in.Data)
	rsps, _ := t.local.AppendTx(tx, nil, vns...)

	data := serializeVnodeIdBytesErrList(rsps)
	return &chord.Payload{Data: data}, nil
}

// StatServe serves a Stat request
func (t *NetTransport) StatServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, k := deserializeVnodeIdsBytes(in.Data)
	rsp, _ := t.local.Stat(k, nil, vns...)

	data := serializeVnodeIdInodeErrList(rsp)
	return &chord.Payload{Data: data}, nil
}

// SetInodeServe serves a SetInode request.
func (t *NetTransport) SetInodeServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	inode := &store.Inode{}
	ind := fbtypes.GetRootAsInode(in.Data, 0)
	inode.Deserialize(ind)

	// TODO: parse opts
	vn, err := t.cs.SetInode(inode, nil)

	data := chord.SerializeVnodeErr(vn, err)
	return &chord.Payload{Data: data}, nil
}

// DeleteInodeServe serves a DeleteInode request.
func (t *NetTransport) DeleteInodeServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	inode := &store.Inode{}
	ind := fbtypes.GetRootAsInode(in.Data, 0)
	inode.Deserialize(ind)

	// TODO: parse opts
	vn, err := t.cs.DeleteInode(inode, nil)

	data := chord.SerializeVnodeErr(vn, err)
	return &chord.Payload{Data: data}, nil
}

// GetBlockServe serves a GetBlock request
func (t *NetTransport) GetBlockServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, k := deserializeVnodeIdsBytes(in.Data)
	rsp, _ := t.local.GetBlock(k, nil, vns...)

	data := serializeVnodeIdBytesErrList(rsp)
	return &chord.Payload{Data: data}, nil
}

func (t *NetTransport) SetBlockServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, val := deserializeVnodeIdsBytes(in.Data)
	rsp, _ := t.local.SetBlock(val, nil, vns...)

	data := serializeVnodeIdBytesErrList(rsp)
	return &chord.Payload{Data: data}, nil
}

func (t *NetTransport) DeleteBlockServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, val := deserializeVnodeIdsBytes(in.Data)
	rsp, _ := t.local.DeleteBlock(val, nil, vns...)

	data := serializeVnodeIdBytesErrList(rsp)
	return &chord.Payload{Data: data}, nil
}

func (t *NetTransport) LookupLeaderServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	fbkey := fbtypes.GetRootAsByteSlice(in.Data, 0)
	l, vl, _, err := t.cs.LookupLeader(fbkey.BBytes())
	list := append([]*chord.Vnode{l}, vl...)
	data := chord.SerializeVnodeListErr(list, err)

	return &chord.Payload{Data: data}, nil
}

// ReplicateBlocksServe accepts blocks from the stream and adds them the specified vnode. If
// any errors occur, then the last error is returned i.e. cloning will continue even
// though some of the blocks may not be written.
// TODO: use merkel tree to calculate overall block delta and replicate missing blocks..
func (t *NetTransport) ReplicateBlocksServe(stream netrpc.DifuseRPC_ReplicateBlocksServeServer) error {
	// Receive vnode from caller where incoming blocks will be written to.
	var req chord.Payload
	err := stream.RecvMsg(&req)
	if err != nil {
		return err
	}
	// Deserialize vnode id
	bs := fbtypes.GetRootAsByteSlice(req.Data, 0)
	// Get vnode store
	st, err := t.local.GetStore(bs.BBytes())
	if err != nil {
		return err
	}
	// Receive blocks
	for {
		payload, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		// Deserialize block data.
		blkdata := fbtypes.GetRootAsByteSlice(payload.Data, 0)
		if _, e = st.SetBlock(blkdata.BBytes()); e != nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	return stream.SendAndClose(&chord.Payload{})
}

// ReplicateTxServe serves a ReplicateTx request.
// TODO: use merkel tree to calculate transaction delta and replicate only that.
func (t *NetTransport) ReplicateTxServe(stream netrpc.DifuseRPC_ReplicateTxServeServer) error {
	// Receive vnode from caller where incoming blocks will be written to.
	var req chord.Payload
	err := stream.RecvMsg(&req)
	if err != nil {
		return err
	}
	// Deserialize vnode id
	bs := fbtypes.GetRootAsByteSlice(req.Data, 0)
	// Get vnode store
	st, err := t.local.GetStore(bs.BBytes())
	if err != nil {
		return err
	}

	// TODO:
	// receive keys
	// respond with associated hash

	// Receive transactions from remote
	for {
		payload, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		// Deserialize tx
		fbtx := fbtypes.GetRootAsTx(payload.Data, 0)
		tx := deserializeTx(fbtx)
		// Append tx to store
		if e = st.AppendTx(tx); e != nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	return stream.SendAndClose(&chord.Payload{})
}
