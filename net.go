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

	"github.com/ipkg/difuse/gentypes"
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

	cs        ConsistentStore     // consistent storage interface
	clock     sync.RWMutex        // outbound connection lock
	out       map[string]*outConn // outbound connections
	transferq chan<- *ReplRequest // q to send transfer keys requests to
}

// NewNetTransport instantiates a new network transport.
func NewNetTransport() *NetTransport {
	return &NetTransport{
		local: make(localStore),
		out:   make(map[string]*outConn),
	}
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

	data := serializeVnodeIdsBytes(key, vs...)
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

	data := serializeVnodeIdsBytes(blkdata, vs...)
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

	data := serializeVnodeIdsBytes(key, vs...)
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

	data := serializeVnodeIdsBytes(key, vs...)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.DeleteBlockServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) SetModeTxKey(vn *chord.Vnode, key []byte, mode txlog.KeyMode) error {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return err
	}

	payload := chord.SerializeVnodeIntBytesToPayload(vn, int(mode), key)
	if _, err = out.client.SetModeTxKeyServe(context.Background(), payload); err != nil {
		t.reapConn(out)
		return err
	}
	return nil
}

func (t *NetTransport) GetTxKey(vn *chord.Vnode, key []byte) (*txlog.TxKey, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vn)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.GetTxKeyServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	fbtk := gentypes.GetRootAsTxKey(resp.Data, 0)
	var kt txlog.TxKey
	kt.Deserialize(fbtk)
	return &kt, nil
}

// MerkleRootTx requests the transaction merkle root for the key.
func (t *NetTransport) MerkleRootTx(vn *chord.Vnode, key []byte) ([]byte, error) {

	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vn)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.MerkleRootTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	//return deserializeVnodeIdBytesErrList(resp.Data), nil
	fbbs := gentypes.GetRootAsByteSlice(resp.Data, 0)
	return fbbs.BBytes(), nil
}

// AppendTx sends a append transaction request to all vnodes on a given host
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

func (t *NetTransport) ProposeTx(tx *txlog.Tx, options *RequestOptions, vs ...*chord.Vnode) ([]*VnodeResponse, error) {

	out, err := t.getConn(vs[0].Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsTx(tx, vs)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.ProposeTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	return deserializeVnodeIdBytesErrList(resp.Data), nil
}

func (t *NetTransport) Transactions(vn *chord.Vnode, key, seek []byte) (txlog.TxSlice, error) {

	/*st, err := t.local.GetStore(local.Id)
	if err != nil {
		return err
	}*/

	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(serializeTxRequest(fb, key, seek, vn))

	req := &chord.Payload{Data: fb.Bytes[fb.Head():]}
	stream, err := out.client.TransactionsServe(context.Background(), req)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	txs := txlog.TxSlice{}
	for {
		// Receive tx starting from the seek point.
		payload, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		// Deserialize
		fbtx := gentypes.GetRootAsTx(payload.Data, 0)
		tx := deserializeTx(fbtx)
		txs = append(txs, tx)
	}

	return txs, err
	//return deserializeTxListErr(resp.Data)
}

func (t *NetTransport) GetTx(vn *chord.Vnode, key, txhash []byte) (*txlog.Tx, error) {

	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsTwoByteSlices(key, txhash, vn)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.GetTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	fbtx := gentypes.GetRootAsTx(resp.Data, 0)
	return deserializeTx(fbtx), nil
	//return deserializeVnodeIdTxErrList(resp.Data), nil
}

func (t *NetTransport) LastTx(vn *chord.Vnode, key []byte) (*txlog.Tx, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeVnodeIdsBytes(key, vn)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.LastTxServe(context.Background(), payload)
	if err != nil {
		t.reapConn(out)
		return nil, err
	}

	fbtx := gentypes.GetRootAsTx(resp.Data, 0)
	return deserializeTx(fbtx), nil
	//return deserializeVnodeIdTxErrList(resp.Data), nil
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

// TransferTxKeys issues a transfer request of keys from the local to remote vnode.
// This queues a per key replication process on the other end.
func (t *NetTransport) TransferTxKeys(local, remote *chord.Vnode) error {
	// Get local store
	st, err := t.local.GetStore(local.Id)
	if err != nil {
		return err
	}

	//if st.InodeCount() == 0 {
	//	return nil
	//}

	// Get remote conn
	out, err := t.getConn(remote.Host)
	if err != nil {
		return err
	}

	stream, err := out.client.TransferTxKeysServe(context.Background())
	if err != nil {
		t.reapConn(out)
		return err
	}

	// Buffer is built here for efficiency
	fb := flatbuffers.NewBuilder(0)

	//fb.Finish(chord.SerializeVnode(fb, remote))
	fb.Finish(serializeTransferRequest(fb, local, remote))

	// Send remote vnode id and local vnode merkle to remote
	req := &chord.Payload{Data: fb.Bytes[fb.Head():]}
	if err = stream.SendMsg(req); err != nil {
		return err
	}

	// Send keys from view rather than tx log.
	var cnt int
	//err = st.IterInodes(func(key []byte, inode *store.Inode) error {
	err = st.IterTx(func(ktx *txlog.KeyTransactions) error {

		if e := ktx.SetMode(txlog.TransitionKeyMode); e != nil {
			return e
		}

		key := ktx.Key()

		fb.Reset()
		fb.Finish(serializeByteSlice(fb, key))
		//fb.Finish(serializeIdRoot(fb, key, inode.TxRoot()))
		pl := &chord.Payload{Data: fb.Bytes[fb.Head():]}
		er := stream.Send(pl)

		cnt++
		return er
	})

	if err != nil {
		return err
	}

	log.Printf("INF action=transfer entity=keys status=ok count=%d src=%s dst=%s", cnt, ShortVnodeID(local), ShortVnodeID(remote))

	_, err = stream.CloseAndRecv()
	return err
}

// ReplicateBlocks starts replicating blocks from local vnode to remote vnode.
func (t *NetTransport) ReplicateBlocks(local, remote *chord.Vnode) error {
	// Get local store
	st, err := t.local.GetStore(local.Id)
	if err != nil {
		return err
	}
	//return if no blocks in our store.
	if st.BlockCount() == 0 {
		return nil
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

	// Send all blocks
	cnt := 0
	err = st.IterBlocks(func(h []byte, data []byte) error {
		fb.Reset()
		fb.Finish(serializeByteSlice(fb, data))
		blk := &chord.Payload{Data: fb.Bytes[fb.Head():]}
		cnt++
		return stream.Send(blk)
	})

	if err != nil {
		return err
	}

	log.Printf("action=replicate entity=blocks status=ok count=%d src=%s dst=%s", cnt, ShortVnodeID(local), ShortVnodeID(remote))
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

// RegisterTransferQ registers the replication channel to the transport.
func (t *NetTransport) RegisterTransferQ(rq chan<- *ReplRequest) {
	t.transferq = rq
}

// TransactionsServe serves transactions for given the key and the seek position in the tx log.
func (t *NetTransport) TransactionsServe(in *chord.Payload, stream netrpc.DifuseRPC_TransactionsServeServer) error {
	fbtxr := gentypes.GetRootAsTxRequest(in.Data, 0)
	st, err := t.local.GetStore(fbtxr.IdBytes())
	if err != nil {
		return err
	}

	txs, err := st.Transactions(fbtxr.KeyBytes(), fbtxr.SeekBytes())
	if err != nil {
		return err
	}

	fb := flatbuffers.NewBuilder(0)

	for _, tx := range txs {
		fb.Reset()
		fb.Finish(serializeTx(fb, tx))
		pl := &chord.Payload{Data: fb.Bytes[fb.Head():]}
		if err = stream.Send(pl); err != nil {
			return err
		}
	}

	return nil
}

func (t *NetTransport) SetModeTxKeyServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vn, m, key := chord.DeserializeVnodeIntBytes(in.Data)
	mode := txlog.KeyMode(m)

	if err := t.local.SetMode(vn, key, mode); err != nil {
		return nil, err
	}

	return &chord.Payload{}, nil
}

func (t *NetTransport) GetTxKeyServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vs, key := deserializeVnodeIdsBytes(in.Data)
	tk, err := t.local.GetTxKey(vs[0], key)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(tk.Serialize(fb))
	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// GetTxServe serves a GetTx request
func (t *NetTransport) GetTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, key, txhash := deserializeVnodeIdsTwoByteSlices(in.Data)
	tx, err := t.local.GetTx(vns[0], key, txhash)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(serializeTx(fb, tx))

	//data := serializeVnodeIdTxErrList(rsp)
	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// LastTxServe serves a LastTx request
func (t *NetTransport) LastTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, k := deserializeVnodeIdsBytes(in.Data)
	tx, err := t.local.LastTx(vns[0], k)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(serializeTx(fb, tx))
	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// MerkleRootTxServe serves a MerkleRootTx requeset
func (t *NetTransport) MerkleRootTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vs, key := deserializeVnodeIdsBytes(in.Data)
	mr, err := t.local.MerkleRootTx(vs[0], key)
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(serializeByteSlice(fb, mr))

	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// AppendTxServe serves an AppendTx request
func (t *NetTransport) AppendTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, tx := deserializeVnodeIdsTx(in.Data)
	rsps, _ := t.local.AppendTx(tx, nil, vns...)

	data := serializeVnodeIdBytesErrList(rsps)
	return &chord.Payload{Data: data}, nil
}

func (t *NetTransport) ProposeTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	vns, tx := deserializeVnodeIdsTx(in.Data)
	rsps, _ := t.local.ProposeTx(tx, nil, vns...)

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
	ind := gentypes.GetRootAsInode(in.Data, 0)
	inode.Deserialize(ind)

	// TODO: parse opts
	vn, err := t.cs.SetInode(inode, nil)

	data := chord.SerializeVnodeErr(vn, err)
	return &chord.Payload{Data: data}, nil
}

// DeleteInodeServe serves a DeleteInode request.
func (t *NetTransport) DeleteInodeServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	inode := &store.Inode{}
	ind := gentypes.GetRootAsInode(in.Data, 0)
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
	fbkey := gentypes.GetRootAsByteSlice(in.Data, 0)
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
	bs := gentypes.GetRootAsByteSlice(req.Data, 0)
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
		blkdata := gentypes.GetRootAsByteSlice(payload.Data, 0)
		if _, e = st.SetBlock(blkdata.BBytes()); e != nil {
			err = e
		}
	}

	if err != nil {
		return err
	}

	return stream.SendAndClose(&chord.Payload{})
}

// TransferTxKeysServe serves a TransferKeys request.  It takes the stream of input keys and queues them
// to be pulled on the local vnode.
func (t *NetTransport) TransferTxKeysServe(stream netrpc.DifuseRPC_TransferTxKeysServeServer) error {
	var req chord.Payload
	err := stream.RecvMsg(&req)
	if err != nil {
		return err
	}

	fbtr := gentypes.GetRootAsTransferRequest(req.Data, 0)
	sv := fbtr.Src(nil)
	dv := fbtr.Dst(nil)

	src := &chord.Vnode{Id: sv.IdBytes(), Host: string(sv.Host())}
	dst := &chord.Vnode{Id: dv.IdBytes(), Host: string(dv.Host())}

	lst, err := t.local.GetStore(dst.Id)
	if err != nil {
		return err
	}

	// TODO: set dst store into transfer mode for the keys below.
	//localSt.EnableTakeOverMode(true)

	for {
		payload, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}

		bs := gentypes.GetRootAsByteSlice(payload.Data, 0)

		// Create the key on the local store. will return an error if exists.
		lst.CreateTxKey(bs.BBytes())

		if e = lst.SetMode(bs.BBytes(), txlog.TakeoverKeyMode); e != nil {
			log.Printf("ERR msg='%v'", e)
			continue
		}

		t.transferq <- &ReplRequest{Dst: dst, Src: src, Key: bs.BBytes()}
		//log.Printf("action=accept-transfer key=%s src=%s", bs.BBytes(), ShortVnodeID(src))
	}

	// TODO: unset transfer mode
	//localSt.EnableTakeOverMode(false)

	if err != nil {
		return err
	}

	return stream.SendAndClose(&chord.Payload{})

}

/*// TransferKeysServe takes keys from the request and queues them to be replicated to the
// specified destination vnode.
func (t *NetTransport) TransferKeysServe(stream netrpc.DifuseRPC_TransferKeysServeServer) error {
	// Receive from caller vnode where incoming blocks will be written to and merkle root
	var req chord.Payload
	err := stream.RecvMsg(&req)
	if err != nil {
		return err
	}
	// Deserialize vnode id and remote store merkle.  The merkle root is the value
	// that this store needs to be at.
	tr := gentypes.GetRootAsTransferRequest(req.Data, 0)
	src := tr.Src(nil)
	sv := &chord.Vnode{Id: src.IdBytes(), Host: string(src.Host())}
	dst := tr.Dst(nil)
	dv := &chord.Vnode{Id: dst.IdBytes(), Host: string(dst.Host())}

	// Receive replication requests
	for {
		payload, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}
		// Deserialize
		fbo := gentypes.GetRootAsIdRoot(payload.Data, 0)
		t.replq <- &ReplRequest{Src: sv, Dst: dv, Key: fbo.IdBytes()}
	}

	if err != nil {
		return err
	}

	return stream.SendAndClose(&chord.Payload{})
}*/

func (t *NetTransport) getConn(host string) (*outConn, error) {
	t.clock.RLock()
	if v, ok := t.out[host]; ok {
		defer t.clock.RUnlock()
		return v, nil
	}
	t.clock.RUnlock()

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
