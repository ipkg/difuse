package difuse

import (
	"io"
	"log"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/gentypes"
	"github.com/ipkg/difuse/rpc"
	"github.com/ipkg/difuse/txlog"
	"github.com/ipkg/difuse/utils"
)

type outConn struct {
	host     string
	conn     *grpc.ClientConn
	client   rpc.DifuseRPCClient
	lastUsed int64
}

// NetTransport is the net rpc transport
type NetTransport struct {
	host string // host for this node used to re-construct vnodes.

	lock  sync.Mutex          // lock for local stores
	local localStore          // map of local vnode stores.
	clock sync.RWMutex        // outbound connection lock
	out   map[string]*outConn // outbound connections

	takeoverq chan<- *TakeoverReq
}

// NewNetTransport instantiates a new network transport.
func NewNetTransport(host string) *NetTransport {
	nt := &NetTransport{
		host:  host,
		local: make(localStore),
		out:   make(map[string]*outConn),
	}

	go nt.reapConns()

	return nt
}

// TransferTxBlocks transfers tx blocks from a local to remote vnode.
func (t *NetTransport) TransferTxBlocks(local, remote *chord.Vnode) error {
	st, err := t.local.getStore(local)
	if err != nil {
		return err
	}

	// Snapshot existing vnode store to use as consistent view being sent to remote.
	ss, err := st.tbstore.Snapshot()
	if err != nil {
		return err
	}

	conn, err := t.getConn(remote.Host)
	if err != nil {
		return err
	}

	// Signal take over.
	stream, err := conn.client.TakeoverTxBlocksServe(context.Background())
	if err != nil {
		return err
	}

	// Init flatbuff
	fb := flatbuffers.NewBuilder(0)
	// Serialize destination vnode
	ofs := serializeByteSlice(fb, remote.Id)
	fb.Finish(ofs)
	m := &chord.Payload{Data: fb.Bytes[fb.Head():]}

	// Send dest. vnode
	if err = stream.SendMsg(m); err != nil {
		return err
	}

	// Send all tx blocks from the local vnode snapshot to the remote dest. vnode.
	err = ss.Iter(func(txb *txlog.TxBlock) error {

		fb.Reset()
		ofs := txb.Serialize(fb)
		fb.Finish(ofs)

		payload := &chord.Payload{Data: fb.Bytes[fb.Head():]}
		if er := stream.Send(payload); er != nil {
			log.Printf("ERR msg='%v'", er)
		} else {
			log.Printf("vn=%s action=transfered key=%s count=%d", utils.ShortVnodeID(local), txb.Key(), len(txb.TxIds()))
		}

		return nil
	})

	return err
}

// GetTxBlock retreives a transaction block from the remote vnode.
func (t *NetTransport) GetTxBlock(vn *chord.Vnode, key []byte) (*txlog.TxBlock, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeTwoByteSlices(vn.Id, key)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.GetTxBlockServe(context.Background(), payload)
	if err != nil {
		return nil, err
	}

	t.returnConn(out)

	fbtk := gentypes.GetRootAsTxBlock(resp.Data, 0)
	var kt txlog.TxBlock
	kt.Deserialize(fbtk)
	return &kt, nil
}

// ProposeTx proposes a new transaction to the remote vnode.
func (t *NetTransport) ProposeTx(vn *chord.Vnode, tx *txlog.Tx) error {

	out, err := t.getConn(vn.Host)
	if err != nil {
		return err
	}

	data := serializeVnodeIdTx(vn.Id, tx)
	payload := &chord.Payload{Data: data}

	_, err = out.client.ProposeTxServe(context.Background(), payload)

	t.returnConn(out)

	return err
}

// GetTx gets a transaction from the remote vnode.
func (t *NetTransport) GetTx(vn *chord.Vnode, txhash []byte) (*txlog.Tx, error) {

	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeTwoByteSlices(vn.Id, txhash)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.GetTxServe(context.Background(), payload)
	if err != nil {
		return nil, err
	}

	t.returnConn(out)

	fbtx := gentypes.GetRootAsTx(resp.Data, 0)
	var tx txlog.Tx
	tx.Deserialize(fbtx)

	return &tx, nil
}

// NewTx requests a new transaction from the given vnoode.
func (t *NetTransport) NewTx(vn *chord.Vnode, key []byte) (*txlog.Tx, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	data := serializeTwoByteSlices(vn.Id, key)
	payload := &chord.Payload{Data: data}

	resp, err := out.client.NewTxServe(context.Background(), payload)
	if err != nil {
		return nil, err
	}

	t.returnConn(out)

	fbtx := gentypes.GetRootAsTx(resp.Data, 0)
	var tx txlog.Tx
	tx.Deserialize(fbtx)

	return &tx, nil
}

func (t *NetTransport) RegisterTakeoverQ(ch chan<- *TakeoverReq) {
	t.takeoverq = ch
}

// Register registers a store to a vnode.
func (t *NetTransport) Register(vn *chord.Vnode, store *VnodeStore) {
	key := vn.String()

	t.lock.Lock()
	t.local[key] = store
	t.lock.Unlock()
}

// GetTxBlockServe serves a tx block request.  It retreives a transaction block from the given vnode
func (t *NetTransport) GetTxBlockServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	bs := gentypes.GetRootAsTwoByteSlices(in.Data, 0)

	tk, err := t.local.GetTxBlock(&chord.Vnode{Id: bs.B1Bytes()}, bs.B2Bytes())
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(tk.Serialize(fb))
	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// GetTxServe serves a GetTx request.  It tries to retrieve a transaction from given vnode.
func (t *NetTransport) GetTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	bs := gentypes.GetRootAsTwoByteSlices(in.Data, 0)

	tx, err := t.local.GetTx(&chord.Vnode{Id: bs.B1Bytes()}, bs.B2Bytes())
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(tx.Serialize(fb))

	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// NewTxServe serves a new transaction request.  It creates a new transaction using the given vnode
// to obtain the previous hash.
func (t *NetTransport) NewTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	bs := gentypes.GetRootAsTwoByteSlices(in.Data, 0)

	tx, err := t.local.NewTx(&chord.Vnode{Id: bs.B1Bytes()}, bs.B2Bytes())
	if err != nil {
		return nil, err
	}

	fb := flatbuffers.NewBuilder(0)
	fb.Finish(tx.Serialize(fb))

	return &chord.Payload{Data: fb.Bytes[fb.Head():]}, nil
}

// ProposeTxServe serves a propose tx request.
func (t *NetTransport) ProposeTxServe(ctx context.Context, in *chord.Payload) (*chord.Payload, error) {
	obj := gentypes.GetRootAsVidTx(in.Data, 0)
	txobj := obj.Tx(nil)

	var tx txlog.Tx
	tx.Deserialize(txobj)

	if err := t.local.ProposeTx(&chord.Vnode{Id: obj.VidBytes()}, &tx); err != nil {
		return nil, err
	}

	return &chord.Payload{Data: []byte{}}, nil
}

// TakeoverTxBlocksServe takes over ownership of tx blocks sent to it.  This queues the fetching of tx's
// within the block which eventually get applied by the fsm.
func (t *NetTransport) TakeoverTxBlocksServe(stream rpc.DifuseRPC_TakeoverTxBlocksServeServer) error {
	// Recv vnode id
	var req chord.Payload
	err := stream.RecvMsg(&req)
	if err != nil {
		return err
	}

	vid := gentypes.GetRootAsByteSlice(req.Data, 0)
	vn := &chord.Vnode{Id: vid.BBytes(), Host: t.host}

	//_, err = t.local.getStore(vn)
	//if err != nil {
	//	return err
	//}

	//txl := st.(*txlog.TxLog)
	//tbstore := txl.GetTxBlockStore()

	for {
		pl, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}

		fbtb := gentypes.GetRootAsTxBlock(pl.Data, 0)
		txb := &txlog.TxBlock{}
		txb.Deserialize(fbtb)

		//err = st.TakeOver(txb)
		log.Printf("INF action=begin-takeover vn=%s key=%s", utils.ShortVnodeID(vn), txb.Key())
		t.takeoverq <- &TakeoverReq{Dst: vn, TxBlock: txb}
		//if e = t.local.QueueBlockReplay(vn, txb); e != nil {
		//	log.Printf("ERR msg='%v'", e)
		//}
	}

	return err
}

func (t *NetTransport) returnConn(conn *outConn) {
	t.clock.Lock()
	if _, ok := t.out[conn.host]; ok {
		t.out[conn.host].lastUsed = time.Now().Unix()
	}
	t.clock.Unlock()
}

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
		host:     host,
		conn:     conn,
		client:   rpc.NewDifuseRPCClient(conn),
		lastUsed: time.Now().Unix(),
	}

	t.clock.Lock()
	t.out[host] = oc
	t.clock.Unlock()

	return oc, nil

}

// reapConns reaps idle connections every 30 seconds
func (t *NetTransport) reapConns() {
	for {
		time.Sleep(30 * time.Second)
		t.reapOnce()
	}
}

// reapOnce does a one time reap across all connections, closing and removing any that have been idle
// for more than 2 mins.
func (t *NetTransport) reapOnce() {

	now := time.Now().Unix()

	t.clock.Lock()
	for k, v := range t.out {
		// expire all older than 2 mins.
		if (now - v.lastUsed) > 120 {
			v.conn.Close()
			delete(t.out, k)
		}
	}
	t.clock.Unlock()
}

func serializeByteSlice(fb *flatbuffers.Builder, b []byte) flatbuffers.UOffsetT {
	ip := fb.CreateByteString(b)
	gentypes.ByteSliceStart(fb)
	gentypes.ByteSliceAddB(fb, ip)
	return gentypes.ByteSliceEnd(fb)
}

func serializeVnodeIdTx(vid []byte, tx *txlog.Tx) []byte {
	fb := flatbuffers.NewBuilder(0)
	vp := fb.CreateByteString(vid)
	txp := tx.Serialize(fb)

	gentypes.VidTxStart(fb)
	gentypes.VidTxAddVid(fb, vp)
	gentypes.VidTxAddTx(fb, txp)
	ofs := gentypes.VidTxEnd(fb)
	fb.Finish(ofs)
	return fb.Bytes[fb.Head():]
}

func serializeTwoByteSlices(b1, b2 []byte) []byte {
	fb := flatbuffers.NewBuilder(0)
	p1 := fb.CreateByteString(b1)
	p2 := fb.CreateByteString(b2)

	gentypes.TwoByteSlicesStart(fb)
	gentypes.TwoByteSlicesAddB1(fb, p1)
	gentypes.TwoByteSlicesAddB2(fb, p2)
	ofs := gentypes.TwoByteSlicesEnd(fb)
	fb.Finish(ofs)
	return fb.Bytes[fb.Head():]
}
