package difuse

import (
	"io"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/rpc"
	"github.com/ipkg/difuse/types"
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

	/*// Init flatbuff
	fb := flatbuffers.NewBuilder(0)
	// Serialize destination vnode
	ofs := serializeByteSlice(fb, remote.Id)
	fb.Finish(ofs)
	m := &chord.Payload{Data: fb.Bytes[fb.Head():]}*/

	// Send dest. vnode
	if err = stream.SendMsg(remote); err != nil {
		return err
	}

	// Send all tx blocks from the local vnode snapshot to the remote dest. vnode.
	err = ss.Iter(func(txb *types.TxBlock) error {

		//fb.Reset()
		//ofs := txb.Serialize(fb)
		//fb.Finish(ofs)

		//payload := &chord.Payload{Data: fb.Bytes[fb.Head():]}
		if er := stream.Send(txb); er != nil {
			log.Printf("ERR msg='%v'", er)
		} else {
			log.Printf("vn=%s action=transfered key=%s count=%d", utils.ShortVnodeID(local), txb.Key, len(txb.TXs))
		}

		return nil
	})

	return err
}

// GetTxBlock retreives a transaction block from the remote vnode.
func (t *NetTransport) GetTxBlock(vn *chord.Vnode, key []byte) (*types.TxBlock, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	resp, err := out.client.GetTxBlockServe(context.Background(), &types.VnodeBytes{Vnode: vn, Bytes: key})

	t.returnConn(out)
	return resp, err
}

// ProposeTx proposes a new transaction to the remote vnode.
func (t *NetTransport) ProposeTx(vn *chord.Vnode, tx *types.Tx) error {

	out, err := t.getConn(vn.Host)
	if err != nil {
		return err
	}

	_, err = out.client.ProposeTxServe(context.Background(), &types.VnodeTx{Vnode: vn, Tx: tx})

	t.returnConn(out)
	return err
}

// GetTx gets a transaction from the remote vnode.
func (t *NetTransport) GetTx(vn *chord.Vnode, txhash []byte) (*types.Tx, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	resp, err := out.client.GetTxServe(context.Background(), &types.VnodeBytes{Vnode: vn, Bytes: txhash})

	t.returnConn(out)
	return resp, err
}

// NewTx requests a new transaction from the given vnoode.
func (t *NetTransport) NewTx(vn *chord.Vnode, key []byte) (*types.Tx, error) {
	out, err := t.getConn(vn.Host)
	if err != nil {
		return nil, err
	}

	resp, err := out.client.NewTxServe(context.Background(), &types.VnodeBytes{Vnode: vn, Bytes: key})
	t.returnConn(out)
	return resp, err
}

func (t *NetTransport) RegisterTakeoverQ(ch chan<- *TakeoverReq) {
	t.takeoverq = ch
}

// Register registers a store to a vnode.
func (t *NetTransport) Register(vn *chord.Vnode, store *VnodeStore) {
	key := vn.StringID()

	t.lock.Lock()
	t.local[key] = store
	t.lock.Unlock()
}

// GetTxBlockServe serves a tx block request.  It retreives a transaction block from the given vnode
func (t *NetTransport) GetTxBlockServe(ctx context.Context, in *types.VnodeBytes) (*types.TxBlock, error) {
	return t.local.GetTxBlock(in.Vnode, in.Bytes)
}

// GetTxServe serves a GetTx request.  It tries to retrieve a transaction from given vnode.
func (t *NetTransport) GetTxServe(ctx context.Context, in *types.VnodeBytes) (*types.Tx, error) {
	return t.local.GetTx(in.Vnode, in.Bytes)
}

// NewTxServe serves a new transaction request.  It creates a new transaction using the given vnode
// to obtain the previous hash.
func (t *NetTransport) NewTxServe(ctx context.Context, in *types.VnodeBytes) (*types.Tx, error) {
	return t.local.NewTx(in.Vnode, in.Bytes)
}

// ProposeTxServe serves a propose tx request.
func (t *NetTransport) ProposeTxServe(ctx context.Context, in *types.VnodeTx) (*types.VnodeBytes, error) {
	return &types.VnodeBytes{}, t.local.ProposeTx(in.Vnode, in.Tx)
}

// TakeoverTxBlocksServe takes over ownership of tx blocks sent to it.  This queues the fetching of tx's
// within the block which eventually get applied by the fsm.
func (t *NetTransport) TakeoverTxBlocksServe(stream rpc.DifuseRPC_TakeoverTxBlocksServeServer) error {
	// Recv vnode id
	vn := &chord.Vnode{}

	//var req chord.Payload
	err := stream.RecvMsg(vn)
	if err != nil {
		return err
	}

	//vid := gentypes.GetRootAsByteSlice(req.Data, 0)
	//vn := &chord.Vnode{Id: vid.BBytes(), Host: t.host}

	//_, err = t.local.getStore(vn)
	//if err != nil {
	//	return err
	//}

	//txl := st.(*txlog.TxLog)
	//tbstore := txl.GetTxBlockStore()

	for {
		txb, e := stream.Recv()
		if e != nil {
			if e != io.EOF {
				err = e
			}
			break
		}

		//fbtb := gentypes.GetRootAsTxBlock(pl.Data, 0)
		//txb := &txlog.TxBlock{}
		//txb.Deserialize(fbtb)

		//err = st.TakeOver(txb)
		log.Printf("INF action=begin-takeover vn=%s key=%s", utils.ShortVnodeID(vn), txb.Key)
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

	conn, err := grpc.Dial(host, grpc.WithInsecure())
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

/*func serializeByteSlice(fb *flatbuffers.Builder, b []byte) flatbuffers.UOffsetT {
	ip := fb.CreateByteString(b)
	gentypes.ByteSliceStart(fb)
	gentypes.ByteSliceAddB(fb, ip)
	return gentypes.ByteSliceEnd(fb)
}

func serializeVnodeIdTx(vid []byte, tx *types.Tx) []byte {
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
}*/
