package difuse

import (
	"github.com/ipkg/difuse/txlog"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

/*// VnodeStore implements a store for a single vnode
type VnodeStore interface {
	NewTx(key []byte) (*txlog.Tx, error)
	GetTx(id []byte) (*txlog.Tx, error)
	ProposeTx(tx *txlog.Tx) error

	GetTxBlock(key []byte) (*txlog.TxBlock, error)
	//IterTxBlock(func(*txlog.TxBlock) error) error

	//Snapshot() (VnodeStore, error)
}*/

// Transport is the transport interface for various rpc calls
type Transport interface {
	NewTx(vn *chord.Vnode, key []byte) (*txlog.Tx, error)
	GetTx(vn *chord.Vnode, txhash []byte) (*txlog.Tx, error)
	ProposeTx(vn *chord.Vnode, tx *txlog.Tx) error

	GetTxBlock(vn *chord.Vnode, key []byte) (*txlog.TxBlock, error)
	TransferTxBlocks(local, remote *chord.Vnode) error

	Register(*chord.Vnode, *VnodeStore)

	RegisterTakeoverQ(chan<- *TakeoverReq)
}

// FSMFactory implements an interface to generate FSM's for earch shard.
type FSMFactory interface {
	New(*chord.Vnode) txlog.FSM
}

type Difuse struct {
	conf      *Config         // overall config including chord
	transport *localTransport // transport for rpc calls against vnode store rpc's
	cs        *consistentTransport

	tm *transferMgr
}

// NewDifuse instantiates a new Difuse instance with a signator and the given config and network transport
func NewDifuse(conf *Config, remote Transport) *Difuse {
	d := &Difuse{
		conf:      conf,
		transport: newLocalTransport(remote),
	}

	// 1.5x of vnodes
	bufsize := conf.Chord.NumSuccessors + (conf.Chord.NumSuccessors / 2)
	d.tm = newTransferMgr(bufsize, d.transport)

	d.transport.RegisterTakeoverQ(d.tm.takeoverq)

	return d
}

// SignTx signs the given transaction
func (d *Difuse) SignTx(tx *txlog.Tx) error {
	return tx.Sign(d.conf.Signator)
}

// ProposeTx proposes a new transaction to the network.
func (d *Difuse) ProposeTx(tx *txlog.Tx, opts utils.RequestOptions) (*utils.ResponseMeta, error) {
	return d.cs.ProposeTx(tx, opts)
}

// NewTx returns a new transaction using an available vnode for the previous hash.
func (d *Difuse) NewTx(key []byte, opts utils.RequestOptions) (*txlog.Tx, *utils.ResponseMeta, error) {
	return d.cs.NewTx(key, opts)
}

// RegisterChord registers the chord ring and transport.  It starts by getting all local vnodes and
// initializes each vnode store.
func (d *Difuse) RegisterChord(ring *chord.Ring, trans chord.Transport) error {
	// Init consistent transport with options..
	d.cs = newConsistentTransport(d.conf.Chord, ring, d.transport)

	// Get local vnodes
	lvns, err := trans.ListVnodes(d.conf.Chord.Hostname)
	if err != nil {
		return err
	}

	// Initialize stores for each vnode.
	for _, vn := range lvns {
		/*txstore := txlog.NewBoltTxStore(d.conf.DataDir, vn.String())
		if er := txstore.Open(0600); er != nil {
			return er
		}*/
		fsm := d.conf.FSM.New(vn)
		txstore := txlog.NewMemTxStore()
		vstore := NewVnodeStore(txstore, d.cs, fsm)

		d.transport.Register(vn, vstore)
	}

	//d.adminServer = &httpServer{cs: d.cs}
	//go http.ListenAndServe(d.conf.HTTPAddr, d.adminServer)

	// Register with tansfer/takeover manager
	d.tm.cs = d.cs
	// Start takeover/transfer mgr.
	go d.tm.start()

	return nil
}
