package txlog

import (
	"encoding/hex"
	"errors"
	"log"
	"sync"
	"time"

	chord "github.com/ipkg/go-chord"
)

const (
	defaultTxBuf = 32

	errPrevHash = "previous hash key=%s want=%x have=%x"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrKeyNotFound = errors.New("key not found")
	ErrTxNotFound  = errors.New("tx not found")
	ErrPrevHash    = errors.New("previous hash")
)

// FSM represents the finite state machine
type FSM interface {
	Apply(ktx *Tx) error // called when a tx has been accepted by the log
	Vnode() *chord.Vnode // vnode of the fsm is attached to.
}

// Transport interface is the network transport for transaction logs.
type Transport interface {
	// tx to broadcast and the vnode broadcasting this tx
	BroadcastTx(tx *Tx, src *chord.Vnode) error
}

type orphan struct {
	lastSeen int64
	TxSlice
}

// TxLog is a key based transaction log
type TxLog struct {
	// signer
	kp Signator
	// transaction store
	store TxStore
	// Last transaction in the log.  This is the last transaction in the queue by key and
	// not in the stable store.
	txlock  sync.RWMutex
	lastQTx map[string]*Tx

	// incoming verified transactions from the user
	in chan *Tx
	// tx's that need reconciliation.  tx's are sent to this channel when an out
	// of order tx is received.
	//rec chan *Tx

	shutdown chan bool
	// finite state machine called when log is to be applied
	fsm FSM

	// orphan tx's that do not have votes yet.
	olock   sync.RWMutex
	orphans map[string]*orphan
	// votes required before a tx can be added to the log
	reqvotes int
	// network transport
	transport Transport
}

func NewTxLog(kp Signator, store TxStore, trans Transport, fsm FSM) *TxLog {
	txl := &TxLog{
		fsm:       fsm,
		kp:        kp,
		store:     store,
		shutdown:  make(chan bool, 1),
		in:        make(chan *Tx, defaultTxBuf),
		lastQTx:   make(map[string]*Tx),
		orphans:   make(map[string]*orphan),
		reqvotes:  3,
		transport: trans,
	}

	return txl
}

// LastTx returns the last transaction for a given key in the log.  The transaction
// may be retreived from the stable store if currently not in the queue.
func (txl *TxLog) LastTx(key []byte) (*Tx, error) {
	txl.txlock.RLock()
	if v, ok := txl.lastQTx[string(key)]; ok {
		defer txl.txlock.RUnlock()
		return v, nil
	}
	txl.txlock.RUnlock()

	return txl.store.Last(key)
}

// NewTx get a new transaction. The transaction needs to be signed before making a call to AppendTx
func (txl *TxLog) NewTx(key []byte) (*Tx, error) {
	if lktx, _ := txl.LastTx(key); lktx != nil {
		return NewTx(key, lktx.Hash(), nil), nil
	}

	// Create a new key with the prev hash set to zero.
	return NewTx(key, ZeroHash(), nil), nil
}

func (txl *TxLog) verifyTx(ktx *Tx) error {

	if err := ktx.VerifySignature(txl.kp); err != nil {
		return err
	}

	ltx, _ := txl.LastTx(ktx.Key)
	if ltx != nil {
		lh := ltx.Hash()
		if !EqualBytes(lh, ktx.PrevHash) {
			//log.Printf(errPrevHash, ktx.Key, ltx.Hash()[:8], ktx.PrevHash[:8])
			return ErrPrevHash
		}
	} else {
		// If last tx is not ound make sure this tx's previous hash is zero i.e
		// the first tx for this key.
		zh := ZeroHash()
		if !EqualBytes(ktx.PrevHash, zh) {
			//log.Printf(errPrevHash, ktx.Key, zh[:8], ktx.PrevHash[:8])
			return ErrPrevHash
		}
	}
	return nil
}

// AppendTx to the log.  Verfiy the signature before submitting to the channel.
func (txl *TxLog) AppendTx(tx *Tx) error {
	// Check if we have ktx in the our store.
	_, err := txl.store.Get(tx.Key, tx.Hash())
	if err == nil {
		return nil
	}

	if err := tx.VerifySignature(txl.kp); err != nil {
		return err
	}

	return txl.queueTx(tx)

	//return nil
}

// ProposeTx adds tx to the orphan tx pool.  If we have required votes then queue the tx
// to be appended to the log and remove any orphan tx's that have the prev hash of the tx
// that was just accepted.
func (txl *TxLog) ProposeTx(tx *Tx) error {
	// check if we have it in the buffer
	//_, err := txl.LastTx(tx.Key)
	//if err == nil {
	//	return nil
	//}

	// Check if we have ktx in the our store.
	_, err := txl.store.Get(tx.Key, tx.Hash())
	if err == nil {
		return nil
	}

	if err := tx.VerifySignature(txl.kp); err != nil {
		return err
	}

	return txl.proposeTx(tx)
}

func (txl *TxLog) proposeTx(tx *Tx) error {
	txkey := hex.EncodeToString(tx.Hash())

	txl.olock.Lock()
	v, ok := txl.orphans[txkey]
	if !ok {
		txl.orphans[txkey] = &orphan{TxSlice: TxSlice{tx}, lastSeen: time.Now().Unix()}
		txl.olock.Unlock()
		return txl.transport.BroadcastTx(tx, txl.fsm.Vnode())
	}

	defer txl.olock.Unlock()

	v.TxSlice = append(v.TxSlice, tx)
	v.lastSeen = time.Now().Unix()
	txl.orphans[txkey] = v

	if len(v.TxSlice) == txl.reqvotes {
		log.Printf("action=elected tx=%x key=%s vnode=%x", tx.Hash()[:8], tx.Key, txl.fsm.Vnode().Id[:8])
		return txl.queueTx(tx)
	}

	//delete(txl.orphans, txkey)

	// remove all tx's matching the above prev hash as they are no longer valid.
	/*for k, v := range txl.orphans {
		if EqualBytes(v.First().PrevHash, tx.PrevHash) {
			delete(txl.orphans, k)
		}
	}*/

	return nil
}

func (txl *TxLog) reapOrphans() {
	for {
		time.Sleep(30 * time.Second)
		txl.reapOrphansOnce()
	}
}

func (txl *TxLog) reapOrphansOnce() {

	n := time.Now().Unix()

	txl.olock.Lock()
	defer txl.olock.Unlock()

	for k, v := range txl.orphans {
		if idle := n - v.lastSeen; idle >= 10 {
			delete(txl.orphans, k)
		}
	}
}

// Start the txlog to process incoming transactions and reconciler
func (txl *TxLog) Start() {
	// start reaping orphan tx's
	go txl.reapOrphans()

	// Range over incoming tx's until channel is closed.
	for ktx := range txl.in {

		if err := txl.applyTx(ktx); err != nil {
			log.Printf("action=apply status=failed key='%s' msg='%v'", ktx.Key, err)
			continue
		}
	}
	txl.shutdown <- true
}

// Queue tx for fsm to apply
func (txl *TxLog) queueTx(ktx *Tx) error {
	ltx, _ := txl.LastTx(ktx.Key)
	if ltx != nil {
		lh := ltx.Hash()
		if !EqualBytes(lh, ktx.PrevHash) {
			//log.Printf(errPrevHash, ktx.Key, ltx.Hash()[:8], ktx.PrevHash[:8])
			return ErrPrevHash
		}
	} else {
		// If last tx is not ound make sure this tx's previous hash is zero i.e
		// the first tx for this key.
		zh := ZeroHash()
		if !EqualBytes(ktx.PrevHash, zh) {
			//log.Printf(errPrevHash, ktx.Key, zh[:8], ktx.PrevHash[:8])
			return ErrPrevHash
		}
	}

	txl.txlock.Lock()
	txl.lastQTx[string(ktx.Key)] = ktx
	txl.txlock.Unlock()

	txl.in <- ktx

	return nil
}

// applyTx applies a transaction to the log.
func (txl *TxLog) applyTx(ktx *Tx) error {
	// Add tx to log i.e. tx stable store.
	if err := txl.store.Add(ktx); err != nil {
		return err
	}
	txl.txlock.Lock()
	if v, ok := txl.lastQTx[string(ktx.Key)]; ok && EqualBytes(ktx.Hash(), v.Hash()) {
		delete(txl.lastQTx, string(ktx.Key))
	}
	txl.txlock.Unlock()

	// Called user defined fsm
	return txl.fsm.Apply(ktx)
}

// Shutdown closes the incoming tx channel and waits for a shutdown from the loop.
func (txl *TxLog) Shutdown() {
	close(txl.in)
	//close(txl.rec)
	<-txl.shutdown
}
