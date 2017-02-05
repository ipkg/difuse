package txlog

import (
	"fmt"
	"log"
	"sync"
)

const (
	defaultTxBufIn = 32

	errPrevHash = "previous hash want=%x have=%x"
)

var (
	//errPrevHash = fmt.Errorf("previous hash mismatch")
	errNotFound = fmt.Errorf("not found")
)

// FSM represents the finite state machine
type FSM interface {
	Apply(ktx *Tx) error
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
	// out to the store. unbuffered as we want to block
	shutdown chan bool
	// finite state machine called when log is to be applied
	fsm FSM
}

func NewTxLog(kp Signator, store TxStore, fsm FSM) *TxLog {
	txl := &TxLog{
		fsm:      fsm,
		kp:       kp,
		store:    store,
		shutdown: make(chan bool),
		in:       make(chan *Tx, defaultTxBufIn),
		lastQTx:  make(map[string]*Tx),
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

// AppendTx to the log.  Verfiy the signature before submitting to the channel.
func (txl *TxLog) AppendTx(ktx *Tx) error {
	err := ktx.VerifySignature(txl.kp)
	if err != nil {
		return err
	}

	ltx, _ := txl.LastTx(ktx.Key)
	if ltx != nil {
		lh := ltx.Hash()
		if !EqualBytes(lh, ktx.PrevHash) {
			return fmt.Errorf(errPrevHash, lh[:8], ktx.PrevHash[:8])
		}
	} else {
		// If last tx is found make sure this tx's previous hash is zero i.e
		// the first tx for this key.
		zh := ZeroHash()
		if !EqualBytes(ktx.PrevHash, zh) {
			return fmt.Errorf(errPrevHash, zh[:8], ktx.PrevHash[:8])
		}
	}

	txl.updateQLastTx(ktx)
	// Queue tx for fsm to apply
	txl.in <- ktx

	return nil
}

// Start the txlog to process incoming transactions
func (txl *TxLog) Start() {
	// Range over incoming tx's until channel is closed.
	for ktx := range txl.in {

		if err := txl.applyTx(ktx); err != nil {
			log.Printf("action=apply status=failed key='%s' msg='%v'", ktx.Key, err)
			continue
		}
	}

	txl.shutdown <- true
}

func (txl *TxLog) updateQLastTx(ktx *Tx) {
	txl.txlock.Lock()
	txl.lastQTx[string(ktx.Key)] = ktx
	txl.txlock.Unlock()
}

// applyTx applies a transaction to the log.
func (txl *TxLog) applyTx(ktx *Tx) error {

	/*err := txl.fsm.Apply(ktx)
		if err != nil {
			return err
		}

		if err = txl.store.Add(ktx); err == nil {
			// update last tx taking into account the tx buffer
			txl.txlock.Lock()
			if v, ok := txl.lastQTx[string(ktx.Key)]; ok && EqualBytes(ktx.Hash(), v.Hash()) {
				delete(txl.lastQTx, string(ktx.Key))
			}
			txl.txlock.Unlock()
		}
	return err
	*/
	if err := txl.store.Add(ktx); err != nil {
		return err
	}
	txl.txlock.Lock()
	if v, ok := txl.lastQTx[string(ktx.Key)]; ok && EqualBytes(ktx.Hash(), v.Hash()) {
		delete(txl.lastQTx, string(ktx.Key))
	}
	txl.txlock.Unlock()

	return txl.fsm.Apply(ktx)

}

// Shutdown closes the incoming tx channel and waits for a shutdown from the loop.
func (txl *TxLog) Shutdown() {
	close(txl.in)
	<-txl.shutdown
}
