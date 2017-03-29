// Package txlog implements a key based transaction log.
package txlog

import (
	"fmt"
	"log"
	"sync"

	"github.com/ipkg/difuse/types"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

var (
	errTxExists = fmt.Errorf("tx exists")
	errPrevHash = fmt.Errorf("previous hash mismatch")
)

// FSM represents the finite state machine
type FSM interface {
	Apply(ktx *types.Tx) error // called when a tx has been accepted by the log
	Vnode() *chord.Vnode       // vnode of the fsm is attached to.
}

// Transport implements a network transport for the log
type Transport interface {
	ProposeTx(tx *types.Tx, opts types.RequestOptions) (*types.ResponseMeta, error)
	GetTx(hash []byte, opts types.RequestOptions) (*types.Tx, *types.ResponseMeta, error)
}

// TxLog is the core of the transaction log.  It does all of the heavy duty lifting ensuring order,
// integrity, votes etc.
type TxLog struct {
	signator types.Signator
	txstore  TxStore
	tbstore  TxBlockStore

	vc *txElector

	tlock   sync.RWMutex
	lastQTx map[string]*types.Tx

	// buffer to append tx's to the log
	appendCh chan *types.Tx
	// buffer to replay tx's for the given block
	replayBlock chan *types.TxBlock
	// buffer to walk tx's in reverse order
	walkTx chan *types.Tx

	transport Transport

	fsm FSM
}

// NewTxLog initializes a new transaction log.
func NewTxLog(signator types.Signator, tbstore TxBlockStore, txstore TxStore, trans Transport, fsm FSM) *TxLog {
	txl := &TxLog{
		lastQTx:     make(map[string]*types.Tx),
		appendCh:    make(chan *types.Tx, 32),
		walkTx:      make(chan *types.Tx, 16),
		replayBlock: make(chan *types.TxBlock, 64),
		vc:          newTxElector(4),
		txstore:     txstore,
		tbstore:     tbstore,
		fsm:         fsm,
		transport:   trans,
		signator:    signator,
	}

	return txl
}

// QueueBlockReplay submits a tx block to be replayed.
func (txl *TxLog) QueueBlockReplay(txb *types.TxBlock) {
	txl.replayBlock <- txb
}

// NewTx creates a new transaction for the given key.  It uses the previous hash or uses the zero
// hash if it is the first transaction for the key.
func (txl *TxLog) NewTx(key []byte) (*types.Tx, error) {
	// Check store to make sure key is usable
	if blk, err := txl.tbstore.Get(key); err == nil {
		if m, ok := blk.Degraded(); ok {
			return nil, fmt.Errorf("key not available - mode: %s", m)
		}
	}

	// Get last tx from log.
	lktx, _ := txl.LastTx(key)
	if lktx == nil {
		// Create a new key with the prev hash set to zero.
		return types.NewTx(key, make([]byte, 32), nil), nil
	}

	ntx := types.NewTx(key, lktx.Hash(), nil)
	ntx.Header.Height = lktx.Header.Height + 1
	return ntx, nil
}

// ProposeTx proposes a new transaction to the network.
func (txl *TxLog) ProposeTx(tx *types.Tx) error {
	var (
		th = tx.Hash()
		lh []byte
	)

	// Get TxBlock
	block, err := txl.tbstore.Get(tx.Key)
	if err == nil {
		// Check if block contains tx
		if block.ContainsTx(th) {
			return errTxExists
		}
		// Do not allow voting if block degraded
		if m, ok := block.Degraded(); ok {
			return fmt.Errorf("tx block unavailable - mode: %s", m)
		}
		// Set last tx from block
		lh = block.LastTx()
	} else {
		// TxBlock does not exist.  Set to zero
		lh = make([]byte, 32)
	}

	if err = tx.VerifySignature(txl.signator); err != nil {
		return err
	}

	// Cast a vote to see if we need to queue or broadcast the tx or if there was a hash mismatch.
	queue, bcast, mismatch := txl.vc.vote(lh, tx)
	if queue {
		log.Printf("DBG vn=%s action=elected key=%s tx=%x", utils.ShortVnodeID(txl.fsm.Vnode()), tx.Key, tx.Hash()[:8])
		// Queue tx
		mismatch = txl.queueTx(tx)
	} else if bcast {
		log.Printf("DBG vn=%s action=broadcast key=%s", utils.ShortVnodeID(txl.fsm.Vnode()), tx.Key)
		// Propose tx to the network
		_, err = txl.transport.ProposeTx(tx, types.RequestOptions{Source: txl.fsm.Vnode()})
		return err
	}

	// Queue may issue mismatch so check separately here.
	if mismatch {
		log.Printf("TODO vn=%s key=%s tx=%x msg='Check if tx is part of the tx block or submit to replay queue'",
			utils.ShortVnodeID(txl.fsm.Vnode()), tx.Key, tx.Hash()[:8])

		txl.walkTx <- tx
		return errPrevHash
	}

	return nil
}

// LastTx returns the last transaction from the log's perpsective.  This may include ones currently
// queueud in the buffer.
func (txl *TxLog) LastTx(key []byte) (*types.Tx, error) {
	// Check in-mem first
	txl.tlock.RLock()
	if v, ok := txl.lastQTx[string(key)]; ok {
		defer txl.tlock.RUnlock()
		return v, nil
	}
	txl.tlock.RUnlock()

	// Get tx block
	block, err := txl.tbstore.Get(key)
	if err != nil {
		return nil, err
	}

	// Get last tx id from block
	ltxHash := block.LastTx()
	if ltxHash == nil {
		return nil, errTxNotFound
	}
	// Return last tx from store.
	return txl.txstore.Get(ltxHash)
}

// Start starts the transaction log to allow transaction appension
func (txl *TxLog) Start() {
	go txl.vc.reapOrphans()

	go txl.startBlockReplayer()
	go txl.startTxWalker()
	go txl.startAppender()
}

func (txl *TxLog) queueTx(tx *types.Tx) (hashMismatch bool) {
	// check previous hash
	var lhash []byte
	if ltx, err := txl.LastTx(tx.Key); err == nil {
		lhash = ltx.Hash()
	} else {
		lhash = make([]byte, 32)
	}

	if !utils.EqualBytes(lhash, tx.Header.PrevHash) {
		return true
	}

	// update buffer
	txl.tlock.Lock()
	txl.lastQTx[string(tx.Key)] = tx
	txl.tlock.Unlock()

	txl.appendCh <- tx

	return false
}

func (txl *TxLog) appendTx(tx *types.Tx) error {
	// Check if we have ktx in the our store.
	_, err := txl.txstore.Get(tx.Hash())
	if err == nil {
		return nil
	}

	if err := tx.VerifySignature(txl.signator); err != nil {
		return err
	}

	if mm := txl.queueTx(tx); mm {
		return errPrevHash
	}

	return nil
}

func (txl *TxLog) applyTx(tx *types.Tx) error {
	// Add tx to log i.e. tx stable store.
	if err := txl.txstore.Set(tx); err != nil {
		return err
	}

	// check block
	block, err := txl.tbstore.Get(tx.Key)
	if err != nil {
		block = types.NewTxBlock(tx.Key)
	}
	block.AppendTx(tx.Hash())

	if err = txl.tbstore.Set(block); err != nil {
		return err
	}

	txl.tlock.Lock()
	if v, ok := txl.lastQTx[string(tx.Key)]; ok && utils.EqualBytes(tx.Hash(), v.Hash()) {
		delete(txl.lastQTx, string(tx.Key))
	}
	txl.tlock.Unlock()

	// Called user defined fsm
	return txl.fsm.Apply(tx)
}

func (txl *TxLog) startBlockReplayer() {
	opts := types.RequestOptions{}

	for txb := range txl.replayBlock {
		// Set tx block if needed and set to take over mode
		txl.tbstore.Create(txb.Key, types.TxBlockMode_TAKEOVER)

		opts.PeerSetKey = txb.Key
		tx, _, err := txl.transport.GetTx(txb.LastTx(), opts)
		if err != nil {
			log.Printf("ERR action=replay-block msg='%v'", err)
			continue
		}

		// Send to tx walker.
		txl.walkTx <- tx
	}
}

func (txl *TxLog) startAppender() {
	// Range over incoming tx's until channel is closed.
	for ktx := range txl.appendCh {

		err := txl.applyTx(ktx)
		if err != nil {
			log.Printf("action=append status=failed key='%s' msg='%v'", ktx.Key, err)
			continue
		}
	}
}

// given a tx walk backwards.
func (txl *TxLog) startTxWalker() {
	for tx := range txl.walkTx {

		var ltxID []byte
		// check if we have a last tx for the  key
		block, err := txl.tbstore.Get(tx.Key)
		if err == nil {
			ltxID = block.LastTx()
		}

		// This means get all tx's from the beginning
		if ltxID == nil {
			ltxID = make([]byte, 32)
		}

		// Get all required tx's working our way backwards.
		out, err := txl.reverseFetch(tx, ltxID)
		if err != nil {
			log.Printf("ERR action=replay key=%s msg='%v'", tx.Key, err)
			continue
		}

		if len(out) > 0 {

			// Check we have the correct log for the key. If the last fetched tx does not match the last tx
			// in the local store the key is considered diverged and requires further checking and reconciliation.

			/*c := out[len(out)-1]
			if !utils.EqualBytes(c.Hash(), ltxID) {
				log.Printf("ERR vn=%s action=replay key=%s msg='key diverged'", utils.ShortVnodeID(txl.fsm.Vnode()), tx.Key)
				log.Printf("DBG key=%s last-tx=%x fetched=%x", tx.Key, ltxID[:8], c.Hash()[:8])
				continue
			}*/

			// queue in reverse order we got the tx's in
			for i := len(out) - 1; i >= 0; i-- {
				// queue tx so checks can be performed.  we ignore prev hash mismatches here
				if er := txl.appendTx(out[i]); er != nil {
					log.Printf("ERR key=%s msg='%v'", out[i].Key, er)
				}
			}

		}

		// append tx originally received in walker q
		if er := txl.appendTx(tx); er != nil {
			log.Printf("ERR key=%s msg='%v'", tx.Key, er)
			continue
		}
		log.Printf("action=txwalk key=%s count=%d", tx.Key, len(out)+1)

		// TODO: send key to be verified and marked as normal

		if er := txl.tbstore.SetMode(tx.Key, types.TxBlockMode_NORMAL); er != nil {
			log.Printf("ERR key=%s msg='%v'", tx.Key, er)
		}

	}
}

// fetch all tx's starting from the last tx working backwards to the end hash
func (txl *TxLog) reverseFetch(startTx *types.Tx, end []byte) ([]*types.Tx, error) {
	// Get all required tx's working our way backwards.
	var (
		err   error
		out   = make([]*types.Tx, 0)
		opts  = types.RequestOptions{PeerSetKey: startTx.Key}
		phash = startTx.Header.PrevHash
	)

	for {
		// exit loop if we found our position or reached the beginning.
		if utils.IsZeroHash(phash) || utils.EqualBytes(phash, end) {
			break
		}

		t, _, er := txl.transport.GetTx(phash, opts)
		if er != nil {
			err = er
			break
		}

		out = append(out, t)

		phash = t.Header.PrevHash

	}

	return out, err
}
