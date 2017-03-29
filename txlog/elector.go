package txlog

import (
	"encoding/hex"
	"sync"
	"time"

	"github.com/ipkg/difuse/types"
	"github.com/ipkg/difuse/utils"
)

// orphanTx is a transaction that has not yet had enough votes i.e. hasn't been added to the log yet.
type orphanTx struct {
	lastSeen int64
	votes    int
	tx       *types.Tx
}

type txElector struct {
	mu sync.RWMutex
	m  map[string]*orphanTx

	quorum       int           // required votes
	reapInterval time.Duration // reap interval default: 30
	voteIdle     int64         // time in secs. after which vote is reaped. default: 15
}

// newTxElector manages the process of electing transactions.  quorum is the no. of votes required before
// a transaction can be appended to the log.
func newTxElector(quorum int) *txElector {
	return &txElector{
		m:            make(map[string]*orphanTx),
		reapInterval: 30 * time.Second,
		voteIdle:     15,
		quorum:       quorum,
	}
}

func (vc *txElector) reapOrphans() {
	for {
		time.Sleep(vc.reapInterval)
		vc.reapOrphansOnce()
	}
}

// reapOrphansOnce reaps orphan txs.
func (vc *txElector) reapOrphansOnce() {

	n := time.Now().Unix()

	vc.mu.Lock()
	defer vc.mu.Unlock()

	for k, v := range vc.m {
		if idle := n - v.lastSeen; idle >= vc.voteIdle {
			delete(vc.m, k)
		}
	}
}

// submit a vote returning whether the tx needs to be queued for appension, broadcasted to the network
// and/or repair/catchup is required.
func (vc *txElector) vote(lastHash []byte, tx *types.Tx) (queue, broadcast, hashMismatch bool) {

	txkey := hex.EncodeToString(tx.Hash())

	vc.mu.Lock()
	defer vc.mu.Unlock()

	v, ok := vc.m[txkey]
	if !ok {
		if !utils.EqualBytes(lastHash, tx.Header.PrevHash) {
			hashMismatch = true
			return
		}

		vc.m[txkey] = &orphanTx{tx: tx, lastSeen: time.Now().Unix(), votes: 1}
		broadcast = true
		return
	}

	v.votes++
	v.lastSeen = time.Now().Unix()
	vc.m[txkey] = v

	if v.votes == vc.quorum {
		queue = true
		return
	}

	return
}
