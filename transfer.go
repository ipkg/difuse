package difuse

import (
	"log"

	"github.com/ipkg/difuse/types"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

type transferReq struct {
	src *chord.Vnode
	dst *chord.Vnode
}

type TakeoverReq struct {
	Src     *chord.Vnode
	Dst     *chord.Vnode
	TxBlock *types.TxBlock
}

type transferMgr struct {
	transport *localTransport
	// transfer of key from local to remote vnode
	transferq chan *transferReq
	// accept keys from a remote transfer to takeover
	takeoverq chan *TakeoverReq

	cs *consistentTransport
}

func newTransferMgr(bufsize int, trans *localTransport) *transferMgr {
	return &transferMgr{
		transport: trans,
		transferq: make(chan *transferReq, bufsize),
		takeoverq: make(chan *TakeoverReq, 8),
	}
}

/*func (tm *transferMgr) queue(tr *transferReq) {
	tm.transferq <- tr
}*/

func (tm *transferMgr) start() {
	go tm.startProcessingTransferQ()
	go tm.startProcessingTakeoverQ()
}

func (tm *transferMgr) startProcessingTransferQ() {
	for req := range tm.transferq {
		// transfer tx blocks
		if err := tm.transport.TransferTxBlocks(req.src, req.dst); err != nil {
			log.Printf("ERR action=transfer src=%s dst=%s msg='%v'", utils.ShortVnodeID(req.src), utils.ShortVnodeID(req.dst), err)
			continue
		}
		//log.Printf("INF action=transfer src=%s dst=%s", utils.ShortVnodeID(req.src), utils.ShortVnodeID(req.dst))
	}
}

func (tm *transferMgr) startProcessingTakeoverQ() {

	//opts := utils.RequestOptions{}

	for txr := range tm.takeoverq {
		/*st, err := tm.transport.local.getStore(txr.Dst)
		if err != nil {
			log.Printf("ERR action=takeover vn=%s msg='%v'", utils.ShortVnodeID(txr.Dst), err)
			continue
		}

		log.Printf("TODO action=takeover dst=%s key=%s txcount=%d", utils.ShortVnodeID(txr.Dst), txr.TxBlock.Key(), len(txr.TxBlock.TxIds()))
		for _, txhash := range txr.TxBlock.TxIds() {
			tx, _, err := tm.cs.GetTx(txhash, utils.RequestOptions{PeerSetKey: txr.TxBlock.Key()})
			if err != nil {
				log.Printf("ERR key=%s tx=%x msg='%v'", txr.TxBlock.Key(), txhash, err)
				continue
			}

			log.Printf("GOT %s %x", txr.TxBlock.Key(), tx.Hash())

		}*/

		tm.transport.local.QueueBlockReplay(txr.Dst, txr.TxBlock)
		//tm.transport.local.getStore(txr.Dst)

		//opts.PeerSetKey = txr.TxBlock.Key()
		//tx, _,err := tm.cs.GetTx(txr.TxBlock.LastTx(), opts)
		//if err != nil {
		//	log.Printf("ERR action=takeover dst=%s key=%s txcount=%d", utils.ShortVnodeID(txr.Dst), txr.TxBlock.Key(), len(txr.TxBlock.TxIds()))
		//	continue
		//}

		// Create TxBlock txr.txb
		// Set to takeover mode
		// Submit txr.TxBlock.Last() to replay

		//txids := txr.TxBlock.TxIds()
		//for _, txi := range txids {
		//log.Printf("TODO action=takeover dst=%s key=%s tx=%x", utils.ShortVnodeID(txr.Dst), txr.TxBlock.Key(), txi[:8])
		//if err = tm.transport.AppendTx(txr.Dst, tx); err != nil {
		//	log.Printf("ERR action=takeover dst=%s key=%s tx=%x msg='%v'", utils.ShortVnodeID(txr.Dst), txr.TxBlock.Key(), txi[:8], err)
		//}
		//}

	}
}
