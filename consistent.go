package difuse

import (
	"log"
	"strings"

	"github.com/ipkg/difuse/txlog"
	"github.com/ipkg/difuse/utils"
	chord "github.com/ipkg/go-chord"
)

// consistentTransport provides consistent operations around the ring.
type consistentTransport struct {
	conf      *chord.Config
	ring      *chord.Ring
	transport Transport
}

func newConsistentTransport(conf *chord.Config, ring *chord.Ring, trans Transport) *consistentTransport {
	return &consistentTransport{
		transport: trans,
		ring:      ring,
		conf:      conf,
	}
}

func (c *consistentTransport) setDefaultOpts(opts *utils.RequestOptions) {
	if opts.PeerSetSize <= 0 {
		opts.PeerSetSize = c.conf.NumSuccessors
	}
}

func (c *consistentTransport) ProposeTx(tx *txlog.Tx, opts utils.RequestOptions) (*utils.ResponseMeta, error) {

	c.setDefaultOpts(&opts)

	khash, vs, err := c.ring.Lookup(opts.PeerSetSize, tx.Key)
	if err != nil {
		return nil, err
	}

	// Set the first vnode for meta as this will be broadcasted to designate the starting point.
	rmeta := &utils.ResponseMeta{KeyHash: khash, Vnode: vs[0]}

	if opts.Source != nil {
		// Broadcast to all vnodes skipping the source.
		for _, vn := range vs {
			if vn.String() == opts.Source.String() {
				continue
			}

			if er := c.transport.ProposeTx(vn, tx); er != nil && !strings.Contains(er.Error(), "tx exists") {
				log.Printf("ERR msg='%v'", er)
			}
		}

	} else {
		// Broadcast to all vnodes
		for _, vn := range vs {
			if er := c.transport.ProposeTx(vn, tx); er != nil && !strings.Contains(er.Error(), "tx exists") {
				log.Printf("ERR msg='%v'", er)
			}
		}

	}

	return rmeta, err
}

// GetTxBlock gets the first available non-erroring tx block
func (c *consistentTransport) GetTxBlock(key []byte, opts utils.RequestOptions) (*txlog.TxBlock, *utils.ResponseMeta, error) {
	c.setDefaultOpts(&opts)

	khash, vs, err := c.ring.Lookup(opts.PeerSetSize, key)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &utils.ResponseMeta{KeyHash: khash}

	for _, vn := range vs {
		blk, er := c.transport.GetTxBlock(vn, key)
		if er != nil {
			err = er
			continue
		}

		rmeta.Vnode = vn
		return blk, rmeta, nil
	}

	return nil, rmeta, err
}

// GetTxBlockAll gets all copies of a TxBlock
func (c *consistentTransport) GetTxBlockAll(key []byte, opts utils.RequestOptions) ([]*txlog.TxBlock, *utils.ResponseMeta, error) {
	c.setDefaultOpts(&opts)

	khash, vs, err := c.ring.Lookup(opts.PeerSetSize, key)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &utils.ResponseMeta{KeyHash: khash}

	resp := make([]*txlog.TxBlock, len(vs))
	for i, vn := range vs {
		blk, er := c.transport.GetTxBlock(vn, key)
		if er != nil {
			err = er
			continue
		}
		resp[i] = blk
	}
	return resp, rmeta, err
}

func (c *consistentTransport) NewTx(key []byte, opts utils.RequestOptions) (*txlog.Tx, *utils.ResponseMeta, error) {
	c.setDefaultOpts(&opts)

	khash, vs, err := c.ring.Lookup(opts.PeerSetSize, key)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &utils.ResponseMeta{KeyHash: khash}

	for _, vn := range vs {
		tx, er := c.transport.NewTx(vn, key)
		if er != nil {
			err = er
			continue
		}
		rmeta.Vnode = vn
		return tx, rmeta, nil
	}

	return nil, rmeta, err
}

func (c *consistentTransport) GetTx(txhash []byte, opts utils.RequestOptions) (*txlog.Tx, *utils.ResponseMeta, error) {
	c.setDefaultOpts(&opts)

	khash, vs, err := c.ring.Lookup(opts.PeerSetSize, opts.PeerSetKey)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &utils.ResponseMeta{KeyHash: khash}

	for _, vn := range vs {
		tx, er := c.transport.GetTx(vn, txhash)
		if er != nil {
			err = er
			continue
		}
		rmeta.Vnode = vn
		return tx, rmeta, nil
	}

	return nil, rmeta, err
}

// GetTxAll gets all copies of a tx.
func (c *consistentTransport) GetTxAll(txhash []byte, opts utils.RequestOptions) ([]*txlog.Tx, *utils.ResponseMeta, error) {
	c.setDefaultOpts(&opts)

	khash, vs, err := c.ring.Lookup(opts.PeerSetSize, opts.PeerSetKey)
	if err != nil {
		return nil, nil, err
	}

	rmeta := &utils.ResponseMeta{KeyHash: khash}

	out := make([]*txlog.Tx, len(vs))
	for i, vn := range vs {
		tx, er := c.transport.GetTx(vn, txhash)
		if er != nil {
			err = er
			continue
		}
		out[i] = tx
	}

	return out, rmeta, err
}
