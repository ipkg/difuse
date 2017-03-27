package txlog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/ipkg/difuse/gentypes"
)

type BoltTxStore struct {
	db     *bolt.DB
	dbfile string
}

func (b *BoltTxStore) Open(mode os.FileMode) (err error) {
	//0600
	if b.db, err = bolt.Open(b.dbfile, mode, nil); err == nil {
		err = b.db.Update(func(btx *bolt.Tx) error {
			_, er := btx.CreateBucketIfNotExists([]byte("tx"))
			return er
		})
	}
	return
}

func (b *BoltTxStore) Close() error {
	return b.db.Close()
}

func NewBoltTxStore(datadir, name string) *BoltTxStore {
	return &BoltTxStore{
		dbfile: filepath.Join(datadir, name+".tx"),
	}
}

func (b *BoltTxStore) Get(txhash []byte) (tx *Tx, err error) {
	err = b.db.View(func(btx *bolt.Tx) error {
		bucket := btx.Bucket([]byte("tx"))
		txbin := bucket.Get(txhash)
		if txbin == nil {
			return fmt.Errorf("tx not found: %x", txhash)
		}

		obj := gentypes.GetRootAsTx(txbin, 0)

		tx = &Tx{}
		tx.Deserialize(obj)
		return nil
	})

	return
}

func (b *BoltTxStore) Set(tx *Tx) error {
	return b.db.Update(func(btx *bolt.Tx) error {
		bkt := btx.Bucket([]byte("tx"))

		fb := flatbuffers.NewBuilder(0)
		fb.Finish(tx.Serialize(fb))

		value := fb.Bytes[fb.Head():]

		return bkt.Put(tx.Hash(), value)
	})
}

/*type BoltTxBlockStore struct {
	db     *bolt.DB
	dbfile string
}

func NewBoltTxBlockStore(datadir, name string) *BoltTxBlockStore {
	return &BoltTxBlockStore{
		dbfile: filepath.Join(datadir, name+".txb"),
	}
}

func (b *BoltTxBlockStore) Open(mode os.FileMode) (err error) {
	if b.db, err = bolt.Open(b.dbfile, mode, nil); err == nil {
		err = b.db.Update(func(btx *bolt.Tx) error {
			_, er := btx.CreateBucketIfNotExists([]byte("txb"))
			return er
		})
	}
	return
}

// Create a key if it doesn't exist and set the mode
func (b *BoltTxBlockStore) Create(key []byte, mode TxBlockMode) error {
	return b.db.Update(func(btx *bolt.Tx) error {
		bkt := btx.Bucket([]byte("txb"))
		txbin := bkt.Get(key)
		if txbin == nil {
			txb := NewTxBlock(key)
			txb.SetMode(mode)

			fb := flatbuffers.NewBuilder(0)
			fb.Finish(txb.Serialize(fb))
			txbin = fb.Bytes[fb.Head():]
		} else {
			obj := gentypes.GetRootAsTxBlock(txbin, 0)
			obj.MutateMode(int32(mode))
		}

		return bkt.Put(key, txbin)
	})
}

// Set sets the given tx block to store
func (b *BoltTxBlockStore) Set(tb *TxBlock) error {
	return b.db.Update(func(btx *bolt.Tx) error {
		bkt := btx.Bucket([]byte("txb"))

		fb := flatbuffers.NewBuilder(0)
		fb.Finish(tb.Serialize(fb))

		value := fb.Bytes[fb.Head():]

		return bkt.Put(tb.Key(), value)
	})
}

// Get key for the tx.  This is the object encompassing the tx's for a key.
func (b *BoltTxBlockStore) Get(key []byte) (txb *TxBlock, err error) {
	err = b.db.View(func(btx *bolt.Tx) error {
		bucket := btx.Bucket([]byte("txb"))
		txbin := bucket.Get(key)
		if txbin == nil {
			return fmt.Errorf("txblock not found: %x", key)
		}

		obj := gentypes.GetRootAsTxBlock(txbin, 0)

		txb = &TxBlock{}
		txb.Deserialize(obj)
		return nil
	})

	return
}

// Mode return the mode
func (b *BoltTxBlockStore) Mode(key []byte) (txbm TxBlockMode, err error) {
	err = b.db.View(func(btx *bolt.Tx) error {
		bucket := btx.Bucket([]byte("txb"))
		txbin := bucket.Get(key)
		if txbin == nil {
			return fmt.Errorf("txblock not found: %x", key)
		}

		obj := gentypes.GetRootAsTxBlock(txbin, 0)
		txbm = TxBlockMode(obj.Mode())

		return nil
	})

	return
}

// SetMode sets the key mode
func (b *BoltTxBlockStore) SetMode(key []byte, mode TxBlockMode) error {
	return b.db.Update(func(btx *bolt.Tx) error {
		bkt := btx.Bucket([]byte("txb"))

		txbin := bkt.Get(key)
		if txbin == nil {
			return fmt.Errorf("txblock not found: %x", key)
		}

		obj := gentypes.GetRootAsTxBlock(txbin, 0)
		obj.MutateMode(int32(mode))

		return bkt.Put(key, txbin)
	})
}

// Snapshot returns a consistent snapshot
func (b *BoltTxBlockStore) Snapshot() (TxBlockStore, error) {}

// Iter iterates over txblocks
func (b *BoltTxBlockStore) Iter(func(*TxBlock) error) error {}

// Close closes the db handle
func (b *BoltTxBlockStore) Close() error {
	return b.db.Close()
}*/
