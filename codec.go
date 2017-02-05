package difuse

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/fbtypes"
	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

func serializeVnodeIdsBytes(key []byte, vns []*chord.Vnode) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vns))

	for i, vn := range vns {
		ip := fb.CreateByteString(vn.Id)
		fbtypes.ByteSliceStart(fb)
		fbtypes.ByteSliceAddB(fb, ip)
		ofs[i] = fbtypes.ByteSliceEnd(fb)
	}

	fbtypes.VnodeIdsBytesStartIdsVector(fb, len(vns))
	for _, o := range ofs {
		fb.PrependUOffsetT(o)
	}

	idsVec := fb.EndVector(len(vns))

	kp := fb.CreateByteString(key)

	fbtypes.VnodeIdsBytesStart(fb)
	fbtypes.VnodeIdsBytesAddB(fb, kp)
	fbtypes.VnodeIdsBytesAddIds(fb, idsVec)

	p := fbtypes.VnodeIdsBytesEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func serializeByteSlice(fb *flatbuffers.Builder, b []byte) flatbuffers.UOffsetT {
	bp := fb.CreateByteString(b)
	fbtypes.ByteSliceStart(fb)
	fbtypes.ByteSliceAddB(fb, bp)
	return fbtypes.ByteSliceEnd(fb)
}

func serializeVnodeIdInodeErrList(rsps []*VnodeResponse) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(rsps))

	for i, vn := range rsps {
		ip := fb.CreateByteString(vn.Id)
		//var dp flatbuffers.UOffsetT
		if vn.Err != nil {
			dp := fb.CreateByteString([]byte(vn.Err.Error()))
			fbtypes.VnodeIdInodeErrStart(fb)
			fbtypes.VnodeIdInodeErrAddId(fb, ip)
			fbtypes.VnodeIdInodeErrAddE(fb, dp)

		} else {
			d := vn.Data.(*store.Inode)
			dp := d.Serialize(fb)

			fbtypes.VnodeIdInodeErrStart(fb)
			fbtypes.VnodeIdInodeErrAddId(fb, ip)
			fbtypes.VnodeIdInodeErrAddInode(fb, dp)
		}
		ofs[i] = fbtypes.VnodeIdInodeErrEnd(fb)
	}

	fbtypes.VnodeIdInodeErrListStartLVector(fb, len(rsps))
	for _, v := range ofs {
		fb.PrependUOffsetT(v)
	}
	l := fb.EndVector(len(rsps))

	fbtypes.VnodeIdInodeErrListStart(fb)
	fbtypes.VnodeIdInodeErrListAddL(fb, l)
	p := fbtypes.VnodeIdInodeErrListEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]

}

func serializeVnodeIdBytesErrList(rsps []*VnodeResponse) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(rsps))

	for i, vn := range rsps {
		ip := fb.CreateByteString(vn.Id)

		if vn.Err != nil {
			dp := fb.CreateByteString([]byte(vn.Err.Error()))
			fbtypes.VnodeIdBytesErrStart(fb)
			fbtypes.VnodeIdBytesErrAddId(fb, ip)
			fbtypes.VnodeIdBytesErrAddE(fb, dp)

		} else {
			d := vn.Data.([]byte)
			dp := fb.CreateByteString(d)
			fbtypes.VnodeIdBytesErrStart(fb)
			fbtypes.VnodeIdBytesErrAddId(fb, ip)
			fbtypes.VnodeIdBytesErrAddB(fb, dp)
		}

		ofs[i] = fbtypes.VnodeIdBytesErrEnd(fb)
	}

	fbtypes.VnodeIdBytesErrListStartLVector(fb, len(rsps))
	for _, v := range ofs {
		fb.PrependUOffsetT(v)
	}
	l := fb.EndVector(len(rsps))

	fbtypes.VnodeIdBytesErrListStart(fb)
	fbtypes.VnodeIdBytesErrListAddL(fb, l)
	p := fbtypes.VnodeIdBytesErrListEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]

}

func serializeVnodeIdTxErrList(vnds []*VnodeResponse) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vnds))

	for i, vn := range vnds {
		ip := fb.CreateByteString(vn.Id)

		if vn.Err != nil {
			dp := fb.CreateByteString([]byte(vn.Err.Error()))
			fbtypes.VnodeIdTxErrStart(fb)
			fbtypes.VnodeIdTxErrAddId(fb, ip)
			fbtypes.VnodeIdTxErrAddE(fb, dp)

		} else {
			tx := vn.Data.(*txlog.Tx)
			tep := serializeTx(fb, tx)

			fbtypes.VnodeIdTxErrStart(fb)
			fbtypes.VnodeIdTxErrAddId(fb, ip)
			fbtypes.VnodeIdTxErrAddTx(fb, tep)
		}
		ofs[i] = fbtypes.VnodeIdTxErrEnd(fb)
	}

	fbtypes.VnodeIdTxErrListStartLVector(fb, len(vnds))
	for _, v := range ofs {
		fb.PrependUOffsetT(v)
	}
	l := fb.EndVector(len(vnds))

	fbtypes.VnodeIdTxErrListStart(fb)
	fbtypes.VnodeIdTxErrListAddL(fb, l)
	p := fbtypes.VnodeIdTxErrListEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func serializeVnodeIdsTwoByteSlices(b1, b2 []byte, vns []*chord.Vnode) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vns))

	for i, vn := range vns {
		ip := fb.CreateByteString(vn.Id)
		fbtypes.ByteSliceStart(fb)
		fbtypes.ByteSliceAddB(fb, ip)
		ofs[i] = fbtypes.ByteSliceEnd(fb)
	}

	fbtypes.VnodeIdsTwoByteSlicesStartIdsVector(fb, len(vns))
	for _, o := range ofs {
		fb.PrependUOffsetT(o)
	}

	idsVec := fb.EndVector(len(vns))
	kp := fb.CreateByteString(b1)
	tp := fb.CreateByteString(b2)

	fbtypes.VnodeIdsTwoByteSlicesStart(fb)
	fbtypes.VnodeIdsTwoByteSlicesAddB1(fb, kp)
	fbtypes.VnodeIdsTwoByteSlicesAddB2(fb, tp)
	fbtypes.VnodeIdsTwoByteSlicesAddIds(fb, idsVec)

	p := fbtypes.VnodeIdsTwoByteSlicesEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func serializeTx(fb *flatbuffers.Builder, tx *txlog.Tx) flatbuffers.UOffsetT {
	kp := fb.CreateByteString(tx.Key)
	pp := fb.CreateByteString(tx.PrevHash)
	sp := fb.CreateByteString(tx.Source)
	dp := fb.CreateByteString(tx.Destination)
	ddp := fb.CreateByteString(tx.Data)
	ssp := fb.CreateByteString(tx.Signature)

	fbtypes.TxStart(fb)
	fbtypes.TxAddKey(fb, kp)
	fbtypes.TxAddPrevHash(fb, pp)
	fbtypes.TxAddSource(fb, sp)
	fbtypes.TxAddDestination(fb, dp)
	fbtypes.TxAddData(fb, ddp)
	fbtypes.TxAddSignature(fb, ssp)
	return fbtypes.TxEnd(fb)
}

func serializeVnodeIdsTx(tx *txlog.Tx, vns []*chord.Vnode) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vns))

	for i, vn := range vns {
		ip := fb.CreateByteString(vn.Id)
		fbtypes.ByteSliceStart(fb)
		fbtypes.ByteSliceAddB(fb, ip)
		ofs[i] = fbtypes.ByteSliceEnd(fb)
	}

	fbtypes.VnodeIdsTxStartIdsVector(fb, len(vns))
	for _, o := range ofs {
		fb.PrependUOffsetT(o)
	}

	idsVec := fb.EndVector(len(vns))
	tep := serializeTx(fb, tx)

	fbtypes.VnodeIdsTxStart(fb)
	fbtypes.VnodeIdsTxAddIds(fb, idsVec)
	fbtypes.VnodeIdsTxAddTx(fb, tep)
	p := fbtypes.VnodeIdsTxEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func deserializeVnodeIdsTwoByteSlices(data []byte) ([]*chord.Vnode, []byte, []byte) {
	tbs := fbtypes.GetRootAsVnodeIdsTwoByteSlices(data, 0)
	l := tbs.IdsLength()
	ids := make([]*chord.Vnode, l)
	for i := 0; i < l; i++ {
		var vid fbtypes.ByteSlice
		tbs.Ids(&vid, i)
		ids[l-i-1] = &chord.Vnode{Id: vid.BBytes()}
	}
	return ids, tbs.B1Bytes(), tbs.B2Bytes()
}

func deserializeVnodeIdTxErrList(data []byte) []*VnodeResponse {
	vibel := fbtypes.GetRootAsVnodeIdTxErrList(data, 0)

	l := vibel.LLength()
	out := make([]*VnodeResponse, l)
	for i := 0; i < l; i++ {
		var obj fbtypes.VnodeIdTxErr
		vibel.L(&obj, i)
		vid := &VnodeResponse{Id: obj.IdBytes()}

		e := obj.E()
		if e != nil && len(e) > 0 {
			vid.Err = fmt.Errorf("%s", e)
		} else {
			//vid.Data
			tx := obj.Tx(nil)
			vid.Data = deserializeTx(tx)
		}

		out[l-i-1] = vid
	}
	return out
}

func deserializeTx(tx *fbtypes.Tx) *txlog.Tx {
	return &txlog.Tx{
		Key:       tx.KeyBytes(),
		Data:      tx.DataBytes(),
		Signature: tx.SignatureBytes(),
		TxHeader: &txlog.TxHeader{
			PrevHash:    tx.PrevHashBytes(),
			Source:      tx.SourceBytes(),
			Destination: tx.DestinationBytes(),
		},
	}
}

func deserializeVnodeIdsTx(data []byte) ([]*chord.Vnode, *txlog.Tx) {
	idsTx := fbtypes.GetRootAsVnodeIdsTx(data, 0)
	l := idsTx.IdsLength()
	ids := make([]*chord.Vnode, l)

	for i := 0; i < l; i++ {
		var vid fbtypes.ByteSlice
		idsTx.Ids(&vid, i)
		ids[l-i-1] = &chord.Vnode{Id: vid.BBytes()}
	}
	fbtx := idsTx.Tx(nil)
	tx := deserializeTx(fbtx)

	return ids, tx
}

func deserializeVnodeIdsBytes(data []byte) ([]*chord.Vnode, []byte) {
	vnb := fbtypes.GetRootAsVnodeIdsBytes(data, 0)
	l := vnb.IdsLength()

	ids := make([]*chord.Vnode, l)
	for i := 0; i < l; i++ {
		var vid fbtypes.ByteSlice
		vnb.Ids(&vid, i)
		// deserialize in reverse
		ids[l-i-1] = &chord.Vnode{Id: vid.BBytes()}
	}
	return ids, vnb.BBytes()
}

func deserializeVnodeIdBytesErrList(data []byte) []*VnodeResponse {
	be := fbtypes.GetRootAsVnodeIdBytesErrList(data, 0)
	l := be.LLength()
	out := make([]*VnodeResponse, l)
	for i := 0; i < l; i++ {
		var obj fbtypes.VnodeIdBytesErr
		be.L(&obj, i)

		vid := &VnodeResponse{Id: obj.IdBytes()}

		e := obj.E()
		if e != nil && len(e) > 0 {
			vid.Err = fmt.Errorf("%s", e)
		} else {
			vid.Data = obj.BBytes()
		}
		// deserialize in reverse
		out[l-i-1] = vid
	}
	return out
}

func deserializeInode(ind *fbtypes.Inode) *store.Inode {
	inode := &store.Inode{
		Id:     ind.IdBytes(),
		Size:   ind.Size(),
		Inline: (ind.Inline() == byte(1)),
	}

	l := ind.BlocksLength()
	bh := make([][]byte, l)
	for i := 0; i < l; i++ {
		var obj fbtypes.ByteSlice
		ind.Blocks(&obj, i)
		// deserialize in reverse
		bh[l-i-1] = obj.BBytes()
	}
	inode.Blocks = bh

	return inode
}

func deserializeVnodeIdInodeErrList(data []byte) []*VnodeResponse {
	viel := fbtypes.GetRootAsVnodeIdInodeErrList(data, 0)
	l := viel.LLength()

	vrl := make([]*VnodeResponse, l)
	for i := 0; i < l; i++ {
		var obj fbtypes.VnodeIdInodeErr
		viel.L(&obj, i)

		vr := &VnodeResponse{Id: obj.IdBytes()}
		if e := obj.E(); e != nil && len(e) > 0 {
			vr.Err = fmt.Errorf("%s", e)
		} else {
			ind := obj.Inode(nil)
			vr.Data = deserializeInode(ind)
		}
		// deserialize in reverse
		vrl[l-i-1] = vr
	}
	return vrl
}
