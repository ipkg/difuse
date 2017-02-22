package difuse

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/gentypes"
	"github.com/ipkg/difuse/store"
	"github.com/ipkg/difuse/txlog"
)

func serializeTransferRequest(fb *flatbuffers.Builder, src, dst *chord.Vnode) flatbuffers.UOffsetT {
	sp := chord.SerializeVnode(fb, src)
	dp := chord.SerializeVnode(fb, dst)

	gentypes.TransferRequestStart(fb)
	gentypes.TransferRequestAddSrc(fb, sp)
	gentypes.TransferRequestAddDst(fb, dp)
	return gentypes.TransferRequestEnd(fb)
}

func serializeTxRequest(fb *flatbuffers.Builder, key, seek []byte, vn *chord.Vnode) flatbuffers.UOffsetT {
	kp := fb.CreateByteString(key)
	ip := fb.CreateByteString(vn.Id)

	var sp flatbuffers.UOffsetT
	if seek != nil {
		sp = fb.CreateByteString(seek)
	}

	gentypes.TxRequestStart(fb)
	gentypes.TxRequestAddId(fb, ip)
	gentypes.TxRequestAddKey(fb, kp)
	if seek != nil {
		gentypes.TxRequestAddSeek(fb, sp)
	}
	return gentypes.TxRequestEnd(fb)
}

func serializeIdRoot(fb *flatbuffers.Builder, id, root []byte) flatbuffers.UOffsetT {
	ip := fb.CreateByteString(id)
	rp := fb.CreateByteString(root)

	gentypes.IdRootStart(fb)
	gentypes.IdRootAddId(fb, ip)
	gentypes.IdRootAddRoot(fb, rp)
	return gentypes.IdRootEnd(fb)
}

func serializeVnodeIdsBytes(key []byte, vns ...*chord.Vnode) []byte {
	if len(vns) == 0 {
		return nil
	}
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vns))

	for i, vn := range vns {
		ip := fb.CreateByteString(vn.Id)
		gentypes.ByteSliceStart(fb)
		gentypes.ByteSliceAddB(fb, ip)
		ofs[i] = gentypes.ByteSliceEnd(fb)
	}

	gentypes.VnodeIdsBytesStartIdsVector(fb, len(vns))
	for _, o := range ofs {
		fb.PrependUOffsetT(o)
	}

	idsVec := fb.EndVector(len(vns))

	kp := fb.CreateByteString(key)

	gentypes.VnodeIdsBytesStart(fb)
	gentypes.VnodeIdsBytesAddB(fb, kp)
	gentypes.VnodeIdsBytesAddIds(fb, idsVec)

	p := gentypes.VnodeIdsBytesEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func serializeByteSlice(fb *flatbuffers.Builder, b []byte) flatbuffers.UOffsetT {
	bp := fb.CreateByteString(b)
	gentypes.ByteSliceStart(fb)
	gentypes.ByteSliceAddB(fb, bp)
	return gentypes.ByteSliceEnd(fb)
}

func serializeVnodeIdInodeErrList(rsps []*VnodeResponse) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(rsps))

	for i, vn := range rsps {
		ip := fb.CreateByteString(vn.Id)
		//var dp flatbuffers.UOffsetT
		if vn.Err != nil {
			dp := fb.CreateByteString([]byte(vn.Err.Error()))
			gentypes.VnodeIdInodeErrStart(fb)
			gentypes.VnodeIdInodeErrAddId(fb, ip)
			gentypes.VnodeIdInodeErrAddE(fb, dp)

		} else {
			d := vn.Data.(*store.Inode)
			dp := d.Serialize(fb)

			gentypes.VnodeIdInodeErrStart(fb)
			gentypes.VnodeIdInodeErrAddId(fb, ip)
			gentypes.VnodeIdInodeErrAddInode(fb, dp)
		}
		ofs[i] = gentypes.VnodeIdInodeErrEnd(fb)
	}

	gentypes.VnodeIdInodeErrListStartLVector(fb, len(rsps))
	for _, v := range ofs {
		fb.PrependUOffsetT(v)
	}
	l := fb.EndVector(len(rsps))

	gentypes.VnodeIdInodeErrListStart(fb)
	gentypes.VnodeIdInodeErrListAddL(fb, l)
	p := gentypes.VnodeIdInodeErrListEnd(fb)
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
			gentypes.VnodeIdBytesErrStart(fb)
			gentypes.VnodeIdBytesErrAddId(fb, ip)
			gentypes.VnodeIdBytesErrAddE(fb, dp)

		} else {
			d := vn.Data.([]byte)
			dp := fb.CreateByteString(d)
			gentypes.VnodeIdBytesErrStart(fb)
			gentypes.VnodeIdBytesErrAddId(fb, ip)
			gentypes.VnodeIdBytesErrAddB(fb, dp)
		}

		ofs[i] = gentypes.VnodeIdBytesErrEnd(fb)
	}

	gentypes.VnodeIdBytesErrListStartLVector(fb, len(rsps))
	for _, v := range ofs {
		fb.PrependUOffsetT(v)
	}
	l := fb.EndVector(len(rsps))

	gentypes.VnodeIdBytesErrListStart(fb)
	gentypes.VnodeIdBytesErrListAddL(fb, l)
	p := gentypes.VnodeIdBytesErrListEnd(fb)
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
			gentypes.VnodeIdTxErrStart(fb)
			gentypes.VnodeIdTxErrAddId(fb, ip)
			gentypes.VnodeIdTxErrAddE(fb, dp)

		} else {
			tx := vn.Data.(*txlog.Tx)
			tep := serializeTx(fb, tx)

			gentypes.VnodeIdTxErrStart(fb)
			gentypes.VnodeIdTxErrAddId(fb, ip)
			gentypes.VnodeIdTxErrAddTx(fb, tep)
		}
		ofs[i] = gentypes.VnodeIdTxErrEnd(fb)
	}

	gentypes.VnodeIdTxErrListStartLVector(fb, len(vnds))
	for _, v := range ofs {
		fb.PrependUOffsetT(v)
	}
	l := fb.EndVector(len(vnds))

	gentypes.VnodeIdTxErrListStart(fb)
	gentypes.VnodeIdTxErrListAddL(fb, l)
	p := gentypes.VnodeIdTxErrListEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func serializeVnodeIdsTwoByteSlices(b1, b2 []byte, vns ...*chord.Vnode) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vns))

	for i, vn := range vns {
		ip := fb.CreateByteString(vn.Id)
		gentypes.ByteSliceStart(fb)
		gentypes.ByteSliceAddB(fb, ip)
		ofs[i] = gentypes.ByteSliceEnd(fb)
	}

	gentypes.VnodeIdsTwoByteSlicesStartIdsVector(fb, len(vns))
	for _, o := range ofs {
		fb.PrependUOffsetT(o)
	}

	idsVec := fb.EndVector(len(vns))
	kp := fb.CreateByteString(b1)
	tp := fb.CreateByteString(b2)

	gentypes.VnodeIdsTwoByteSlicesStart(fb)
	gentypes.VnodeIdsTwoByteSlicesAddB1(fb, kp)
	gentypes.VnodeIdsTwoByteSlicesAddB2(fb, tp)
	gentypes.VnodeIdsTwoByteSlicesAddIds(fb, idsVec)

	p := gentypes.VnodeIdsTwoByteSlicesEnd(fb)
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

	gentypes.TxStart(fb)
	gentypes.TxAddKey(fb, kp)
	gentypes.TxAddPrevHash(fb, pp)
	gentypes.TxAddSource(fb, sp)
	gentypes.TxAddDestination(fb, dp)
	gentypes.TxAddData(fb, ddp)
	gentypes.TxAddSignature(fb, ssp)
	gentypes.TxAddTimestamp(fb, tx.Timestamp)
	return gentypes.TxEnd(fb)
}

func serializeVnodeIdsTx(tx *txlog.Tx, vns []*chord.Vnode) []byte {
	fb := flatbuffers.NewBuilder(0)

	ofs := make([]flatbuffers.UOffsetT, len(vns))

	for i, vn := range vns {
		ip := fb.CreateByteString(vn.Id)
		gentypes.ByteSliceStart(fb)
		gentypes.ByteSliceAddB(fb, ip)
		ofs[i] = gentypes.ByteSliceEnd(fb)
	}

	gentypes.VnodeIdsTxStartIdsVector(fb, len(vns))
	for _, o := range ofs {
		fb.PrependUOffsetT(o)
	}

	idsVec := fb.EndVector(len(vns))
	tep := serializeTx(fb, tx)

	gentypes.VnodeIdsTxStart(fb)
	gentypes.VnodeIdsTxAddIds(fb, idsVec)
	gentypes.VnodeIdsTxAddTx(fb, tep)
	p := gentypes.VnodeIdsTxEnd(fb)
	fb.Finish(p)

	return fb.Bytes[fb.Head():]
}

func deserializeVnodeIdsTwoByteSlices(data []byte) ([]*chord.Vnode, []byte, []byte) {
	tbs := gentypes.GetRootAsVnodeIdsTwoByteSlices(data, 0)
	l := tbs.IdsLength()
	ids := make([]*chord.Vnode, l)
	for i := 0; i < l; i++ {
		var vid gentypes.ByteSlice
		tbs.Ids(&vid, i)
		ids[l-i-1] = &chord.Vnode{Id: vid.BBytes()}
	}
	return ids, tbs.B1Bytes(), tbs.B2Bytes()
}

func deserializeVnodeIdTxErrList(data []byte) []*VnodeResponse {
	vibel := gentypes.GetRootAsVnodeIdTxErrList(data, 0)

	l := vibel.LLength()
	out := make([]*VnodeResponse, l)
	for i := 0; i < l; i++ {
		var obj gentypes.VnodeIdTxErr
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

func deserializeTx(tx *gentypes.Tx) *txlog.Tx {
	return &txlog.Tx{
		Key:       tx.KeyBytes(),
		Data:      tx.DataBytes(),
		Signature: tx.SignatureBytes(),
		TxHeader: &txlog.TxHeader{
			Timestamp:   tx.Timestamp(),
			PrevHash:    tx.PrevHashBytes(),
			Source:      tx.SourceBytes(),
			Destination: tx.DestinationBytes(),
		},
	}
}

func deserializeVnodeIdsTx(data []byte) ([]*chord.Vnode, *txlog.Tx) {
	idsTx := gentypes.GetRootAsVnodeIdsTx(data, 0)
	l := idsTx.IdsLength()
	ids := make([]*chord.Vnode, l)

	for i := 0; i < l; i++ {
		var vid gentypes.ByteSlice
		idsTx.Ids(&vid, i)
		ids[l-i-1] = &chord.Vnode{Id: vid.BBytes()}
	}
	fbtx := idsTx.Tx(nil)
	tx := deserializeTx(fbtx)

	return ids, tx
}

func deserializeVnodeIdsBytes(data []byte) ([]*chord.Vnode, []byte) {
	vnb := gentypes.GetRootAsVnodeIdsBytes(data, 0)
	l := vnb.IdsLength()

	ids := make([]*chord.Vnode, l)
	for i := 0; i < l; i++ {
		var vid gentypes.ByteSlice
		vnb.Ids(&vid, i)
		// deserialize in reverse
		ids[l-i-1] = &chord.Vnode{Id: vid.BBytes()}
	}
	return ids, vnb.BBytes()
}

func deserializeVnodeIdBytesErrList(data []byte) []*VnodeResponse {
	be := gentypes.GetRootAsVnodeIdBytesErrList(data, 0)
	l := be.LLength()
	out := make([]*VnodeResponse, l)
	for i := 0; i < l; i++ {
		var obj gentypes.VnodeIdBytesErr
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

func deserializeVnodeIdInodeErrList(data []byte) []*VnodeResponse {
	viel := gentypes.GetRootAsVnodeIdInodeErrList(data, 0)
	l := viel.LLength()

	vrl := make([]*VnodeResponse, l)
	for i := 0; i < l; i++ {
		var obj gentypes.VnodeIdInodeErr
		viel.L(&obj, i)

		vr := &VnodeResponse{Id: obj.IdBytes()}
		if e := obj.E(); e != nil && len(e) > 0 {
			vr.Err = fmt.Errorf("%s", e)
		} else {
			ind := obj.Inode(nil)

			inds := &store.Inode{}
			inds.Deserialize(ind)
			vr.Data = inds
		}
		// deserialize in reverse
		vrl[l-i-1] = vr
	}
	return vrl
}
