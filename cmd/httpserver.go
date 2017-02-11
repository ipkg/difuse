package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse"
)

const (
	headerResponseTime = "Response-Time"
	headerVnode        = "Vnode"
)

type httpServer struct {
	tt *difuse.Difuse
}

func (hs *httpServer) handleData(w http.ResponseWriter, r *http.Request) (interface{}, error) {

	var (
		key   = []byte(r.URL.Path[1:])
		ct    = newCallTimer()
		data  interface{}
		meta  *difuse.ResponseMeta
		err   error
		rtime float64
		opts  = parseOptions(r)
	)

	switch r.Method {
	case "GET":
		if opts == nil {
			ct.start()
			data, meta, err = hs.tt.Get(key)
		} else {
			ct.start()
			data, meta, err = hs.tt.Get(key, *opts)
		}
		rtime = ct.stop()

	case "POST":
		var b []byte
		if b, err = ioutil.ReadAll(r.Body); err == nil {
			r.Body.Close()

			ct.start()
			meta, err = hs.tt.Set(key, b)
			rtime = ct.stop()
		}

	case "DELETE":
		ct.start()
		_, meta, err = hs.tt.Delete(key)
		rtime = ct.stop()

	default:
		return nil, fmt.Errorf("Method not allowed")

	}

	w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", rtime))
	w.Header().Set(headerVnode, difuse.ShortVnodeID(meta.Vnode))

	return data, err
}

func (hs *httpServer) handleLocate(w http.ResponseWriter, r *http.Request) (interface{}, error) {
	var (
		spath = strings.TrimPrefix(r.URL.Path[1:], "locate/")
		ct    = newCallTimer()
		data  interface{}
		err   error
		etime float64
	)

	switch {
	case strings.HasPrefix(spath, "tx/last/"):
		key := strings.TrimPrefix(spath, "tx/last/")
		ct.start()
		data, err = hs.tt.LocateLastTx([]byte(key))
		etime = ct.stop()

	case strings.HasPrefix(spath, "inode/"):
		key := strings.TrimPrefix(spath, "inode/")
		ct.start()
		data, err = hs.tt.LocateInode([]byte(key))
		etime = ct.stop()

	default:
		return nil, fmt.Errorf("not found")

	}

	w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", etime))
	return data, err
}

func (hs *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upath := r.URL.Path[1:]

	var (
		ct    = newCallTimer()
		data  interface{}
		err   error
		etime float64

		meta = &difuse.ResponseMeta{}
		opts = parseOptions(r)
	)

	switch {
	case strings.HasPrefix(upath, "stat/"):
		kstr := strings.TrimPrefix(upath, "stat/")

		if opts == nil {
			ct.start()
			data, meta, err = hs.tt.Stat([]byte(kstr))
		} else {
			ct.start()
			data, meta, err = hs.tt.Stat([]byte(kstr), *opts)
		}
		etime = ct.stop()

		w.Header().Set(headerVnode, difuse.ShortVnodeID(meta.Vnode))
		w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", etime))

	case strings.HasPrefix(upath, "leader/"):
		kstr := strings.TrimPrefix(upath, "leader/")

		var (
			l  *chord.Vnode
			vs []*chord.Vnode
			//vm map[string][]*chord.Vnode
		)

		ct.start()
		l, vs, _, err = hs.tt.LookupLeader([]byte(kstr))
		etime = ct.stop()

		w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", etime))

		if err == nil {
			data = map[string]interface{}{
				"leader": l,
				"vnodes": vs,
				//"hosts":  vm,
			}
		}

	case strings.HasPrefix(upath, "locate/"):
		data, err = hs.handleLocate(w, r)

	default:
		data, err = hs.handleData(w, r)
	}

	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	// no error and no response
	if data == nil {
		return
	}

	var b []byte
	switch data.(type) {
	case []byte:
		b = data.([]byte)

	default:
		var e error
		if b, e = json.Marshal(data); e != nil {
			w.WriteHeader(400)
			w.Write([]byte(e.Error()))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}

	w.Write(b)

}

type callTimer struct {
	t time.Time
}

func newCallTimer() *callTimer {
	return &callTimer{}
}

func (ct *callTimer) start() {
	ct.t = time.Now()
}

// returns elapsed in Milliseconds
func (ct *callTimer) stop() float64 {
	et := time.Now()
	return float64(et.UnixNano()-ct.t.UnixNano()) / 1000000
}
