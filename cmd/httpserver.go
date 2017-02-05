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
)

type httpServer struct {
	tt *difuse.Difuse
}

func (hs *httpServer) handleData(w http.ResponseWriter, r *http.Request) (interface{}, error) {
	key := []byte(r.URL.Path[1:])
	var (
		data interface{}
		err  error
		ct   = newCallTimer()
	)

	switch r.Method {
	case "GET":
		ct.start()
		data, err = hs.tt.Get(key)
		rtms := ct.stop()
		w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", rtms))

	case "POST":
		var b []byte
		if b, err = ioutil.ReadAll(r.Body); err == nil {
			r.Body.Close()

			ct.start()

			_, err = hs.tt.Set(key, b)

			rtms := ct.stop()
			w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", rtms))
		}

	case "DELETE":
		ct.start()

		_, err = hs.tt.Delete(key)

		rtms := ct.stop()
		w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", rtms))

	default:
		err = fmt.Errorf("Method not allowed")
	}

	return data, err
}

func (hs *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upath := r.URL.Path[1:]

	var (
		data  interface{}
		err   error
		etime float64
	)

	switch {
	case strings.HasPrefix(upath, "stat/"):
		kstr := strings.TrimPrefix(upath, "stat/")

		ct := newCallTimer()
		ct.start()

		data, err = hs.tt.Stat([]byte(kstr))

		etime = ct.stop()
		w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", etime))

	case strings.HasPrefix(upath, "leader/"):
		kstr := strings.TrimPrefix(upath, "leader/")

		ct := newCallTimer()
		ct.start()

		var (
			l  *chord.Vnode
			vs []*chord.Vnode
			vm map[string][]*chord.Vnode
		)
		l, vs, vm, err = hs.tt.LookupLeader([]byte(kstr))

		etime = ct.stop()
		w.Header().Set(headerResponseTime, fmt.Sprintf("%fms", etime))

		if err == nil {
			data = map[string]interface{}{
				"leader": l,
				"order":  vs,
				"hosts":  vm,
			}
		}

	default:
		data, err = hs.handleData(w, r)
	}

	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

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
