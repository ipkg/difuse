package difuse

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/ipkg/difuse/types"
	"github.com/ipkg/difuse/utils"
)

type HTTPAdminServer struct {
	cs     *consistentTransport
	prefix string
}

func NewHTTPAdminServer(dif *Difuse, prefix string) *HTTPAdminServer {
	s := &HTTPAdminServer{cs: dif.cs, prefix: prefix}
	if !strings.HasSuffix(s.prefix, "/") {
		s.prefix += "/"
	}
	http.Handle(prefix, s)
	return s
}

func (h *HTTPAdminServer) parseConsistency(r *http.Request) types.Consistency {
	c, ok := r.URL.Query()["consistency"]
	if !ok {
		return types.Consistency_LAZY
	}
	return ParseConsistency(c[0])
}

func (h *HTTPAdminServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		urlPath = strings.TrimPrefix(r.URL.Path, h.prefix)
		data    interface{}
		err     error
		meta    *types.ResponseMeta
	)

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		return
	}

	switch {
	case strings.HasPrefix(urlPath, "status"):
		out := map[string]interface{}{}
		out["Vnodes"], err = h.cs.ring.ListVnodes(h.cs.conf.Hostname)
		data = out

	case strings.HasPrefix(urlPath, "txblock/"):
		sid := strings.TrimPrefix(urlPath, "txblock/")
		key := []byte(sid)

		if consistency := h.parseConsistency(r); consistency >= 0 {
			opts := types.RequestOptions{Consistency: h.parseConsistency(r)}
			if opts.Consistency == types.Consistency_ALL {
				data, err = h.cs.GetTxBlockAll(key, opts)
			} else {
				data, meta, err = h.cs.GetTxBlock(key, opts)
			}
		} else {
			err = fmt.Errorf("invalid consistency")
		}

	case strings.HasPrefix(urlPath, "tx/"):
		sid := strings.TrimPrefix(urlPath, "tx/")
		var txhash []byte
		if txhash, err = hex.DecodeString(sid); err != nil {
			break
		}

		if consistency := h.parseConsistency(r); consistency >= 0 {
			opts := types.RequestOptions{Consistency: h.parseConsistency(r)}
			if opts.Consistency == types.Consistency_ALL {
				data, err = h.cs.GetTxAll(txhash, opts)
			} else {
				data, meta, err = h.cs.GetTx(txhash, opts)
			}
		} else {
			err = fmt.Errorf("invalid consistency")
		}

	}

	if err != nil {
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"error":"` + err.Error() + `"}`))
		return
	}

	var b []byte
	switch data.(type) {
	case []byte:
		b = data.([]byte)

	default:
		w.Header().Set("Content-Type", "application/json")

		if b, err = json.Marshal(data); err != nil {
			w.WriteHeader(500)
			w.Write([]byte(`{"error":"` + err.Error() + `"}`))
			return
		}
	}

	if meta != nil {
		if meta.Vnode != nil {
			w.Header().Set("Vnode", utils.LongVnodeID(meta.Vnode))
		}
		w.Header().Set("Key-Hash", hex.EncodeToString(meta.KeyHash))
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(b)
}
