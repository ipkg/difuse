package difuse

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

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

func (h *HTTPAdminServer) parseConsistency(r *http.Request) uint8 {
	c, ok := r.URL.Query()["consistency"]
	if ok && len(c) > 0 {
		switch c[0] {
		case "all":
			return utils.ConsistencyAll
		}
	}

	return utils.ConsistencyLazy
}

func (h *HTTPAdminServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		urlPath = strings.TrimPrefix(r.URL.Path, h.prefix)
		data    interface{}
		err     error
		meta    *utils.ResponseMeta
	)

	switch {
	case strings.HasPrefix(urlPath, "txblock/"):
		sid := strings.TrimPrefix(urlPath, "txblock/")
		key := []byte(sid)

		opts := utils.RequestOptions{Consistency: h.parseConsistency(r)}
		if opts.Consistency == utils.ConsistencyAll {
			data, meta, err = h.cs.GetTxBlockAll(key, opts)
		} else {
			data, meta, err = h.cs.GetTxBlock(key, opts)
		}

	case strings.HasPrefix(urlPath, "tx/"):
		sid := strings.TrimPrefix(urlPath, "tx/")
		var txhash []byte
		if txhash, err = hex.DecodeString(sid); err != nil {
			break
		}

		opts := utils.RequestOptions{Consistency: h.parseConsistency(r)}
		if opts.Consistency == utils.ConsistencyAll {
			data, meta, err = h.cs.GetTxAll(txhash, opts)
		} else {
			data, meta, err = h.cs.GetTx(txhash, opts)
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

	if meta.Vnode != nil {
		w.Header().Set("Vnode", utils.LongVnodeID(meta.Vnode))
	}
	w.Header().Set("Key-Hash", hex.EncodeToString(meta.KeyHash))

	w.Write(b)
}
