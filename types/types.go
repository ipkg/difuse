package types

import (
	"encoding/hex"
	"encoding/json"
)

func (m *ResponseMeta) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"Vnode":   map[string]interface{}{"Id": hex.EncodeToString(m.Vnode.Id), "Host": m.Vnode.Host},
		"KeyHash": hex.EncodeToString(m.KeyHash),
	})
}
