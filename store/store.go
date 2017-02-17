package store

import "fmt"

const (
	// TxTypeSet represents an update transaction type
	TxTypeSet byte = iota + 3
	// TxTypeDelete represents a delete transaction type
	TxTypeDelete
)

var (
	errKeyNotFound   = fmt.Errorf("key not found")
	errBlockNotFound = fmt.Errorf("block not found")
	errAlreadyExists = fmt.Errorf("already exists")
	errInvalidTxType = fmt.Errorf("invalid tx type")
)
