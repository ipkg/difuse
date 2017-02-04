package store

import "fmt"

const (
	// TxTypeSet represents a set transaction type
	TxTypeSet byte = iota + 3
	// TxTypeDelete represents a delete transaction type
	TxTypeDelete
)

var (
	// ErrNotFound not found err
	ErrNotFound      = fmt.Errorf("not found")
	errAlreadyExists = fmt.Errorf("already exists")
	errInvalidTxType = fmt.Errorf("invalid tx type")
)
