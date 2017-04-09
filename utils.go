package difuse

import "github.com/ipkg/difuse/types"

// ParseConsistency parses the consistency string into its type.
func ParseConsistency(consistency string) types.Consistency {
	switch consistency {
	case "all":
		return types.Consistency_ALL
	case "quorum":
		return types.Consistency_QUORUM
	case "lazy":
		return types.Consistency_LAZY
	}
	return types.Consistency(-1)
}
