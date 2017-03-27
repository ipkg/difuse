package utils

import (
	"fmt"
	"net"
	"strings"

	chord "github.com/ipkg/go-chord"
)

// ZeroHash returns a 32 byte hash with all zeros
func ZeroHash() []byte {
	return make([]byte, 32)
}

// EqualBytes returns whether 2 byte slices have the same data
func EqualBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func IsZeroHash(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// ShortVnodeID returns the shortened vnode id
func ShortVnodeID(vn *chord.Vnode) string {
	if vn == nil {
		return "<nil>/<nil>"
	}
	return fmt.Sprintf("%s/%x", vn.Host, vn.Id[:8])
}

func LongVnodeID(vn *chord.Vnode) string {
	if vn == nil {
		return "<nil>/<nil>"
	}
	return fmt.Sprintf("%s/%x", vn.Host, vn.Id)
}

// AutoDetectIPAddress traverses interfaces eliminating, localhost, ifaces with
// no addresses and ipv6 addresses.  It returns a list by priority
func AutoDetectIPAddress() ([]string, error) {

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	out := []string{}

	for _, ifc := range ifaces {
		if strings.HasPrefix(ifc.Name, "lo") {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil || len(addrs) == 0 {
			continue
		}

		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}

			// Add ipv4 addresses to list
			if ip.To4() != nil {
				out = append(out, ip.String())
			}
		}

	}
	if len(out) == 0 {
		return nil, fmt.Errorf("could not detect ip addresses")
	}

	return out, nil
}

// IsAdvertisableAddress checks if an address can be used as the advertise address.
func IsAdvertisableAddress(hp string) (bool, error) {
	if hp == "" {
		return false, fmt.Errorf("invalid address")
	}

	pp := strings.Split(hp, ":")
	if len(pp) < 1 {
		return false, fmt.Errorf("could not parse: %s", hp)
	}

	if pp[0] == "" {
		return false, nil
	} else if pp[0] == "0.0.0.0" {
		return false, nil
	}

	if _, err := net.ResolveIPAddr("ip4", pp[0]); err != nil {
		return false, err
	}

	return true, nil
}
