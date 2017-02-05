package difuse

import (
	"fmt"
	"net"
	"strings"

	chord "github.com/ipkg/go-chord"
)

func isZeroHash(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// maxVotes returns the txhash and count of the key with the longest peer list
func maxVotes(hm map[string][]string) (lh string, c int) {
	for k, v := range hm {
		l := len(v)
		if l > c {
			c = l
			lh = k
		}
	}
	return
}

func vnodesByHost(vl []*chord.Vnode) map[string][]*chord.Vnode {
	m := map[string][]*chord.Vnode{}
	for _, vn := range vl {
		v, ok := m[vn.Host]
		if !ok {
			m[vn.Host] = []*chord.Vnode{vn}
			continue
		}
		v = append(v, vn)
		m[vn.Host] = v
	}
	return m
}

// hasCandidate returns whether the given list contains candidate
func hasCandidate(candidates []string, candidate string) bool {
	for _, c := range candidates {
		if c == candidate {
			return true
		}
	}
	return false
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

func shortID(vn *chord.Vnode) string {
	if vn != nil {
		return vn.Host + "/" + vn.String()[:12]
	}
	return "<nil>"
}
