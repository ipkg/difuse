package difuse

import "testing"

func TestAutoDetectIPAddress(t *testing.T) {
	ifaces, err := AutoDetectIPAddress()
	if err != nil {
		t.Fatal(err)
	}

	if len(ifaces) == 0 {
		t.Fatal("no addrs found")
	}
}
