package difuse

import (
	"strings"
	"testing"
)

func TestConfigSetPeers(t *testing.T) {
	cfg := DefaultConfig()

	cfg.SetPeers("127.0.0.1:1234, 127.0.0.0:5432")
	if len(cfg.Peers) != 2 {
		t.Fatal("should have 2 peers")
	}

}

func TestConfigValidateAddrs(t *testing.T) {
	cfg := DefaultConfig()

	cfg.BindAddr = "127.0.0.1:12345"
	if err := cfg.ValidateAddrs(); err != nil {
		t.Fatal(err)
	}
	if cfg.BindAddr != cfg.AdvAddr {
		t.Fatal("bind & adv should be the same")
	}

	cfg.BindAddr = ":3245"
	if err := cfg.ValidateAddrs(); err != nil {
		t.Fatal(err)
	}
	if len(cfg.AdvAddr) == 0 {
		t.Error("adv addr not set")
	}

	cfg.AdvAddr = "127.0.0.1:1234"
	if err := cfg.ValidateAddrs(); err != nil {
		t.Fatal(err)
	}
	cfg.AdvAddr = "0.0.0.0:1234"
	if err := cfg.ValidateAddrs(); err == nil {
		t.Error("should fail")
	}
	cfg.AdvAddr = ":1234"
	if err := cfg.ValidateAddrs(); err == nil {
		t.Error("should fail")
	}
	cfg.AdvAddr = ""
	if err := cfg.ValidateAddrs(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(cfg.AdvAddr, ":3245") {
		t.Error("wrong port", cfg.AdvAddr)
	}

}
