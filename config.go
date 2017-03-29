package difuse

import (
	"fmt"
	"hash"
	"net"
	"strings"
	"time"

	"github.com/btcsuite/fastsha256"
	chord "github.com/ipkg/go-chord"

	"github.com/ipkg/difuse/types"
	"github.com/ipkg/difuse/utils"
)

// NetTimeouts holds timeouts for rpc's
type NetTimeouts struct {
	Dial time.Duration
	RPC  time.Duration
	Idle time.Duration
}

// DefaultNetTimeouts initializes sane timeouts
func DefaultNetTimeouts() *NetTimeouts {
	return &NetTimeouts{
		Dial: 3 * time.Second,
		RPC:  5 * time.Second,
		Idle: 300 * time.Second,
	}
}

// Config holds the overall config
type Config struct {
	Chord *chord.Config

	BindAddr string
	AdvAddr  string // Advertise address used by peers

	Peers []string // Existing peers to join

	Timeouts *NetTimeouts

	Signator types.Signator

	FSM FSMFactory

	DataDir string
}

// DefaultConfig returns a sane config
func DefaultConfig() *Config {
	c := &Config{
		Chord:    chord.DefaultConfig(""),
		Timeouts: DefaultNetTimeouts(),
		FSM:      &DummyFSM{},
	}

	c.Chord.NumSuccessors = 7
	c.Chord.NumVnodes = 8
	c.Chord.StabilizeMin = 3 * time.Second
	c.Chord.StabilizeMax = 10 * time.Second
	c.Chord.HashFunc = func() hash.Hash { return fastsha256.New() }

	return c
}

// SetPeers parses a comma separated list of peers into a slice and sets the config.
func (cfg *Config) SetPeers(peers string) {
	for _, v := range strings.Split(peers, ",") {
		if c := strings.TrimSpace(v); c != "" {
			cfg.Peers = append(cfg.Peers, c)
		}
	}
}

// ValidateAddrs validates both bind and adv addresses and set adv if possible.
func (cfg *Config) ValidateAddrs() error {
	if _, err := net.ResolveUDPAddr("udp4", cfg.BindAddr); err != nil {
		return err
	}

	// Check if adv addr is set and validate
	if len(cfg.AdvAddr) != 0 {
		isAdv, err := utils.IsAdvertisableAddress(cfg.AdvAddr)
		if err != nil {
			return err
		} else if !isAdv {
			return fmt.Errorf("address not advertisable")
		}
	} else {
		// Use bind addr if possible
		if isAdv, _ := utils.IsAdvertisableAddress(cfg.BindAddr); isAdv {
			cfg.AdvAddr = cfg.BindAddr
		} else {
			// Lastly, try to auto-detect the ip
			ips, err := utils.AutoDetectIPAddress()
			if err != nil {
				return err
			} else if len(ips) == 0 {
				return fmt.Errorf("could not get advertise address")
			}
			// Add port to advertise address based the one supplied in the bind address
			pp := strings.Split(cfg.BindAddr, ":")
			cfg.AdvAddr = ips[0] + ":" + pp[len(pp)-1]
		}
	}

	cfg.Chord.Hostname = cfg.AdvAddr

	return nil
}
