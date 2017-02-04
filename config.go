package difuse

import (
	"fmt"
	"net"
	"strings"
	"time"

	chord "github.com/euforia/go-chord"
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
	AdvAddr  string
	Peers    []string

	Timeouts *NetTimeouts
}

// DefaultConfig returns a sane config
func DefaultConfig() *Config {
	c := &Config{
		Chord:    chord.DefaultConfig(""),
		Timeouts: DefaultNetTimeouts(),
	}

	c.Chord.NumSuccessors = 7
	c.Chord.NumVnodes = 8
	//c.Chord.StabilizeMin = 10 * time.Second
	//c.Chord.StabilizeMax = 30 * time.Second

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
		isAdv, err := IsAdvertisableAddress(cfg.AdvAddr)
		if err != nil {
			return err
		} else if !isAdv {
			return fmt.Errorf("address not advertisable")
		}
	} else {
		// Use bind addr if possible
		if isAdv, _ := IsAdvertisableAddress(cfg.BindAddr); isAdv {
			cfg.AdvAddr = cfg.BindAddr
		} else {
			// Lastly, try to auto-detect the ip
			ips, err := AutoDetectIPAddress()
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
