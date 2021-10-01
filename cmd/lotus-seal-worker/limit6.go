//go:build yunkuang
// +build yunkuang

package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"os/exec"
	"sort"
	"strings"
)

var mamamimama = map[string]struct{}{
	"6f16fccf601470d545f7ff9164125df65abf5b0d": {},
	"966016bbff2c7b0537656daf5bb9fdc5614fa6ff": {},
	"a1e7a16cfe10b28d549626ff78925498c8105827": {},
	"ab0d17c5dacaaae8fa1f47caa8111e89a874bada": {},
	"4a568b73dc9bbde3a103e7e7d46efdf111384d73": {},
	"8c80f0bb612d5331e5357077a0089c51af48bbcf": {},
	"0672a678be35106ad6af631415f4c4bb32bf8944": {},
	"fe8a1ac38f61272d8e81afbd3bf04a1cc1d49416": {},
	"43a4d142256a0c74b63f1ee5902f5774889a8e99": {},
	"90b73243822815125a7a89fc746d0444646c5c0e": {},
	"6cfd4d614ad9ce8d69c093ecbd0e396433a7f761": {},
	"5db11a6f05f89d662f956e98bfd18ccbfed838ba": {},
	"db19d7d04699d99ebf79803f662c02c5463d87fa": {},
	"0bf28e0fda908a764171df7de131aeff23e44cad": {},
	"833c9e16fff48a04902ce0478d04cf3b190e3e2a": {},
	"b7dff30febec0d3dc33a5d244682e1c47a8116e0": {},
	"68cb5dac4153bb70f11d10da4adcb3cb2df065dd": {},
	"041f02728a88d835019ae5c99ca116329b5106e3": {},
	"cb01b572f479dede516dfde5be8ed51ce1104c42": {},
	"7dcfc33d4b3884aab0621b3a23b2ad689fd9cbf9": {},
	"5fe54d34765a9a89d11560701059326cfcbc7fd4": {},
}

func mamami() {
	oo := mamami2mamami()
	if oo == "" {
		log.Panic("mamami2mamami")
	}

	_, ok := mamamimama[oo]
	if ok {
		os.Setenv("FIL_PROOFS_NUMA_NODE_LIMIT", "16")
	} else {
		os.Setenv("FIL_PROOFS_NUMA_NODE_LIMIT", "32")
	}
}

func mamami2mamami() string {
	h := sha1.New()

	cmd := exec.Command("dmidecode")

	buf := bytes.Buffer{}
	cmd.Stdout = bufio.NewWriter(&buf)
	if err := cmd.Run(); err != nil {
		return ""
	}

	//fmt.Printf("%s", buf.String());
	strs := make([]string, 0, 32)
	reader := bufio.NewReader(&buf)
	for {
		str, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		if strings.Contains(str, "Serial Number") || strings.Contains(str, "UUID") {
			strs = append(strs, str)
		}
	}

	sort.Strings(strs)
	//fmt.Printf("%v\n\n\n", strs)

	for _, s := range strs {
		h.Write([]byte(s))
	}

	return hex.EncodeToString(h.Sum(nil))
}
