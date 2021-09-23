//go:build jm
// +build jm

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
	"fbe5662acca83adc640a7829bb947a5ff7e8d60b": {},
	"da20ad4a77b44b4497deb6b2d24c949c96b6e89f": {},
	"e2bfd7800fb23451fa3f67d72a7efae17d8d82c3": {},
	"b84038aeee5c451c29efd43493b8c79323fbb895": {},
	"e4cb2c4e70ec6fc6e088859dcdc7d8dc57c23cf4": {},
	"07d1e211a6be4b9ccb5571a6f6679fb8294ea9c7": {},
	"87e16798c7b5f33fc19c6a3ba8d7f827118b3f9f": {},
	"b81bb2b45489ad6b909679c7974a21ca5e673f4c": {},
	"8ebbfb17978d6f86e34cbd503abe99e6ef446f31": {},
	"1441dcfb771c8793c86c1efd60c909a3578edb83": {},
	"02dc1e038c1a51bd505af83e72ba97ca3a66347b": {},
	"6a8c2ba0ce4fefe6a5c8c7ebca0df59f03f3006d": {},
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
