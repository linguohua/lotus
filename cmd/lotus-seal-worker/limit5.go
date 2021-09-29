//go:build changsha
// +build changsha

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
	"9bfdbb2866ce55c217c2d85b62196e874f55e0c3": {},
	"a103b47b29ad8549c323cc69005488f31ebc2fe9": {},
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
