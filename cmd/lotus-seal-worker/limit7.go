//go:build leshan
// +build leshan

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
	"60af7c30a7ba68fc109e199155fe77f81d1b8d91": {},
	"ccdbaefd9b1f13f37be90c35f14d6d5a666ece8f": {},
	"0b52e7f6e4a9dd0d254b430e9cd491ac182f0e6f": {},
	"d0b834c5f9fc0f42c268455050d331ad3161b54c": {},
	"fa0825e2312ebaef330b5b8853cb57a22d6f7140": {},
	"ee4dca4302db97001d756ea7cf068578d13c3f00": {},
	"4df3720869ec2a288bac4127a01e1da08bccc9ba": {},
	"6026eda96e8d0628c692369c5d897a66497b827f": {},
	"a3224890a87b2989b586853ecd0edd93ca661b9d": {},
	"f87a79d7741d70ea6ee50a688965a74b05bc8956": {},
	"77b054dd6ff9388be89cc9a261b8ca6e7ab66183": {},
	"940d0e2be3f4e13c92ad4e2d4eda354f1e34fb7b": {},
	"2946fca51708bd2c92459ddcbca852ec66cf1a06": {},
	"816a34087f373c5c9d3189bc565c3d2ab2f70387": {},
	"c0a44b416e1c9620a63c8d43daee6bba3961e19a": {},
	"dd3da7969b12ab56ee46a2dc841618f1c7acfa77": {},
	"9a79eba8c6ca0116186fbe2670dd376926797a9e": {},
	"6ebb5ee16e2ce0efee7a6a72531cf2dea78f3b2a": {},
	"428f27378b2ecc65e41fb7c655bfcc269f06b929": {},
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
