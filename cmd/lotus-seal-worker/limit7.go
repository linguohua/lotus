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
	"1fb2b4420d0c9dc544d3d84a59c504b8b9a2f848": {},
	"f9365e7a15175b6ec320a741eab08a4d149e115e": {},
	"b503568ae349729676081ec4f19b524cf474e02d": {},
	"a83fd3af7925d380956f8dff3f06c9fdb75c9045": {},
	"039b30966e0ce71fcd069cc03c61cb96abd26ce8": {},
	"43c5b6f6a223f22bb73d53878b111592eea8b586": {},
	"97d92b02da83eed389582a665f7700cac34e6201": {},
	"ade539be664226ee31ff64ea8a4f456b48b64227": {},
	"d89b423e4708c2342941a17da4d4fc9fca4b94e8": {},
	"9d240ffbaf9a2814b5584ce213a7f4e9de6c1a53": {},
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
