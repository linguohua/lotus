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
	"4e336cee35f5cbc819f96ef81a62ebeaa61deb58": {},
	"6b1685fb6df8c5bd3d969ba6038f4d15d601b022": {},
	"239907e7dd0374687956a79620239d6cba0c6ffe": {},
	"8799c75f77c6b6da0da786575520de3a2df6ab53": {},
	"6c8ef60cd3e4cfc68b84cfcd3a458577216e3c19": {},
	"25867a56e0dccec50130720655c1a53b31202945": {},
	"0bd97d736b5fdf489dd9b062bb3e1b5dd1fa4eb0": {},
	"de460d90cf0b8ddad8537c7797f18d93f47ac793": {},
	"9b0f3bd7903ab8ed3902a19ac95e8c41c22f761d": {},
	"ca90667feeb6e9227d6968566ad5f774717116e4": {},
	"e5f8ff51930629867debe66641466e0f9b6caed6": {},
	"de7642f9e3f321090ef05251b23c6c95f43f9f83": {},
	"5e5c74a3f1b65c2718374d5badcf51d7041e87c8": {},
	"c20a601dd0a9715bc0fa11665b11fca70ca7f1ad": {},
	"128566ed5cf5e0b0b05d7cfa5af4b3d42293fb22": {},
	"23d1b6c5190efbd58fc8ccc043871644ab19d84b": {},
	"ecf8051a60edadaf5b838130aa87b7805f999b70": {},
	"b99d83d700c918823d45b45ceadb9bc360af67b0": {},
	"65b09f39fe3c05d52b9aa66e49162bec8fb73c24": {},
	"600e0af203fd67bac60a2c1857a26b2eb57b0f89": {},
	"bcdaf6575227a694dabb8f2aa650e515d24ed788": {},
	"9a55bb4acf59b80d69763dcb4fffd46aa84790b8": {},
	"fa0536d475e6d9c1a9cb58017a5f1a300d498f7f": {},
	"237b7d9d0f07f580ca9c9609d0e157b8cb728c55": {},
	"12cd78428a85786e88348c099b3e726a77d8f570": {},
	"fad12208aa6d5d65b6d52a660b1682a31ba49c54": {},
	"36f1d59b0fafb3be146a33642c47f46d4e7a155f": {},
	"62e250abd64cd7da8e2c1029ddd80ffce805eb91": {},
	"3d6f423237ea5be270a6a8f2d8c8cc7a570e0d58": {},
	"7c1230feb21c2c70bfd73a768823051679aa8548": {},
	"8274ba8f3c9b4a84e15c9215cd9fb289c4ba776f": {},
	"b6a74f4528873e0f97491c094ff6160708f01710": {},
	"81f0074c064eae7cb181285dd3164b6ec38a11e4": {},
	"f049fa7c591f3cc9f64946d019342520da955dfe": {},
	"63aece7fd1e8de73a836cf4033ff38489fb6c767": {},
	"b98cd428065568bd1d0f814489127e3ca45f01e4": {},
	"76137fbac9e6265bf1190b2e6dadb5f882baac5e": {},
	"579a36f206d7539d6925954a56179296b5aff6b4": {},
	"79b2b7a5bdcb74a225772bfd8c03963ba96ab3a6": {},
	"ba39cb2ce8b16e25c1e06bc4b3c2f40ecaa63a9b": {},
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
