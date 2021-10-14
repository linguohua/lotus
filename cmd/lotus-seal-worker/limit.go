//go:build dubai
// +build dubai

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
	"b3fc3a65f8262479191d42fc133fb049897a8900": {},
	"61a2d17d384a308592a753055e1b2b0d91f89a70": {},
	"8ea2ddba7800925b99b20114374c5a9b90c57caf": {},
	"462f002523e9862acafb8042cf30edff6c88eebf": {},
	"5578b406ca055fc85a09015e1f7a4861059f0ec2": {},
	"873daa17076ca88935bffccc57f6797267bd2e61": {},
	"fb97eb4d15121b887fd1b53727302cec9ec693a7": {},
	"896d2ec6832c5d7069a1c0b0ae57bf633d11e6da": {},
	"0b87a4316a7e15da2198adff61005683fcdef83c": {},
	"890d04e5789c63fa725d066165b8e2583f923bed": {},
	"4d56375abe245110630d9f1f7b50a2a4ae5c87ef": {},
	"4a996baa671f4311df21104e22ca95a576a181c6": {},
	"3c6614406b2f56ad75144c7bdf06a7029201ba7d": {},
	"db57532ab2d6396868ff9256629b950412bc4ff6": {},
	"7ea50a382d9e4805d249a6fa75e7fb0cf2c62117": {},
	"537e64617e209af30e9333abd4da5298883db108": {},
	"63cfb32d6a44a72781e845d72f9141df9628b876": {},
	"7dd48950de101940e16d685d9df8e74faa64a6aa": {},
	"790821a339af3533ced4884675c085e19e4e9772": {},
	"64238a66cf9222ba51e2daac78b8fc8bbeb5da66": {},
	"536b7551a503ed832280d51e3403c90c67cd89b2": {},
	"d45de6fe55586564aba1f8d43f29e903d5e6e0ad": {},
	"3c20a64ec80b0f03a1f0b07e260b7454f56a6a71": {},
	"48cee66b48fb3d1f504e63ceff3bd57c45e72dec": {},
	"4aa55ce6a12cd945737a602d418a706b0a24a617": {},
	"eecec50709f41446888b7df4632d4434285f7f2b": {},
	"729cc343b50149c67d254e85715a0e85ae049af1": {},
	"f0d9058fe1cf2e2d27dd103a8ed1b3a34950ba5e": {},
	"221c10df0eed2091e1a237d3f6184a20141ba25f": {},
	"47d90316dabc3f9854f595c396d28201ee765daf": {},
	"f315c208f8aafa05bf40255400d023595aaedd49": {},
	"060c14aae88ebbe17425563b5df4d29a46e6dc7f": {},
	"2b80b47f73a9e9433844b68acf1435d933b6f110": {},
	"7bbc9a67fce181ec8ae48788748904502232fced": {},
	"fa7e15e4fda634ec1ae020a56e3f9934fbb2e252": {},
	"619372608a084d929c46c834f8f357a00a84b418": {},
	"276689ae50442837c5cdb2a685fe78b60dd7b120": {},
	"1ba49186976a8db64d0e4258bf7c4e8bbee3520c": {},
	"1814b5e03ae6873601452d1f4fd1ea923c5a7aa4": {},
	"9c244a40949faed4929b8667c31f7cd3f91f6d9e": {},
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
