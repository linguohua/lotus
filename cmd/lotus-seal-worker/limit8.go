//go:build hongkong
// +build hongkong

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
	"645462d05784f82107557ea41dd44341a3d1751b": {},
	"baed8e5621c417e393efcc75a611aea9783fda30": {},
	"6801d45ea792eac2d7b33b48bc3dd0de43b2e2db": {},
	"9f11a79b4931db1c3f3ed9859f3c65acc63bddf8": {},
	"f51ba401faaf21ef03ed718ccc99f87ca9e4adbc": {},
	"4d1ae90449ec5cc7fd8858915fbf7f94872d73a6": {},
	"f42eac529cea0624e11a095f59d420cf4013404d": {},
	"f4e8ebc9a1040f857eb066c308882f7c51fe3a16": {},
	"cd000649abf7b504b6f90199377f0d6548777817": {},
	"259a77557a7f3add0b679f1e69161c0757a167de": {},
	"6408d469d9781fc03147b5b1e1ca794310a5370d": {},
	"6b0c413c126d0817875f92823f563f8a4489a0b1": {},
	"d3db2c2e309b2fc5a1e55378616eb5efbee7bb3a": {},
	"88b791cd631fb65ba8dac519057da18bc07d122b": {},
	"7576c52cb7f976d0fd1c2b676cf05a779f410ff4": {},
	"263c9091fcd3450b3782d6367266e843266650c9": {},
	"00da1c36046c41fc0c5b0bff878b1edf48875865": {},
	"730949b8fed6b6b7b863d424e9d5feadf659f85a": {},
	"ba873e337e9483b5ab3fab2a7f8ed186e49c9e3c": {},
	"dc355ea4297e5a4a2cfdc0b336431eb5c11e9898": {},
	"6c4afd599ca2de54c092a40b573abc4aef08e8f8": {},
	"c5ff76d983fc11b44e47f0fe13048a9b8670ea6e": {},
	"b2a35b2150e1ae1e4e7c4c21ecfdd407a2bc40cd": {},
	"742d6fb0ed2b705665d72e853500a365ec4a9b2c": {},
	"84306dfdcfb664923d4239ebe31b9a80eefb3059": {},
	"e9dea57fa8c02278cea47a2a7beeeb9a4932c636": {},
	"a379a1f9fb3f816b76ed940e12443f451de68f02": {},
	"7e2e395fecb33d7a159f7716acd5f0bff1ea7c84": {},
	"ea8fff799143a3e4a883be483ba3ab5edc8a296a": {},
	"d423017f13995b8af6b2975a7827b947b3ee45b2": {},
	"849b960087ba67a50f42901ac33508ebf8dcb64b": {},
	"1ae9f0c4e0ee039f7989e7a0abea3c5e2ba953c2": {},
	"a84a4dc457aff088bfbfba38de6927776ecfa3e1": {},
	"29cf4004592834142b53586f6932d912651fd165": {},
	"daf0ff487f9bcb52ded3c7eeb8106f3b1d401c50": {},
	"067627b138c5134b30ca234e42af841139524d11": {},
	"9c0ab52515d2cac837fb959bbdf90c4870db50b6": {},
	"11ff1127be54d98712d543b3a58db1c9c5c0e3b4": {},
	"9459f9168aa20f66d97a8424193c0a3f67755006": {},
	"afdcc4ee89caa0ae469a5fbbe7e67ab816c2de63": {},
	"f50302b60e1b6c0587e0a69df4aa6cbac7c26c67": {},
	"c691d42cc66c6e3e1fe3c870bd54a8fd057e1503": {},
	"9664aed9a77f0fa2759144477206b09aa5bebf78": {},
	"632eb79de115c981e01feb2cb4db9851ff10da10": {},
	"5722329abcc86d0a7001d249bdb8935857d6630f": {},
	"8d733b8d3c42e7bf1e32289c80e2a53329aac073": {},
	"f08407c88b0bbbc925b80fe4cab377340be9c544": {},
	"032a5da416a7103865c32162d72f0ab2d4f0e54a": {},
	"f2cb4560cac6ded5c18555d790e82dbffae657da": {},
	"f59c87b634a303279b4905b1d6b6f80fa4b07f64": {},
	"37a5b4ec1101d73b6bd04f28e9d38752f1fecfe8": {},
	"a9c54ff519033e0ab337677e56404941084d0df3": {},
	"46f2de4e60be524c3e8ecabca77594cc3fb09041": {},
	"359e119952ad680094650fafb8bd4d3d7b4b5873": {},

	"0dbdd1fc2fde58b182d9dc50589996acbd42a9f7": {},
	"886929c546996ed7b94d2dbc18ad86cfd0d75995": {},
	"4b3b584dd9152960429dedb248a586300bf701ba": {},
	"bd8f6efdd0a10f1b4273df1c9a9330d9506a9e05": {},
	"1f15d59466437444da88ca3c76c7338cbe85e151": {},
	"875e0197a1d5b259fa2fe0c61571cd9c2af82b58": {},
	"ff87c49660a5cd6e8c7e610eb3ec533f91178440": {},
	"980e3cbdfccd9a35852500ca354d5f6f1f74d409": {},
	"a35bfc02111bf7025769fb98e97a6dc636967b89": {},
	"1a61e60e91c606d50e4fb6f10bd99dedec08c210": {},
	"dd3d5594848bdc044211ef85bb0f691db77b3938": {},
	"70eb18d2fd56c87a95c93f08ed9e37f1bef4e797": {},
	"317710cbd6938fcb3695fee4310b602dfa5827b3": {},
	"1961d4eae2a80747281d2bde27be983403c98364": {},
	"056069705cb3eff43528ec8c33baec2aa30f3855": {},
	"4f2c20d29ea445760cd55abe04b1aa6a7af0605b": {},
	"26a77f10904eaf4a63f99fd54c741cbc147c370d": {},
	"d850e0b07e38f357af969f0de1e08633e9c3a546": {},
	"37285ccd312616353ee4fc76001a473dbb481cc3": {},
	"174eeab1a972a192d8468884e35f0e594fc46edf": {},
	"fda7d9f2ad2a1c00a81bb73c232f80ff6bcda16a": {},
	"39ec6107dbaf03896ddfd5cc584de19b42df5b46": {},
	"d06dae87b75ec6e024a62d0cd5f9d7981137e952": {},
	"f3015ef17a05c68592fb0913ecf70478f3ddfd11": {},
	"776bdb05df8c8fd0c3de8b94ddedfaaefa66afc4": {},
	"adeafd4d4b14a65a3ad8a993e181a5af30ef23c7": {},
	"9191e8d5e801072d69f156978d30f651d1434122": {},
	"5873272c0036fd83cd6cf3483a4231d37ae99d8d": {},
	"8078f603f7c8ddfcdc792d06a4443c801ab6153c": {},
	"2a2f2c0f5384b4d0c82eda6eb9034873d59806ae": {},
	"2fad8a485dfd51056e0c264d80c0b41f4aa7b0db": {},
	"f958ca9f5e9a31b194ca3a8cbad7a26b63b8075a": {},
	"18dabb73b88ddf79c48e46cae1f1287ebf0475d5": {},
	"de6acb65d62a802bd69eb543a2ebe3ebc52cdf0f": {},
	"3b6c9b051dab832d143e3328aed568fb32759209": {},
	"d80391905072fda04511baeaa30279cb55611596": {},
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
