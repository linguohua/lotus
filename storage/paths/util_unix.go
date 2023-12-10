package paths

import (
	"bytes"

	"fmt"
	"io/ioutil"

	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
)

// func move(from, to string) error {
// 	from, err := homedir.Expand(from)
// 	if err != nil {
// 		return xerrors.Errorf("move: expanding from: %w", err)
// 	}

// 	to, err = homedir.Expand(to)
// 	if err != nil {
// 		return xerrors.Errorf("move: expanding to: %w", err)
// 	}

// 	if filepath.Base(from) != filepath.Base(to) {
// 		return xerrors.Errorf("move: base names must match ('%s' != '%s')", filepath.Base(from), filepath.Base(to))
// 	}

// 	log.Debugw("move sector data", "from", from, "to", to)

// 	// verify if the file exist in target dir
// 	fifrom, err := os.Stat(from)
// 	if err != nil {
// 		log.Errorf("move sector: os.Stat(%s) failed:%s ----> %s, error:%v", from, err)
// 		return xerrors.Errorf("os.Stat(%s): %w", from, err)
// 	}

// 	toDir := filepath.Dir(to)

// 	// `mv` has decades of experience in moving files quickly; don't pretend we
// 	//  can do better

// 	var errOut bytes.Buffer
// 	log.Infof("util_unix.call /usr/bin/env cp bein:%s ----> %s", from, to)
// 	cmd := exec.Command("/usr/bin/env", "cp", "-r", "-t", toDir, from) // nolint
// 	cmd.Stderr = &errOut
// 	if err := cmd.Run(); err != nil {
// 		log.Errorf("util_unix.call /usr/bin/env cp failed:%s ----> %s, error:%v", from, to, err)
// 		return xerrors.Errorf("exec cp (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
// 	}
// 	log.Infof("util_unix.call /usr/bin/env cp end:%s ----> %s, begin verify...", from, to)

// 	fito, err := os.Stat(to)
// 	if err != nil {
// 		log.Errorf("move sector: os.Stat(%s) , error:%v", to, err)
// 		return xerrors.Errorf("os.Stat(%s): %w", to, err)
// 	}

// 	if !fifrom.IsDir() {
// 		if fifrom.Size() != fito.Size() {
// 			msg := fmt.Sprintf("move sector: os.Stat size not match, from [%s] %d != to [%s] %d",
// 				from, fifrom.Size(), to, fito.Size())
// 			log.Errorf(msg)
// 			return xerrors.Errorf(msg)
// 		}
// 	} else {
// 		// dir, enumerate each source file
// 		files, err := ioutil.ReadDir(from)
// 		if err != nil {
// 			msg := fmt.Sprintf("move sector: ioutil.ReadDir(%s) failed,error:%v", from, err)
// 			log.Errorf(msg)
// 			return xerrors.Errorf(msg)
// 		}

// 		for _, f := range files {
// 			// only check level 1 files
// 			if f.IsDir() {
// 				continue
// 			}

// 			dstfn := filepath.Join(to, f.Name())
// 			dstfi, err := os.Stat(dstfn)
// 			if err != nil {
// 				log.Errorf("move sector: os.Stat(%s) , error:%v", dstfn, err)
// 				return xerrors.Errorf("move sector: os.Stat(%s): %w", dstfn, err)
// 			}

// 			if f.Size() != dstfi.Size() {
// 				srcfn := filepath.Join(from, f.Name())
// 				msg := fmt.Sprintf("move sector: os.Stat size not match, from [%s] %d != to [%s] %d",
// 					srcfn, f.Size(), dstfn, dstfi.Size())
// 				log.Errorf(msg)
// 				return xerrors.Errorf(msg)
// 			}
// 		}
// 	}

// 	log.Infof("move sector: %s ----> %s, verify ok, now call rm -rf to remove %s", from, to, from)

// 	// remove
// 	cmd = exec.Command("/usr/bin/env", "rm", "-rf", from) // nolint
// 	cmd.Stderr = &errOut
// 	if err := cmd.Run(); err != nil {
// 		log.Infof("util_unix.call /usr/bin/env rm failed:%s ----> %s, error:%v", from, to, err)
// 	}

// 	return nil
// }

func utilCopy(from, to string) error {
	from, err := homedir.Expand(from)
	if err != nil {
		return xerrors.Errorf("utilCopy, move: expanding from: %w", err)
	}

	to, err = homedir.Expand(to)
	if err != nil {
		return xerrors.Errorf("utilCopy, move: expanding to: %w", err)
	}

	if filepath.Base(from) != filepath.Base(to) {
		return xerrors.Errorf("utilCopy, move: base names must match ('%s' != '%s')", filepath.Base(from), filepath.Base(to))
	}

	log.Debugw("utilCopy, move sector data", "from", from, "to", to)

	// verify if the file exist in target dir
	fifrom, err := os.Stat(from)
	if err != nil {
		log.Errorf("utilCopy, move sector: os.Stat(%s) failed:%s ----> %s, error:%v", from, err)
		return xerrors.Errorf("utilCopy, os.Stat(%s): %w", from, err)
	}

	toDir := filepath.Dir(to)

	// `mv` has decades of experience in moving files quickly; don't pretend we
	//  can do better

	var errOut bytes.Buffer

	log.Infof("utilCopy, call /usr/bin/env cp bein:%s ----> %s", from, to)
	cmd := exec.Command("/usr/bin/env", "cp", "-r", "-t", toDir, from) // nolint
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		log.Errorf("utilCopy, call /usr/bin/env cp failed:%s ----> %s, error:%v", from, to, err)
		return xerrors.Errorf("exec cp (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	log.Infof("utilCopy, call /usr/bin/env cp end:%s ----> %s, begin verify...", from, to)

	fito, err := os.Stat(to)
	if err != nil {
		log.Errorf("utilCopy, move sector: os.Stat(%s) , error:%v", to, err)
		return xerrors.Errorf("utilCopy, os.Stat(%s): %w", to, err)
	}

	if !fifrom.IsDir() {
		if fifrom.Size() != fito.Size() {
			msg := fmt.Sprintf("utilCopy, move sector: os.Stat size not match, from [%s] %d != to [%s] %d",
				from, fifrom.Size(), to, fito.Size())
			log.Errorf(msg)
			return xerrors.Errorf(msg)
		}
	} else {
		// dir, enumerate each source file
		files, err := ioutil.ReadDir(from)
		if err != nil {
			msg := fmt.Sprintf("utilCopy, move sector: ioutil.ReadDir(%s) failed,error:%v", from, err)
			log.Errorf(msg)
			return xerrors.Errorf(msg)
		}

		for _, f := range files {
			// only check level 1 files
			if f.IsDir() {
				continue
			}

			dstfn := filepath.Join(to, f.Name())
			dstfi, err := os.Stat(dstfn)
			if err != nil {
				log.Errorf("utilCopy,move sector: os.Stat(%s) , error:%v", dstfn, err)
				return xerrors.Errorf("utilCopy,move sector: os.Stat(%s): %w", dstfn, err)
			}

			if f.Size() != dstfi.Size() {
				srcfn := filepath.Join(from, f.Name())
				msg := fmt.Sprintf("utilCopy,move sector: os.Stat size not match, from [%s] %d != to [%s] %d",
					srcfn, f.Size(), dstfn, dstfi.Size())
				log.Errorf(msg)
				return xerrors.Errorf(msg)
			}
		}
	}

	log.Infof("utilCopy, move sector: %s ----> %s, cp verify ok ", from, to)

	return nil
}

func utilCopy2(from, to, bandwidth string) error {
	from, err := homedir.Expand(from)
	if err != nil {
		return xerrors.Errorf("utilCopy, move: expanding from: %w", err)
	}

	to, err = homedir.Expand(to)
	if err != nil {
		return xerrors.Errorf("utilCopy, move: expanding to: %w", err)
	}

	if filepath.Base(from) != filepath.Base(to) {
		return xerrors.Errorf("utilCopy, move: base names must match ('%s' != '%s')", filepath.Base(from), filepath.Base(to))
	}

	log.Debugw("utilCopy, move sector data", "from", from, "to", to)

	// verify if the file exist in target dir
	fifrom, err := os.Stat(from)
	if err != nil {
		log.Errorf("utilCopy, move sector: os.Stat(%s) failed:%s ----> %s, error:%v", from, err)
		return xerrors.Errorf("utilCopy, os.Stat(%s): %w", from, err)
	}

	toDir := filepath.Dir(to)

	// `mv` has decades of experience in moving files quickly; don't pretend we
	//  can do better

	var errOut bytes.Buffer
	var cmd *exec.Cmd
	log.Infof("utilCopy, call /usr/bin/env rsync bein:%s ----> %s, with bandwidth:%s", from, to, bandwidth)
	if bandwidth != "" {
		cmd = exec.Command("/usr/bin/env", "rsync", "--bwlimit", bandwidth, "-a", from, toDir) // nolint

	} else {
		cmd = exec.Command("/usr/bin/env", "rsync", "-a", from, toDir) // nolint
	}

	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		log.Errorf("utilCopy, call /usr/bin/env rsync failed:%s ----> %s, error:%v", from, to, err)
		return xerrors.Errorf("exec rsync (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	log.Infof("utilCopy, call /usr/bin/env rsync end:%s ----> %s, begin verify...", from, to)

	fito, err := os.Stat(to)
	if err != nil {
		log.Errorf("utilCopy, move sector: os.Stat(%s) , error:%v", to, err)
		return xerrors.Errorf("utilCopy, os.Stat(%s): %w", to, err)
	}

	if !fifrom.IsDir() {
		if fifrom.Size() != fito.Size() {
			msg := fmt.Sprintf("utilCopy, move sector: os.Stat size not match, from [%s] %d != to [%s] %d",
				from, fifrom.Size(), to, fito.Size())
			log.Errorf(msg)
			return xerrors.Errorf(msg)
		}
	} else {
		// dir, enumerate each source file
		files, err := ioutil.ReadDir(from)
		if err != nil {
			msg := fmt.Sprintf("utilCopy, move sector: ioutil.ReadDir(%s) failed,error:%v", from, err)
			log.Errorf(msg)
			return xerrors.Errorf(msg)
		}

		for _, f := range files {
			// only check level 1 files
			if f.IsDir() {
				continue
			}

			dstfn := filepath.Join(to, f.Name())
			dstfi, err := os.Stat(dstfn)
			if err != nil {
				log.Errorf("utilCopy,move sector: os.Stat(%s) , error:%v", dstfn, err)
				return xerrors.Errorf("utilCopy,move sector: os.Stat(%s): %w", dstfn, err)
			}

			if f.Size() != dstfi.Size() {
				srcfn := filepath.Join(from, f.Name())
				msg := fmt.Sprintf("utilCopy,move sector: os.Stat size not match, from [%s] %d != to [%s] %d",
					srcfn, f.Size(), dstfn, dstfi.Size())
				log.Errorf(msg)
				return xerrors.Errorf(msg)
			}
		}
	}

	log.Infof("utilCopy, move sector: %s ----> %s, cp verify ok ", from, to)

	return nil
}

func utilRemove(p string) error {
	log.Infof("utilRemove, move sector: now remove %s", p)

	// remove
	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "rm", "-rf", p) // nolint

	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		log.Errorf("utilRemove, call /usr/bin/env rm failed:%s, error:%v", p, err)
	}

	return nil
}
