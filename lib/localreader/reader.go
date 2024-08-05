package localreader

import (
	"fmt"
	"io"
	"os"

	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	logging "github.com/ipfs/go-log/v2"
	carv2 "github.com/ipld/go-car/v2"
)

var log = logging.Logger("rpcenc")

type CarReader struct {
	URL            string
	PaddedSize     uint64
	unPaddedReader io.Reader
	carReader      *carv2.Reader
}

func (r *CarReader) Close() error {
	u := r.URL
	r.URL = ""
	if r.carReader != nil {
		log.Infof("CarReader Close call, path:%s, padded size:%d", u, r.PaddedSize)
		return r.carReader.Close()
	}
	return nil
}

func (r *CarReader) Read(out []byte) (int, error) {
	if r.unPaddedReader == nil {
		if r.carReader != nil {
			r.carReader.Close()
		}

		cr, rr, err := openCarfile(r.URL, r.PaddedSize)
		if err != nil {
			return 0, err
		}

		r.carReader = cr
		r.unPaddedReader = rr
	}

	return r.unPaddedReader.Read(out)
}

func NewWithUnPaddedReader(filepath string, paddedSize uint64, unPaddedReader io.Reader) *CarReader {
	return &CarReader{URL: filepath, unPaddedReader: unPaddedReader, PaddedSize: paddedSize}
}

func NewWithPath(filepath string, paddedSize uint64) (*CarReader, error) {
	return &CarReader{URL: filepath, PaddedSize: paddedSize}, nil
}

func openCarfile(filepath string, paddedSize uint64) (*carv2.Reader, io.Reader, error) {

	// Open a reader against the CAR file with the deal data
	v2r, err := carv2.OpenReader(filepath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open CARv2 file: %w", err)
	}

	var size uint64
	switch v2r.Version {
	case 1:
		st, err := os.Stat(filepath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to stat CARv1 file: %w", err)
		}
		size = uint64(st.Size())
	case 2:
		size = v2r.Header.DataSize
	}

	r, err := v2r.DataReader()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get data reader over CAR file: %w", err)
	}

	paddedReader, err := padreader.NewInflator(r, size, abi.UnpaddedPieceSize(paddedSize))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create inflator: %w", err)
	}

	return v2r, paddedReader, nil
}
