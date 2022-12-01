package localreader

import (
	"fmt"
	"io"
	"os"

	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	carv2 "github.com/ipld/go-car/v2"
)

type CarReader struct {
	URL          string
	PaddedSize   uint64
	paddedReader io.Reader
	carReader    *carv2.Reader
}

func (h *CarReader) Close() error {
	h.URL = ""
	if h.carReader != nil {
		return h.carReader.Close()
	}
	return nil
}

func (r *CarReader) Read(out []byte) (int, error) {
	if r.paddedReader == nil {
		if r.carReader != nil {
			r.carReader.Close()
		}

		cr, rr, err := openCarfile(r.URL, r.PaddedSize)
		if err != nil {
			return 0, err
		}

		r.carReader = cr
		r.paddedReader = rr
	}

	return r.paddedReader.Read(out)
}

func NewWithPaddedReader(filepath string, paddedSize uint64, paddedReader io.Reader) *CarReader {
	return &CarReader{URL: filepath, paddedReader: paddedReader, PaddedSize: paddedSize}
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
