package examples

import (
	"io"
	"os"
	"testing"
)

type closeFile struct {
	*os.File
	closed bool
}

func (cf *closeFile) Close() error {
	err := cf.File.Close()
	cf.closed = true
	return err
}

func TestFile(t *testing.T) {
	rawFile, err := os.Open("./example.csv")
	if err != nil {
		panic(err)
	}
	file := closeFile{rawFile, false}

	ch, errch := csvTestModel.ReadChan(ctx, &file)

	r, w := io.Pipe()
	err = jsonTestModel.WriteChan(ctx, w, ch, errch)
	if err != nil {
		panic(err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	snapshot, err := os.ReadFile("./example.json")
	if err != nil {
		panic(err)
	}
	if string(data) != string(snapshot) {
		panic("snapshot not equal")
	}

	if !file.closed {
		panic("file should have been closed")
	}
}
