package examples

import (
	"io"
	"os"
	"testing"
)

func TestFile(t *testing.T) {
	file, err := os.Open("./example.csv")
	if err != nil {
		panic(err)
	}

	ch, errch := csvTestModel.ReadChan(ctx, file)

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
}
