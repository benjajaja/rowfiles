package examples

import (
	"bytes"
	"errors"
	"github.com/benjajaja/rowfiles"
	"io"
	"testing"
)

func TestAsyncRead(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		w.Write([]byte("A,B\n"))
		w.Write([]byte("x,y\n"))
		w.Close()
	}()
	rows, err := csvTestModel.ReadAll(ctx, r)
	if err != nil {
		panic(err)
	}
	if len(rows) != 1 {
		panic("should have one row")
	}
	if rows[0] != testRow {
		panic("row are not equal")
	}
}

func TestAsyncReadError(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		w.Write([]byte("A,B\n"))
		w.Write([]byte("x,y\n"))
		w.CloseWithError(errors.New("source error"))
	}()
	_, err := csvTestModel.ReadAll(ctx, r)
	if err.Error() != "source error" {
		panic("error is not \"source error\"")
	}
}

func TestAsyncWrite(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		err := csvTestModel.WriteAll(ctx, w, []row{
			{"x", "y"},
		})
		if err != nil {
			panic(err)
		}
	}()
	bytes, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if string(bytes) != testCSV {
		panic("not equal")
	}
}

func TestAsyncWriteError(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		csvWriter, err := csvTestModel.Writer(ctx, w)
		if err != nil {
			panic(err)
		}
		csvWriter.Write(ctx, testRow)
		csvWriter.Close(ctx, errors.New("source error"))
	}()
	_, err := io.ReadAll(r)
	if err.Error() != "source error" {
		panic("error is not \"source error\"")
	}
}

func TestChannels(t *testing.T) {
	ch, errch := csvTestModel.ReadChan(ctx, bytes.NewReader([]byte(testCSV)))
	r, w := io.Pipe()
	err := csvTestModel.WriteChan(ctx, w, ch, errch)
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	output, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if string(output) != testCSV {
		panic("not equal: " + string(output))
	}
}

func TestChannelsErrors(t *testing.T) {
	ch, errch := csvTestModel.ReadChan(ctx, bytes.NewReader([]byte("A,B\ngarbage...")))
	r, w := io.Pipe()
	err := csvTestModel.WriteChan(ctx, w, ch, errch)
	if err != nil {
		panic(err)
	}

	_, err = io.ReadAll(r)
	if err == nil {
		panic("should have error")
	}
	if err == io.EOF {
		panic("should not be EOF")
	}
}

func TestPipe(t *testing.T) {
	r, err := rowfiles.Pipe(
		ctx,
		bytes.NewReader([]byte(testCSV)),
		csvTestModel,
		jsonTestModel,
	)
	if err != nil {
		panic(err)
	}

	bytes, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if string(bytes) != testJSON {
		panic("not equal")
	}
}
