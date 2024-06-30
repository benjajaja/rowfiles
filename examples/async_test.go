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
	rows, err := csvTestFormat.ReadAll(ctx, r)
	if err != nil {
		panic(err)
	}
	if len(rows) != 1 {
		panic("should have one row")
	}
	if rows[0] != (row{"x", "y"}) {
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
	_, err := csvTestFormat.ReadAll(ctx, r)
	if err.Error() != "source error" {
		panic("error is not \"source error\"")
	}
}

func TestAsyncWrite(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		err := csvTestFormat.WriteAll(ctx, w, []row{{"c", "d"}})
		if err != nil {
			panic(err)
		}
	}()
	bytes, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if string(bytes) != testCSV {
		panic("not equal: " + string(bytes))
	}
}

func TestAsyncWriteError(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		csvWriter, err := csvTestFormat.Writer(ctx, w)
		if err != nil {
			panic(err)
		}
		csvWriter.Write(ctx, (row{"x", "y"}))
		csvWriter.Close(ctx, errors.New("source error"))
	}()
	_, err := io.ReadAll(r)
	if err.Error() != "source error" {
		panic("error is not \"source error\"")
	}
}

func TestChannels(t *testing.T) {
	ch := csvTestFormat.ReadChan(ctx, bytes.NewReader([]byte(testCSV)))
	r, w := io.Pipe()
	err := csvTestFormat.WriteChan(ctx, w, ch)
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
	ch := csvTestFormat.ReadChan(ctx, bytes.NewReader([]byte("A,B\ngarbage...")))
	r, w := io.Pipe()
	err := csvTestFormat.WriteChan(ctx, w, ch)
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
		csvTestFormat,
		jsonTestFormat,
	)
	if err != nil {
		panic(err)
	}

	bytes, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if string(bytes) != `{"a":"c","b":"d"}
` {
		panic("not equal")
	}
}

func TestMerge(t *testing.T) {
	reader1, err := csvTestFormat.Reader(ctx, bytes.NewReader([]byte(testCSV)))
	if err != nil {
		panic(err)
	}
	reader2, err := jsonTestFormat.Reader(ctx, bytes.NewReader([]byte(testJSON)))
	if err != nil {
		panic(err)
	}

	r, err := rowfiles.Merge(
		ctx,
		csvTestFormat,
		reader1,
		reader2,
	)
	if err != nil {
		panic(err)
	}
	bytes, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	if string(bytes) != `A,B
c,d
c,d
` {
		panic("not equal: " + string(bytes))
	}
}
