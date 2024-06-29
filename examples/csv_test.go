package examples

import (
	"bytes"
	"context"
	"io"
	"rowfiles"
	"testing"
)

type row struct {
	a string
	b string
}

var testModel = rowfiles.NewRowModel[row](CSVModel[row]{
	func() []string {
		return []string{"A", "B"}
	},
	func(record []string) (row, error) {
		return row{record[0], record[1]}, nil
	},
	func(row row) ([]string, error) {
		return []string{row.a, row.b}, nil
	},
})

const testCSV = `A,B
x,y
`

var testRow = row{"x", "y"}
var ctx = context.Background()

func TestReadCSV(t *testing.T) {
	var reader rowfiles.RowReader[row]
	var err error
	reader, err = testModel.Reader(ctx, bytes.NewReader([]byte(testCSV)))
	if err != nil {
		panic(err)
	}
	one, err := reader.Read(ctx)
	if err != nil {
		panic(err)
	}
	if one != testRow {
		panic("not equal")
	}
	_, err = reader.Read(ctx)
	if err != io.EOF {
		panic("not io.EOF")
	}
}

func TestWriteCSV(t *testing.T) {
	var writer rowfiles.RowWriter[row]
	var err error
	buf := bytes.NewBuffer([]byte{})
	writer, err = testModel.Writer(ctx, buf)
	if err != nil {
		panic(err)
	}
	err = writer.Write(ctx, testRow)
	if err != nil {
		panic(err)
	}
	writer.Close(ctx, nil)
	if buf.String() != testCSV {
		panic("not equal")
	}
}

func TestCreateCSVReaderError(t *testing.T) {
	_, err := testModel.Reader(ctx, bytes.NewReader([]byte{}))
	if err == nil {
		panic("should return error")
	}
}

func TestReadCSVError(t *testing.T) {
	reader, err := testModel.Reader(ctx, bytes.NewReader([]byte("A,B\n")))
	if err != nil {
		panic(err)
	}
	_, err = reader.Read(ctx)
	if err == nil {
		panic("should return io.EOF")
	}
}
