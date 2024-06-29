package examples

import (
	"bytes"
	"context"
	"io"
	"rowfiles"
	"testing"
)

type row struct {
	A string `json:"a"`
	B string `json:"b"`
}

var testRow = row{"x", "y"}
var ctx = context.Background()

var csvTestModel = rowfiles.NewRowModel[row](CSVModel[row]{
	func() []string {
		return []string{"A", "B"}
	},
	func(record []string) (row, error) {
		return row{record[0], record[1]}, nil
	},
	func(row row) ([]string, error) {
		return []string{row.A, row.B}, nil
	},
})

const testCSV = `A,B
x,y
`
const testJSON = `{"a":"x","b":"y"}
`

var jsonTestModel = rowfiles.NewRowModel[row](JSONLinesModel[row]{})

func TestReadCSV(t *testing.T) {
	var reader rowfiles.RowReader[row]
	var err error
	reader, err = csvTestModel.Reader(ctx, bytes.NewReader([]byte(testCSV)))
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
	err = reader.Close(ctx, nil)
	if err != nil {
		panic(err)
	}
}

func TestWriteCSV(t *testing.T) {
	var writer rowfiles.RowWriter[row]
	var err error
	buf := bytes.NewBuffer([]byte{})
	writer, err = csvTestModel.Writer(ctx, buf)
	if err != nil {
		panic(err)
	}
	err = writer.Write(ctx, testRow)
	if err != nil {
		panic(err)
	}
	err = writer.Close(ctx, nil)
	if err != nil {
		panic(err)
	}
	if buf.String() != testCSV {
		panic("not equal")
	}
}

func TestCreateCSVReaderError(t *testing.T) {
	// We know that CSVReader produces an error if there's no header.
	_, err := csvTestModel.Reader(ctx, bytes.NewReader([]byte{}))
	if err == nil {
		panic("should return error")
	}
}

func TestReadCSVEOF(t *testing.T) {
	reader, err := csvTestModel.Reader(ctx, bytes.NewReader([]byte("A,B\n")))
	if err != nil {
		panic(err)
	}
	_, err = reader.Read(ctx)
	if err == nil {
		panic("should return io.EOF")
	}
}

func TestMix(t *testing.T) {
	rows, err := csvTestModel.ReadAll(ctx, bytes.NewReader([]byte(testCSV)))
	if err != nil {
		panic(err)
	}
	buf := bytes.NewBuffer([]byte{})
	err = jsonTestModel.WriteAll(ctx, buf, rows)
	if err != nil {
		panic(err)
	}
	if buf.String() != testJSON {
		panic("not equal: " + buf.String())
	}
}
