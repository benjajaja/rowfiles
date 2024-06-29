package examples

import (
	"context"
	"encoding/csv"
	"io"
)

type CSVReader[T any] struct {
	csvReader   *csv.Reader
	header      func() []string
	deserialize func(record []string) (T, error)
}

func (r CSVReader[T]) Read(ctx context.Context) (T, error) {
	record, err := r.csvReader.Read()
	if err != nil {
		return *new(T), err
	}
	return r.deserialize(record)
}

func (r CSVReader[T]) Close(ctx context.Context, err error) error {
	return nil
}

type CSVWriter[T any] struct {
	csvWriter *csv.Writer
	serialize func(row T) ([]string, error)
}

func (r CSVWriter[T]) Write(ctx context.Context, row T) error {
	record, err := r.serialize(row)
	if err != nil {
		return err
	}
	return r.csvWriter.Write(record)
}

func (r CSVWriter[T]) Close(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	r.csvWriter.Flush()
	return r.csvWriter.Error()
}

type CSVModel[T any] struct {
	header      func() []string
	deserialize func(record []string) (T, error)
	serialize   func(row T) ([]string, error)
}

func (m CSVModel[T]) Reader(ctx context.Context, r io.Reader) (CSVReader[T], error) {
	csvReader := csv.NewReader(r)
	_, err := csvReader.Read()
	return CSVReader[T]{csvReader, m.header, m.deserialize}, err
}

func (m CSVModel[T]) Writer(ctx context.Context, w io.Writer) (CSVWriter[T], error) {
	csvWriter := csv.NewWriter(w)
	err := csvWriter.Write(m.header())
	return CSVWriter[T]{csvWriter, m.serialize}, err
}
