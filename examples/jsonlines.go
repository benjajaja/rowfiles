package examples

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
)

type JSONLinesReader[T any] struct {
	scanner *bufio.Scanner
}

func (r JSONLinesReader[T]) Read(ctx context.Context) (T, error) {
	if !r.scanner.Scan() {
		return *new(T), io.EOF
	}
	out := *new(T)
	return out, json.Unmarshal(r.scanner.Bytes(), &out)
}

func (r JSONLinesReader[T]) Close(ctx context.Context, err error) error {
	return nil
}

type JSONLinesWriter[T any] struct {
	ioWriter io.Writer
}

func (w JSONLinesWriter[T]) Write(ctx context.Context, row T) error {
	record, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = w.ioWriter.Write(record)
	if err != nil {
		return err
	}
	_, err = w.ioWriter.Write([]byte{'\n'})
	return err
}

func (r JSONLinesWriter[T]) Close(ctx context.Context, err error) error {
	return err
}

// jsonLinesModel simply writes JSON lines, leveraging the json package.
type JSONLinesModel[T any] struct{}

func (m JSONLinesModel[T]) Reader(ctx context.Context, r io.Reader) (JSONLinesReader[T], error) {
	scanner := bufio.NewScanner(r)
	return JSONLinesReader[T]{scanner}, nil
}

func (m JSONLinesModel[T]) Writer(ctx context.Context, w io.Writer) (JSONLinesWriter[T], error) {
	return JSONLinesWriter[T]{w}, nil
}
