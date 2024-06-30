// Rowfiles 🚣
//
// Go library for reading files that contain rows, similar to io.Reader and io.Writer but `T`
// instead of `byte`.
//
// Some typical formats are CSV, JSONLines, or Parquet.
package rowfiles

import (
	"context"
	"io"
	"sync"
)

// Read rows until EOF.
type RowReader[T any] interface {
	// Read the next row. Returns io.EOF if no more rows.
	Read(context.Context) (T, error)
	// Close the underlying io.Reader, io.ReadCloser, or io.PipeReader.
	Close(context.Context, error) error
}

// Write rows and close.
type RowWriter[T any] interface {
	// Write one row.
	Write(context.Context, T) error
	// Close the format and the underlying io.Writer, io.WriteCloser, or io.PipeWriter.
	Close(context.Context, error) error
}

// Create row readers and writers
type RowModel[T any] interface {
	// Create a RowReader[T] instance.
	Reader(context.Context, io.Reader) (RowReader[T], error)
	// Create a RowWriter[T] instance.
	Writer(context.Context, io.Writer) (RowWriter[T], error)

	// Read all rows
	ReadAll(context.Context, io.Reader) ([]T, error)
	// Write all rows
	WriteAll(context.Context, io.Writer, []T) error

	// Read all rows as channels
	ReadChan(context.Context, io.Reader) <-chan Result[T]
	// Write all rows in channel
	WriteChan(context.Context, io.Writer, <-chan Result[T]) error
}

type Result[T any] struct {
	Result *T
	Err    error
}

// Pipe all rows from one model to another.
func Pipe[T any](
	ctx context.Context,
	r io.Reader,
	in RowModel[T],
	out RowModel[T],
) (io.Reader, error) {
	ch := in.ReadChan(ctx, r)

	r, w := io.Pipe()
	err := out.WriteChan(ctx, w, ch)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func Merge[T any](
	ctx context.Context,
	out RowModel[T],
	readers ...RowReader[T],
) (io.Reader, error) {

	r, w := io.Pipe()

	merged := make(chan Result[T])

	var wg sync.WaitGroup
	wg.Add(len(readers))
	run := func(reader RowReader[T]) {
		for {
			row, err := reader.Read(ctx)
			if err != io.EOF {
				merged <- Result[T]{&row, err}
			}
			if err != nil {
				break
			}
		}
		wg.Done()
	}
	for _, reader := range readers {
		go run(reader)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()

	err := out.WriteChan(ctx, w, merged)
	if err != nil {
		return nil, err
	}
	return r, nil
}
