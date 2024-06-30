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
	// Usually a no-op.
	Close(context.Context, error) error
}

// Write rows and close.
type RowWriter[T any] interface {
	// Write one row.
	Write(context.Context, T) error
	// Close the format.
	Close(context.Context, error) error
}

// Create row readers and writers for a specific file format.
type RowFormat[T any] interface {
	// Create a RowReader[T] instance.
	Reader(context.Context, io.Reader) (RowReader[T], error)
	// Create a RowWriter[T] instance.
	Writer(context.Context, io.Writer) (RowWriter[T], error)

	// Read all rows into a slice.
	ReadAll(context.Context, io.Reader) ([]T, error)
	// Write all rows from a slice.
	WriteAll(context.Context, io.Writer, []T) error

	// Read all rows as channel.
	ReadChan(context.Context, io.Reader) <-chan Result[T]
	// Write all rows from a channel.
	WriteChan(context.Context, io.Writer, <-chan Result[T]) error
}

// Result[T] type for channel operations.
type Result[T any] struct {
	Result *T
	Err    error
}

// Pipe all rows from one format to another.
func Pipe[T any](
	ctx context.Context,
	r io.Reader,
	in RowFormat[T],
	out RowFormat[T],
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
	out RowFormat[T],
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
