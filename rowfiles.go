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

type Result[T any] struct {
	Result *T
	Err    error
}

// Convert a (<-chan T, <-chan error) pair to one <-chan Result[T].
func ResultChannel[T any](
	ch <-chan T,
	errch <-chan error,
	err error,
) <-chan Result[T] {
	out := make(chan Result[T])
	go func() {
		if err != nil {
			out <- Result[T]{nil, err}
			close(out)
			return
		}

		defer func() {
			if err := recoverAsError(); err != nil {
				out <- Result[T]{nil, err}
			}
			close(out)
		}()
		for {
			select {
			case data, ok := <-ch:
				if !ok {
					return
				}
				out <- Result[T]{&data, nil}
			case err := <-errch:
				if err != nil {
					out <- Result[T]{nil, err}
					return
				}
			}
		}
	}()
	return out

}
