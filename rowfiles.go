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
	ReadChan(context.Context, io.Reader) (<-chan T, <-chan error)
	// Write all rows in channel
	WriteChan(context.Context, io.Writer, <-chan T, <-chan error) error
}

func PipeReader[T any](
	ctx context.Context,
	model RowModel[T],
	fn func(*io.PipeWriter),
) (<-chan T, <-chan error, error) {
	r, w := io.Pipe()
	ch, errch := model.ReadChan(ctx, r)
	go func() {
		defer recoverPipeWriter(w)
		fn(w)
	}()
	return ch, errch, nil
}

func PipeWriter[T any](
	ctx context.Context,
	model RowModel[T],
	fn func(chan<- T, chan<- error),
) (io.Reader, error) {
	ch, errch := make(chan T), make(chan error)
	r, w := io.Pipe()
	err := model.WriteChan(ctx, w, ch, errch)
	if err != nil {
		return nil, err
	}
	go func() {
		defer recoverPipeWriter(w)
		fn(ch, errch)
	}()
	return r, nil
}

type pipeWriterReader interface {
	*io.PipeReader | *io.PipeWriter
	CloseWithError(error) error
}

func recoverPipeWriter[T pipeWriterReader](rw T) {
	err := recoverAsError()
	if err != nil {
		rw.CloseWithError(err)
	}
}
