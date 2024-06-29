package rowfiles

import (
	"context"
	"io"
)

// Make a RowModel[T] from just the core Reader and Writer methods.
func NewRowModel[T any, R RowReader[T], W RowWriter[T]](core coreRowModel[T, R, W]) RowModel[T] {
	// The R and W generics are only here because go insist on verbatim method signatures.
	// So we pass in the relevant signature parts as generics.
	return coreRowModelWrapper[T, R, W]{core}
}

type coreRowModel[T any, R any, W any] interface {
	Reader(context.Context, io.Reader) (R, error)
	Writer(context.Context, io.Writer) (W, error)
}

type coreRowModelWrapper[T any, R RowReader[T], W RowWriter[T]] struct {
	Core coreRowModel[T, R, W]
}

func (rm coreRowModelWrapper[T, R, W]) Reader(ctx context.Context, r io.Reader) (RowReader[T], error) {
	core, err := rm.Core.Reader(ctx, r)
	if err != nil {
		return nil, err
	}
	return rowReaderWrapper[T]{core, r}, nil
}

type rowReaderWrapper[T any] struct {
	RowReader[T]
	ioReader io.Reader
}

func (r rowReaderWrapper[T]) Close(ctx context.Context, err error) error {
	err = r.RowReader.Close(ctx, err)
	switch t := r.ioReader.(type) {
	case *io.PipeReader:
		t.CloseWithError(err)
	case io.ReadCloser:
		t.Close()
	}
	return err
}

func (rm coreRowModelWrapper[T, R, W]) Writer(ctx context.Context, w io.Writer) (RowWriter[T], error) {
	core, err := rm.Core.Writer(ctx, w)
	if err != nil {
		return nil, err
	}
	return rowWriterWrapper[T]{core, w}, nil
}

type rowWriterWrapper[T any] struct {
	RowWriter[T]
	w io.Writer
}

func (rw rowWriterWrapper[T]) Close(ctx context.Context, err error) error {
	err = rw.RowWriter.Close(ctx, err)
	switch t := rw.w.(type) {
	case *io.PipeWriter:
		if err := t.CloseWithError(err); err != nil {
			return err
		}
	case io.WriteCloser:
		if err := t.Close(); err != nil {
			return err
		}
	}
	return err
}

func (rm coreRowModelWrapper[T, R, W]) ReadAll(ctx context.Context, r io.Reader) ([]T, error) {
	reader, err := rm.Reader(ctx, r)
	if err != nil {
		return nil, err
	}
	rows := []T{}
	for {
		row, err := reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return rows, reader.Close(ctx, err)
		}
		rows = append(rows, row)
	}
	return rows, reader.Close(ctx, nil)
}

func (rm coreRowModelWrapper[T, R, W]) WriteAll(ctx context.Context, w io.Writer, rows []T) error {
	writer, err := rm.Writer(ctx, w)
	if err != nil {
		return err
	}
	for i := range rows {
		err := writer.Write(ctx, rows[i])
		if err != nil {
			return writer.Close(ctx, err)
		}
	}
	return writer.Close(ctx, nil)
}

func (rm coreRowModelWrapper[T, R, W]) ReadChan(ctx context.Context, r io.Reader) (<-chan T, <-chan error) {
	// TODO: send reader.Read result to channels, close, etc.
	return nil, nil
}

func (rm coreRowModelWrapper[T, R, W]) WriteChan(
	ctx context.Context,
	w io.Writer,
	ch <-chan T,
	errch <-chan error,
) error {
	// TODO: receive from both channels and call writer.Write, close, etc.
	return nil
}
