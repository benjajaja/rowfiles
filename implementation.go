package rowfiles

import (
	"context"
	"fmt"
	"io"
)

// Make a RowFormat[T] from just the core Reader and Writer methods.
func NewRowFormat[T any, R RowReader[T], W RowWriter[T]](core coreRowFormat[T, R, W]) RowFormat[T] {
	// The R and W generics are only here because go insist on verbatim method signatures.
	// So we pass in the relevant signature parts as generics.
	return coreRowFormatWrapper[T, R, W]{core}
}

type coreRowFormat[T any, R any, W any] interface {
	Reader(context.Context, io.Reader) (R, error)
	Writer(context.Context, io.Writer) (W, error)
}

type coreRowFormatWrapper[T any, R RowReader[T], W RowWriter[T]] struct {
	Core coreRowFormat[T, R, W]
}

func (rm coreRowFormatWrapper[T, R, W]) Reader(ctx context.Context, r io.Reader) (RowReader[T], error) {
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

func (rm coreRowFormatWrapper[T, R, W]) Writer(ctx context.Context, w io.Writer) (RowWriter[T], error) {
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

func (rm coreRowFormatWrapper[T, R, W]) ReadAll(ctx context.Context, r io.Reader) ([]T, error) {
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

func (rm coreRowFormatWrapper[T, R, W]) WriteAll(ctx context.Context, w io.Writer, rows []T) error {
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

func (rm coreRowFormatWrapper[T, R, W]) ReadChan(ctx context.Context, r io.Reader) <-chan Result[T] {
	ch := make(chan Result[T])
	go func() {
		defer func() {
			// The reader is not closed on panics, as it might have been closed already.
			if err := recoverAsError(); err != nil {
				ch <- Result[T]{nil, err}
			}
			close(ch)
		}()

		reader, err := rm.Reader(ctx, r)
		if err != nil {
			ch <- Result[T]{nil, err}
			return
		}

		for {
			row, err := reader.Read(ctx)
			if err != nil {
				if err == io.EOF {
					reader.Close(ctx, nil)
					return
				}
				ch <- Result[T]{nil, err}
				reader.Close(ctx, err)
				return
			}
			ch <- Result[T]{&row, nil}
		}
	}()

	return ch
}

func (rm coreRowFormatWrapper[T, R, W]) WriteChan(
	ctx context.Context,
	w io.Writer,
	ch <-chan Result[T],
) error {
	writer, err := rm.Writer(ctx, w)
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			if err := recoverAsError(); err != nil {
				_ = writer.Close(ctx, nil)
			}
		}()
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					_ = writer.Close(ctx, nil)
					return
				}
				if result.Result != nil {
					if err := writer.Write(ctx, *result.Result); err != nil {
						_ = writer.Close(ctx, err)
						return
					}
				} else {
					_ = writer.Close(ctx, result.Err)
					return
				}
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					_ = writer.Close(ctx, err)
					return
				}
			}
		}
	}()
	return nil
}

func recoverAsError() error {
	var err error
	if r := recover(); r != nil {
		if t, ok := r.(error); ok {
			err = t
		} else {
			err = fmt.Errorf("recovered panic: %v", r)
		}
	}
	return err
}
