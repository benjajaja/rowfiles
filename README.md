# Rowfiles 🚣

Go library for reading files that contain rows, similar to io.Reader and io.Writer but `T`
instead of `byte`.

Some typical formats are CSV, JSONLines, or Parquet.

```go
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
	// TODO: ReadChan(context.Context, io.Reader) (<-chan T, <-chan error)
	// Write all rows in channel
	// TODO: WriteChan(context.Context, io.Writer, <-chan T, <-chan error) error
}
```
