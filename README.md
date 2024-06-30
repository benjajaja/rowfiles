# Rowfiles 🚣

Go library for reading files that contain rows, similar to io.Reader and
io.Writer but `T` instead of `byte`.

Some typical formats are CSV, JSONLines, or Parquet.

Because these formats can usually be read progressively, we can avoid reading
the entire contents into memory (a slice), and instead use channels and
goroutines to pipe one row at a time.

```go
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
```

### Defining your own formats

See the examples package, there's a CSV and a JSONLines format included. They
are not included in the base package, because while e.g. CSV is a standard
format, the actual details vary wildy. For example, the header is optional, the
encoding and separator might need to be configured, and more.

The JSONLines format uses simply uses `bufio.Scanner` and
`json.Marshal/Unmarshal`, so no de/serialization is needed.

A format needs only implement `Reader` and `Writer` methods. Extend it to a
full `RowFormat[T]` by writing a constructor like so:

```go
func NewCSVFormat[T any]() RowFormat[T] {
    return rowfiles.NewRowFormat[T](CSVFormat[T]{
        // CSVFormat options
    })
}

// It makes sense to have a singleton that reads specific types in a package.
var myRowCSVFormat = NewCSVFormat[myRow]()
```

### Using a format

See the tests in the examples package for full usage.

#### Read into slice

```go
// Just read all rows into a slice in memory.
file, err := os.Open("rows.csv")
rows, err := myRowCSVFormat.ReadAll(ctx, reader)
```

#### Using channels

```go
file, _ := os.Open("rows.csv")
for result := range myRowCSVFormat.ReadChan(ctx, reader) {
    if result.Err != nil {
        // handle result.Err
    } else {
        // use *result.Result
    }
}
```

```go
ch := make(chan rowfiles.Result[myRow])
var writer io.Writer = uploader("put_a_csv")
err := myRowCSVFormat.WriteChan(ctx, writer, ch)

ch <- rowFiles.Result{Result: &row{...}}
close(ch)
```

#### Upload and download without buffering

```go
var myRowParquetFormat = rowfiles.NewRowFormat[T](ParquetFormat[T]{})

// For example, get a reader that will download a file when read.
var reader io.Reader = download("get_a_csv")

// Pipe CSV rows into an io.Reader that is in parquet format.
result, err := rowfiles.Pipe(ctx, reader, myRowCSVFormat, myRowParquetFormat)

// A function that uploads data incoming into the reader.
upload("put_a_parquet", result)
```

#### Merge several input files of same type but different formats

```go
reader1, _ := csvFormat.Reader(ctx, bytes.NewReader([]byte("<CSV data>")))
reader2, _ := jsonFormat.Reader(ctx, bytes.NewReader([]byte("<JSONLines data>")))

result, _ := rowfiles.Merge(
    ctx,
    csvFormat, // This is the output format
    reader1,
    reader2,
    // ...
)
```

### Error handling, closing, and context

The primitives all return error, and take a context.

The more complex channel / piping / merging parts all use `io.Pipe()` to
propagate errors across `io.Reader`s and `io.Writer`s with `CloseWithError`.

Everyting is closed by "upcasting" to either `*io.Pipe<Reader/Writer>` or
`<Write/Read>Closer`.

Panics from `Row<Reader/Writer>[T]` implementations are recovered in the
channel related goroutines.

ReadChan and WriteChan both check/select context cancellation.
