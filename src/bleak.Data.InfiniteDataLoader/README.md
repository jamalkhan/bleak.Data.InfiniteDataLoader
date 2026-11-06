# bleak.Data.InfiniteDataLoader

Streaming CSV reader, writer and splitter, external sort, and merge join that work on files larger than RAM.

Forward-only, RFC 4180-aware CSV reading and writing with bounded memory, plus the building blocks for
working with files that do not fit in RAM:

| Type | Purpose |
|---|---|
| `CsvReader` | Forward-only, quoted-field-aware reader. Reads 64 KB blocks and finds field boundaries with a vectorized search. |
| `CsvWriter` | Writer with correct quoting/escaping. |
| `CsvSplitter` | Splits a huge file into parts by row count. |
| `ExternalSorter` | Disk-backed, stable external merge sort on a key. Tune it with `ExternalSortOptions`. |
| `MergeJoin` | Streaming merge join of two key-sorted inputs. |

```csharp
using bleak.Data.InfiniteDataLoader;

using var reader = new CsvReader("customers.csv");          // headers are read up front
int email = reader.IndexOf("email");

foreach (string[] row in reader.ReadRecords())               // one record at a time, bounded memory
{
    Console.WriteLine(CsvWriter.FormatRecord(new[] { row[0], row[email] }));
}
```

## Logging

Every entry point takes an optional logger as its **last** parameter. It defaults to `null`, which means
no logging. The library depends only on `Microsoft.Extensions.Logging.Abstractions`, so any provider works.

```csharp
using Microsoft.Extensions.Logging;

ILoggerFactory loggerFactory = /* from DI, or LoggerFactory.Create(b => b.AddConsole()) */;

var split = CsvSplitter.Split("customers.csv", "out", rowsPerFile: 1_000_000, loggerFactory: loggerFactory);

var sorted = ExternalSorter.SortByLongKey(rows, r => long.Parse(r[0]),
    logger: loggerFactory.CreateLogger(typeof(ExternalSorter)));

var joined = MergeJoin.OneToMany(customers, c => long.Parse(c[0]), orders, o => long.Parse(o[1]),
    onOrphan: orphan => orphanCount++,              // data callback, not logging
    logger: loggerFactory.CreateLogger(typeof(MergeJoin)));

using var reader = new CsvReader("customers.csv", logger: loggerFactory.CreateLogger<CsvReader>());
```

| Category (`bleak.Data.InfiniteDataLoader.*`) | Parameter | Information | Debug | Trace |
|---|---|---|---|---|
| `ExternalSorter` | `ILogger? logger` | sort started; sort completed (rows, chunks, bytes spilled, `DurationMs`, rec/s) | temp directory; chunk-sort phase and merge phase finished, with `DurationMs` | one line per spilled chunk (rows, bytes, sort/write ms) |
| `CsvSplitter` | `ILoggerFactory? loggerFactory` | split started; split completed (files, rows, bytes, `DurationMs`, rec/s) | input/output paths; one line per part file (rows, bytes, `DurationMs`) | - |
| `MergeJoin` | `ILogger? logger` | - | join summary: left rows / matched / without children, right rows / matched / orphans, `DurationMs` | - |
| `CsvReader` | `ILogger<CsvReader>? logger` | - | opened (column count); closed (rows, bytes read, `DurationMs`) | - |

Warnings: `CsvSplitter` input with no data rows; `ExternalSorter` temp directory that could not be deleted.
`ExternalSorter.CleanupStaleWorkDirectories` logs one Information line when it deletes leftovers. An input that fits in
one chunk logs a Debug "sorted in memory" line (event 1309) instead of spilling.
Errors: a sort or split that throws is logged with the exception (and rethrown).

Event IDs: 1000-1099 `CsvReader`, 1200-1299 `CsvSplitter`, 1300-1399 `ExternalSorter`, 1400-1499 `MergeJoin`.
`CsvWriter` is a stateless formatter and does not log.

Notes for performance work:

- Nothing is ever logged per row. Messages use source-generated `[LoggerMessage]` methods, so a disabled level
  costs nothing. `CsvReader` only counts rows while Debug is enabled.
- `ExternalSorter` and `MergeJoin` are lazy, so their `DurationMs` is wall-clock time from the first record
  to the last. It includes producing their input and whatever the caller does between records. The
  `ExternalSorter` merge-phase Debug line splits that into `MergeMs` and `ConsumerMs`, and the chunk-sort
  Debug line reports `SortMs` and `WriteMs` (the rest of that phase is time spent reading the input).

**Data guarantee:** at Debug and above, the library logs only metadata: counts, byte sizes, durations, file
names and paths, and column counts. Field values, keys and record contents are never logged at any level.

## External sort options (2.1)

```csharp
var sorted = ExternalSorter.SortByStringKey(rows, r => r[0], new ExternalSortOptions
{
    ChunkSize = 200_000,               // records per chunk (default)
    MaxChunkBytes = 256L << 20,        // optional: spill sooner when records are wide
    TempDirectory = "/Volumes/scratch", // default: the system temp folder
    BackgroundSpill = true,             // default: sort/write a chunk while the next one is read
    MergeReadBufferSize = 64 * 1024,    // default: read buffer per run file during the merge
});
```

What changed in 2.1, without breaking the 2.0 API:

- The sort is **stable**: records with equal keys come out in input order (2.0 left their order unspecified).
- Each record's key is computed once (2.0 computed it twice per comparison).
- Run files are binary (length-prefixed UTF-8), so the merge no longer re-parses CSV. The `delimiter` and
  `quote` parameters of the 2.0 overloads are accepted and ignored.
- With `BackgroundSpill`, a full chunk is sorted and written on a pool thread while the next chunk is read, so up
  to two chunks are in memory.
- An input that fits in one chunk is sorted in memory and never touches the disk.
- Each work directory records its process id. `ExternalSorter.CleanupStaleWorkDirectories(tempDirectory)` deletes
  `extsort_*` directories whose process is gone (or, for pre-2.1 directories, that are older than 24 hours).
- `CsvReader` parses 64 KB blocks with a vectorized search instead of one character at a time; the grammar is
  unchanged (a test compares it against the 2.0 parser on random input). Its file buffers dropped from 1 MB + 2 MB
  to 256 KB + 128 KB, which matters when dozens of readers are open at once.

## Upgrading from 1.x

2.0.0 replaces the `Action<string>? log` callbacks with `Microsoft.Extensions.Logging`:

| 1.x | 2.0 |
|---|---|
| `CsvSplitter.Split(..., Action<string>? log = null)` | `CsvSplitter.Split(..., ILoggerFactory? loggerFactory = null)` |
| `ExternalSorter.SortByLongKey(..., Action<string>? log = null)` | `ExternalSorter.SortByLongKey(..., ILogger? logger = null)` |
| `ExternalSorter.SortByKey<TKey>(..., Action<string>? log = null)` | `ExternalSorter.SortByKey<TKey>(..., ILogger? logger = null)` |
| `ExternalSorter.SortByStringKey(..., Action<string>? log = null, IComparer<string>? comparer = null)` | `ExternalSorter.SortByStringKey(..., IComparer<string>? comparer = null, ILogger? logger = null)` |

`MergeJoin.OneToMany` and the `CsvReader` constructors gained an optional logger as their last parameter.
`SortByKey`, `SortByLongKey` and `SortByStringKey` now reject `chunkSize <= 0` when called, not on first enumeration.

## Install

```sh
dotnet add package bleak.Data.InfiniteDataLoader
```

Requires .NET 10. The only dependency is `Microsoft.Extensions.Logging.Abstractions` (10.0.x).

## License

[The Unlicense](https://github.com/jamalkhan/bleak.Data.InfiniteDataLoader/blob/master/LICENSE) (public domain).
