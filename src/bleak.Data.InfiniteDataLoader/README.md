# bleak.Data.InfiniteDataLoader

Streaming CSV reader, writer and splitter, external sort, and merge join that work on files larger than RAM.

Forward-only, RFC 4180-aware CSV reading and writing with bounded memory, plus the building blocks for
working with files that do not fit in RAM:

| Type | Purpose |
|---|---|
| `CsvReader` | Forward-only, quoted-field-aware reader. |
| `CsvWriter` | Writer with correct quoting/escaping. |
| `CsvSplitter` | Splits a huge file into parts by row count. |
| `ExternalSorter` | Disk-backed external merge sort on a key column. |
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

## Install

```sh
dotnet add package bleak.Data.InfiniteDataLoader
```

## License

[The Unlicense](LICENSE) (public domain).
