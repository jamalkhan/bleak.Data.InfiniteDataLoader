using System.Text;

namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// A forward-only, fully streaming reader for delimited files. It never holds more than a single
/// record in memory, so it scales to arbitrarily large inputs (500MB+, billions of rows).
///
/// This is a modernized, allocation-conscious replacement for the old
/// <c>bleak.InfiniteData.LazyDataTable</c> (which buffered the entire file into a char map and
/// targeted netstandard2.0). It is RFC 4180 aware: it honors a configurable text qualifier,
/// supports delimiters and line breaks embedded inside qualified fields, and treats a doubled
/// qualifier ("") as an escaped qualifier.
///
/// The underlying reader is forward-only, so <see cref="ReadRecords"/> may be enumerated once.
/// </summary>
public sealed class CsvReader : IDisposable
{
    private readonly TextReader _reader;
    private readonly bool _ownsReader;
    private readonly char _delimiter;
    private readonly char _quote;
    private readonly Dictionary<string, int> _headerIndex;

    /// <summary>The header row, in file order. Empty when the source has no rows.</summary>
    public IReadOnlyList<string> Headers { get; }

    /// <summary>Opens a CSV file. Encoding is auto-detected from the byte-order mark (defaults to UTF-8).</summary>
    public CsvReader(string path, char delimiter = ',', char quote = '"')
        : this(OpenFile(path), delimiter, quote, ownsReader: true)
    {
    }

    /// <summary>Reads from an arbitrary <see cref="TextReader"/>. Primarily for testing.</summary>
    public CsvReader(TextReader reader, char delimiter = ',', char quote = '"', bool ownsReader = false)
    {
        _reader = reader;
        _ownsReader = ownsReader;
        _delimiter = delimiter;
        _quote = quote;

        // Consume exactly the header record, leaving the reader positioned at the first data row.
        using var records = ParseAll(_reader, _delimiter, _quote).GetEnumerator();
        if (records.MoveNext())
        {
            Headers = records.Current;
        }
        else
        {
            Headers = Array.Empty<string>();
        }

        _headerIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < Headers.Count; i++)
        {
            _headerIndex[Headers[i]] = i;
        }
    }

    private static StreamReader OpenFile(string path)
    {
        // Large 1MB buffers keep syscalls down on big files; detectEncodingFromByteOrderMarks handles the BOM.
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1 << 20, FileOptions.SequentialScan);
        return new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1 << 20);
    }

    /// <summary>Returns the column index for a header name (case-insensitive), or -1 if absent.</summary>
    public int IndexOf(string column) => _headerIndex.TryGetValue(column, out var i) ? i : -1;

    /// <summary>
    /// Lazily yields each data record (header excluded) as a string array, reading the source in a
    /// single forward pass. Each yielded array is freshly allocated, so callers may retain it.
    /// </summary>
    public IEnumerable<string[]> ReadRecords() => ParseAll(_reader, _delimiter, _quote);

    /// <summary>
    /// The core state-machine parser. Reads characters one at a time from the (internally buffered)
    /// reader and emits one <see cref="string"/>[] per logical record from the current position.
    /// Treats every record equally (no header handling) — used directly for header-less temp files.
    /// </summary>
    public static IEnumerable<string[]> ParseAll(TextReader reader, char delimiter = ',', char quote = '"')
    {
        var fields = new List<string>();
        var field = new StringBuilder();
        var inQuotes = false;
        var recordHasContent = false;

        int read;
        while ((read = reader.Read()) != -1)
        {
            var c = (char)read;

            if (inQuotes)
            {
                if (c == quote)
                {
                    // A doubled quote is an escaped literal quote.
                    if (reader.Peek() == quote)
                    {
                        reader.Read();
                        field.Append(quote);
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    field.Append(c);
                }
                continue;
            }

            if (c == quote)
            {
                inQuotes = true;
                recordHasContent = true;
            }
            else if (c == delimiter)
            {
                fields.Add(field.ToString());
                field.Clear();
                recordHasContent = true;
            }
            else if (c == '\r' || c == '\n')
            {
                // Normalize CRLF: swallow the LF that follows a CR.
                if (c == '\r' && reader.Peek() == '\n')
                {
                    reader.Read();
                }

                if (recordHasContent || field.Length > 0 || fields.Count > 0)
                {
                    fields.Add(field.ToString());
                    field.Clear();
                    yield return fields.ToArray();
                    fields.Clear();
                }
                // else: a blank line between records — ignore it.
                recordHasContent = false;
            }
            else
            {
                field.Append(c);
                recordHasContent = true;
            }
        }

        // Flush a trailing record that has no terminating newline.
        if (recordHasContent || field.Length > 0 || fields.Count > 0)
        {
            fields.Add(field.ToString());
            yield return fields.ToArray();
        }
    }

    public void Dispose()
    {
        if (_ownsReader)
        {
            _reader.Dispose();
        }
    }
}
