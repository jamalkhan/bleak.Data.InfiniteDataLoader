using System.Text;

namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// Splits one delimited file into a series of smaller files, each holding at most a fixed number of
/// data rows. The input is streamed one record at a time via <see cref="CsvReader"/>, so it scales to
/// arbitrarily large inputs (multi-GB) without buffering; output is written record-by-record via
/// <see cref="CsvWriter"/>, so embedded delimiters, quotes, and newlines round-trip correctly and
/// row boundaries are logical (not raw text-line) boundaries.
///
/// The header row is repeated at the top of every part file (when present), so each part is a valid,
/// self-describing CSV. The original is never modified or deleted.
/// </summary>
public static class CsvSplitter
{
    /// <summary>Outcome of a split: how many data rows were read and the part files written, in order.</summary>
    public sealed record SplitResult(int Rows, IReadOnlyList<string> Files);

    /// <summary>
    /// Splits <paramref name="inputPath"/> into <paramref name="outputFolder"/>, at most
    /// <paramref name="rowsPerFile"/> data rows per part file. Part files are named
    /// <c>{stem}_part{NNNN}{ext}</c> (e.g. <c>customers_001_part0001.csv</c>).
    /// </summary>
    public static SplitResult Split(
        string inputPath,
        string outputFolder,
        int rowsPerFile,
        char delimiter = ',',
        char quote = '"',
        bool includeHeader = true,
        Action<string>? log = null)
    {
        if (rowsPerFile < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(rowsPerFile), rowsPerFile, "rowsPerFile must be at least 1.");
        }
        if (!File.Exists(inputPath))
        {
            throw new FileNotFoundException($"Input file not found: {inputPath}", inputPath);
        }
        log ??= _ => { };

        Directory.CreateDirectory(outputFolder);

        var stem = Path.GetFileNameWithoutExtension(inputPath);
        var ext = Path.GetExtension(inputPath); // includes the leading '.', or "" if none

        using var reader = new CsvReader(inputPath, delimiter, quote);
        var header = includeHeader ? reader.Headers : Array.Empty<string>();
        var writeHeader = header.Count > 0;

        var files = new List<string>();
        var totalRows = 0;

        StreamWriter? writer = null;
        var rowsInPart = 0;
        var partNumber = 0;
        var sb = new StringBuilder(4096);

        try
        {
            foreach (var record in reader.ReadRecords())
            {
                // Start a fresh part at the first row and whenever the current part is full.
                if (writer is null || rowsInPart == rowsPerFile)
                {
                    writer?.Dispose();
                    partNumber++;
                    var partPath = Path.Combine(outputFolder, $"{stem}_part{partNumber:D4}{ext}");
                    writer = OpenPart(partPath);
                    files.Add(partPath);
                    rowsInPart = 0;

                    if (writeHeader)
                    {
                        sb.Clear();
                        CsvWriter.AppendRecord(sb, header, delimiter, quote);
                        writer.Write(sb);
                        writer.Write('\n');
                    }
                    log($"[split] writing {Path.GetFileName(partPath)}");
                }

                sb.Clear();
                CsvWriter.AppendRecord(sb, record, delimiter, quote);
                writer.Write(sb);
                writer.Write('\n');
                rowsInPart++;
                totalRows++;
            }
        }
        finally
        {
            writer?.Dispose();
        }

        if (totalRows == 0)
        {
            log($"[split] {Path.GetFileName(inputPath)} has no data rows — nothing written.");
        }

        return new SplitResult(totalRows, files);
    }

    // UTF-8 without a BOM, large buffer — matches typical CSV and keeps syscalls down on big files.
    private static StreamWriter OpenPart(string path)
    {
        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 20);
        return new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), 1 << 20);
    }
}
