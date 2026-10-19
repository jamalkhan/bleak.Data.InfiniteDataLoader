using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
///
/// Logging: pass an <see cref="ILoggerFactory"/>. The splitter logs under category
/// <c>bleak.Data.InfiniteDataLoader.CsvSplitter</c> (Information at start/completion, Debug per part file)
/// and hands <c>ILogger&lt;CsvReader&gt;</c> to the reader it opens.
/// </summary>
public static partial class CsvSplitter
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
        ILoggerFactory? loggerFactory = null)
    {
        if (rowsPerFile < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(rowsPerFile), rowsPerFile, "rowsPerFile must be at least 1.");
        }
        if (!File.Exists(inputPath))
        {
            throw new FileNotFoundException($"Input file not found: {inputPath}", inputPath);
        }
        loggerFactory ??= NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger(typeof(CsvSplitter));

        var start = Stopwatch.GetTimestamp();
        var inputName = Path.GetFileName(inputPath);
        var inputBytes = new FileInfo(inputPath).Length;
        Log.SplitStarted(logger, inputName, inputBytes, rowsPerFile);
        Log.SplitTarget(logger, inputPath, outputFolder);

        var files = new List<string>();
        var totalRows = 0;
        long bytesWritten = 0;

        StreamWriter? writer = null;
        string? partPath = null;
        long partStart = 0;
        var rowsInPart = 0;
        var partNumber = 0;

        void ClosePart()
        {
            if (writer is null)
            {
                return;
            }
            writer.Flush();
            var bytes = writer.BaseStream.Position;
            writer.Dispose();
            writer = null;
            bytesWritten += bytes;
            Log.PartWritten(logger, partPath!, partNumber, rowsInPart, bytes, Stopwatch.GetElapsedTime(partStart).TotalMilliseconds);
        }

        try
        {
            Directory.CreateDirectory(outputFolder);

            var stem = Path.GetFileNameWithoutExtension(inputPath);
            var ext = Path.GetExtension(inputPath); // includes the leading '.', or "" if none

            using var reader = new CsvReader(inputPath, delimiter, quote, loggerFactory.CreateLogger<CsvReader>());
            var header = includeHeader ? reader.Headers : Array.Empty<string>();
            var writeHeader = header.Count > 0;
            var sb = new StringBuilder(4096);

            try
            {
                foreach (var record in reader.ReadRecords())
                {
                    // Start a fresh part at the first row and whenever the current part is full.
                    if (writer is null || rowsInPart == rowsPerFile)
                    {
                        ClosePart();
                        partNumber++;
                        partPath = Path.Combine(outputFolder, $"{stem}_part{partNumber:D4}{ext}");
                        partStart = Stopwatch.GetTimestamp();
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
                    }

                    sb.Clear();
                    CsvWriter.AppendRecord(sb, record, delimiter, quote);
                    writer.Write(sb);
                    writer.Write('\n');
                    rowsInPart++;
                    totalRows++;
                }
                ClosePart();
            }
            finally
            {
                // Only reached with an open writer when an exception is propagating.
                writer?.Dispose();
            }
        }
        catch (Exception ex)
        {
            Log.SplitFailed(logger, ex, inputName, files.Count, totalRows, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            throw;
        }

        if (totalRows == 0)
        {
            Log.NoDataRows(logger, inputName);
        }

        var durationMs = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        Log.SplitCompleted(logger, inputName, files.Count, totalRows, inputBytes, bytesWritten, durationMs,
            ExternalSorter.RecordsPerSecond(totalRows, durationMs));

        return new SplitResult(totalRows, files);
    }

    // UTF-8 without a BOM, large buffer — matches typical CSV and keeps syscalls down on big files.
    private static StreamWriter OpenPart(string path)
    {
        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 20);
        return new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), 1 << 20);
    }

    // EventIds 1200-1299.
    private static partial class Log
    {
        [LoggerMessage(EventId = 1200, Level = LogLevel.Information,
            Message = "CSV split started: {FileName} ({ByteCount} bytes), {BatchSize} rows per file")]
        public static partial void SplitStarted(ILogger logger, string fileName, long byteCount, int batchSize);

        [LoggerMessage(EventId = 1201, Level = LogLevel.Information,
            Message = "CSV split completed: {FileName}: {FileCount} files, {RowCount} rows, {ByteCount} bytes read, {OutputByteCount} bytes written, {DurationMs} ms, {RecordsPerSecond} rec/s")]
        public static partial void SplitCompleted(ILogger logger, string fileName, int fileCount, long rowCount, long byteCount, long outputByteCount, double durationMs, double recordsPerSecond);

        [LoggerMessage(EventId = 1202, Level = LogLevel.Debug,
            Message = "CSV split input {FilePath}, output folder {OutputFolder}")]
        public static partial void SplitTarget(ILogger logger, string filePath, string outputFolder);

        [LoggerMessage(EventId = 1203, Level = LogLevel.Debug,
            Message = "CSV split part {PartNumber} written: {FilePath}, {RowCount} rows, {ByteCount} bytes, {DurationMs} ms")]
        public static partial void PartWritten(ILogger logger, string filePath, int partNumber, int rowCount, long byteCount, double durationMs);

        [LoggerMessage(EventId = 1204, Level = LogLevel.Warning,
            Message = "CSV split: {FileName} has no data rows, nothing written")]
        public static partial void NoDataRows(ILogger logger, string fileName);

        [LoggerMessage(EventId = 1205, Level = LogLevel.Error,
            Message = "CSV split failed: {FileName} after {FileCount} files, {RowCount} rows, {DurationMs} ms")]
        public static partial void SplitFailed(ILogger logger, Exception exception, string fileName, int fileCount, long rowCount, double durationMs);
    }
}
