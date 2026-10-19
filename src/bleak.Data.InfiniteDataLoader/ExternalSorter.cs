using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// External (disk-backed) merge sort for delimited records that are far too large to hold in RAM.
///
/// Phase 1 streams the input into fixed-size buffers, sorts each buffer by key, and spills it to a
/// temp "run" file. Phase 2 does a k-way merge across all runs using a min-heap, yielding records in
/// global key order. Peak memory is roughly one buffer (phase 1) or one record per run plus the heap
/// (phase 2) — independent of total input size.
///
/// The result is a lazily-evaluated stream; temp files are created on first enumeration and deleted
/// when enumeration completes or the enumerator is disposed.
///
/// Logging: pass any <see cref="ILogger"/> (recommended: <c>loggerFactory.CreateLogger(typeof(ExternalSorter))</c>).
/// Information at start/completion, Debug per phase, Trace per spilled chunk. Never per row, never record values.
/// Because the sort is lazy, the completion <c>DurationMs</c> is wall-clock from first enumeration to the
/// last record, so it includes time the caller spends between records. The Debug merge-phase line
/// reports the caller's share separately (<c>ConsumerMs</c>).
/// </summary>
public static partial class ExternalSorter
{
    private const string ChunkSortPhase = "chunk-sort";
    private const string MergePhase = "merge";

    /// <summary>Sorts <paramref name="records"/> ascending by a 64-bit integer key.</summary>
    public static IEnumerable<string[]> SortByLongKey(
        IEnumerable<string[]> records,
        Func<string[], long> keySelector,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        ILogger? logger = null)
        => SortByKey(records, keySelector, Comparer<long>.Default, chunkSize, tempDirectory, delimiter, quote, logger);

    /// <summary>Sorts <paramref name="records"/> ascending by a string key (ordinal by default). Supports
    /// composite keys (build a single delimited string) and non-numeric keys.</summary>
    public static IEnumerable<string[]> SortByStringKey(
        IEnumerable<string[]> records,
        Func<string[], string> keySelector,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        IComparer<string>? comparer = null,
        ILogger? logger = null)
        => SortByKey(records, keySelector, comparer ?? StringComparer.Ordinal, chunkSize, tempDirectory, delimiter, quote, logger);

    /// <summary>
    /// Sorts <paramref name="records"/> ascending by an arbitrary key with the given comparer. The merge
    /// join only requires the same comparer to be used on every side, so any total order works.
    /// </summary>
    public static IEnumerable<string[]> SortByKey<TKey>(
        IEnumerable<string[]> records,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        ILogger? logger = null)
    {
        if (chunkSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "chunkSize must be greater than zero.");
        }

        return SortIterator(records, keySelector, comparer, chunkSize, tempDirectory, delimiter, quote, logger ?? NullLogger.Instance);
    }

    private static IEnumerable<string[]> SortIterator<TKey>(
        IEnumerable<string[]> records,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        int chunkSize,
        string? tempDirectory,
        char delimiter,
        char quote,
        ILogger logger)
    {
        var sortStart = Stopwatch.GetTimestamp();
        var workDir = Path.Combine(tempDirectory ?? Path.GetTempPath(), "extsort_" + Guid.NewGuid().ToString("N"));
        Log.SortStarted(logger, chunkSize);

        long total = 0;
        long merged = 0;
        long bytesSpilled = 0;
        var runFiles = new List<string>();
        var completed = false;
        var failed = false;

        try
        {
            // -------- Phase 1: produce sorted runs --------
            // Phase 1 has no yield, so failures can be caught and logged here.
            try
            {
                Directory.CreateDirectory(workDir);
                Log.WorkDirectoryCreated(logger, workDir);

                var phaseStart = Stopwatch.GetTimestamp();
                double sortMs = 0, writeMs = 0;
                var buffer = new List<string[]>(Math.Min(chunkSize, 1 << 16));

                void Spill()
                {
                    var run = WriteSortedRun(buffer, keySelector, comparer, workDir, runFiles.Count, delimiter, quote, logger);
                    runFiles.Add(run.Path);
                    bytesSpilled += run.Bytes;
                    sortMs += run.SortMs;
                    writeMs += run.WriteMs;
                    buffer.Clear();
                }

                foreach (var rec in records)
                {
                    buffer.Add(rec);
                    total++;
                    if (buffer.Count >= chunkSize)
                    {
                        Spill();
                    }
                }
                if (buffer.Count > 0)
                {
                    Spill();
                }

                Log.ChunkSortPhaseFinished(logger, ChunkSortPhase, runFiles.Count, total, bytesSpilled,
                    Stopwatch.GetElapsedTime(phaseStart).TotalMilliseconds, sortMs, writeMs);
            }
            catch (Exception ex)
            {
                failed = true;
                Log.SortFailed(logger, ex, ChunkSortPhase, total, Stopwatch.GetElapsedTime(sortStart).TotalMilliseconds);
                throw;
            }

            // -------- Phase 2: k-way merge --------
            var mergeStart = Stopwatch.GetTimestamp();
            // Measuring the caller's time between records costs two timestamps per row, so only do it
            // when the Debug merge-phase line that reports it will be written.
            var timeConsumer = logger.IsEnabled(LogLevel.Debug);
            long consumerTicks = 0;

            using (var merge = Merge(runFiles, keySelector, comparer, delimiter, quote).GetEnumerator())
            {
                while (true)
                {
                    bool has;
                    try
                    {
                        has = merge.MoveNext();
                    }
                    catch (Exception ex)
                    {
                        failed = true;
                        Log.SortFailed(logger, ex, MergePhase, merged, Stopwatch.GetElapsedTime(sortStart).TotalMilliseconds);
                        throw;
                    }
                    if (!has)
                    {
                        break;
                    }

                    merged++;
                    if (timeConsumer)
                    {
                        var yieldedAt = Stopwatch.GetTimestamp();
                        yield return merge.Current;
                        consumerTicks += Stopwatch.GetTimestamp() - yieldedAt;
                    }
                    else
                    {
                        yield return merge.Current;
                    }
                }
            }

            completed = true;
            var mergeMs = Stopwatch.GetElapsedTime(mergeStart).TotalMilliseconds;
            var consumerMs = Stopwatch.GetElapsedTime(0, consumerTicks).TotalMilliseconds;
            Log.MergePhaseFinished(logger, MergePhase, merged, runFiles.Count, mergeMs, mergeMs - consumerMs, consumerMs);

            var durationMs = Stopwatch.GetElapsedTime(sortStart).TotalMilliseconds;
            Log.SortCompleted(logger, total, runFiles.Count, bytesSpilled, durationMs, RecordsPerSecond(total, durationMs));
        }
        finally
        {
            if (!completed && !failed)
            {
                // The caller stopped enumerating (e.g. Take(n)) before the merge finished.
                Log.SortStoppedEarly(logger, merged, total, Stopwatch.GetElapsedTime(sortStart).TotalMilliseconds);
            }
            TryDeleteDirectory(workDir, logger);
        }
    }

    internal static double RecordsPerSecond(long rows, double durationMs)
        => durationMs > 0 ? Math.Round(rows / (durationMs / 1000.0), 1) : 0;

    private static IEnumerable<string[]> Merge<TKey>(
        List<string> runFiles,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        char delimiter,
        char quote)
    {
        var readers = new List<TextReader>(runFiles.Count);
        var cursors = new IEnumerator<string[]>[runFiles.Count];
        try
        {
            // Min-heap over run index, prioritized by the current record's key.
            var heap = new PriorityQueue<int, TKey>(comparer);
            for (var i = 0; i < runFiles.Count; i++)
            {
                var reader = new StreamReader(runFiles[i], Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1 << 20);
                readers.Add(reader);
                var cursor = CsvReader.ParseAll(reader, delimiter, quote).GetEnumerator();
                cursors[i] = cursor;
                if (cursor.MoveNext())
                {
                    heap.Enqueue(i, keySelector(cursor.Current));
                }
            }

            while (heap.Count > 0)
            {
                var i = heap.Dequeue();
                yield return cursors[i].Current;
                if (cursors[i].MoveNext())
                {
                    heap.Enqueue(i, keySelector(cursors[i].Current));
                }
            }
        }
        finally
        {
            foreach (var cursor in cursors)
            {
                cursor?.Dispose();
            }
            foreach (var reader in readers)
            {
                reader.Dispose();
            }
        }
    }

    private readonly record struct SpilledRun(string Path, long Bytes, double SortMs, double WriteMs);

    private static SpilledRun WriteSortedRun<TKey>(
        List<string[]> buffer,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        string workDir,
        int index,
        char delimiter,
        char quote,
        ILogger logger)
    {
        var sortStart = Stopwatch.GetTimestamp();
        buffer.Sort((a, b) => comparer.Compare(keySelector(a), keySelector(b)));
        var writeStart = Stopwatch.GetTimestamp();

        var path = Path.Combine(workDir, $"run_{index:D6}.csv");
        long bytes;
        using (var writer = new StreamWriter(path, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 20)
        {
            NewLine = "\n",
        })
        {
            var sb = new StringBuilder();
            foreach (var rec in buffer)
            {
                sb.Clear();
                CsvWriter.AppendRecord(sb, rec, delimiter, quote);
                writer.WriteLine(sb.ToString());
            }
            writer.Flush();
            bytes = writer.BaseStream.Position;
        }

        var sortMs = Stopwatch.GetElapsedTime(sortStart, writeStart).TotalMilliseconds;
        var writeMs = Stopwatch.GetElapsedTime(writeStart).TotalMilliseconds;
        Log.ChunkSpilled(logger, index, buffer.Count, bytes, sortMs + writeMs, sortMs, writeMs);
        return new SpilledRun(path, bytes, sortMs, writeMs);
    }

    private static void TryDeleteDirectory(string dir, ILogger logger)
    {
        try
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
        catch (Exception ex)
        {
            // Best-effort cleanup; temp files will be reclaimed by the OS eventually.
            Log.CleanupFailed(logger, ex, dir);
        }
    }

    // EventIds 1300-1399.
    private static partial class Log
    {
        [LoggerMessage(EventId = 1300, Level = LogLevel.Information,
            Message = "External sort started: chunk size {BatchSize} rows")]
        public static partial void SortStarted(ILogger logger, int batchSize);

        [LoggerMessage(EventId = 1301, Level = LogLevel.Information,
            Message = "External sort completed: {RowCount} rows, {ChunkCount} chunks, {ByteCount} bytes spilled, {DurationMs} ms, {RecordsPerSecond} rec/s")]
        public static partial void SortCompleted(ILogger logger, long rowCount, int chunkCount, long byteCount, double durationMs, double recordsPerSecond);

        [LoggerMessage(EventId = 1302, Level = LogLevel.Debug,
            Message = "External sort working directory {FilePath}")]
        public static partial void WorkDirectoryCreated(ILogger logger, string filePath);

        [LoggerMessage(EventId = 1303, Level = LogLevel.Debug,
            Message = "External sort {Phase} phase finished: {ChunkCount} chunks, {RowCount} rows, {ByteCount} bytes spilled, {DurationMs} ms (sort {SortMs} ms, spill write {WriteMs} ms, remainder is reading input)")]
        public static partial void ChunkSortPhaseFinished(ILogger logger, string phase, int chunkCount, long rowCount, long byteCount, double durationMs, double sortMs, double writeMs);

        [LoggerMessage(EventId = 1304, Level = LogLevel.Debug,
            Message = "External sort {Phase} phase finished: {RowCount} rows from {ChunkCount} chunks, {DurationMs} ms (merge {MergeMs} ms, consumer {ConsumerMs} ms)")]
        public static partial void MergePhaseFinished(ILogger logger, string phase, long rowCount, int chunkCount, double durationMs, double mergeMs, double consumerMs);

        [LoggerMessage(EventId = 1305, Level = LogLevel.Trace,
            Message = "External sort chunk {ChunkIndex} spilled: {RowCount} rows, {ByteCount} bytes, {DurationMs} ms (sort {SortMs} ms, write {WriteMs} ms)")]
        public static partial void ChunkSpilled(ILogger logger, int chunkIndex, int rowCount, long byteCount, double durationMs, double sortMs, double writeMs);

        [LoggerMessage(EventId = 1306, Level = LogLevel.Debug,
            Message = "External sort enumeration stopped early: {RowCount} of {TotalRowCount} rows returned, {DurationMs} ms")]
        public static partial void SortStoppedEarly(ILogger logger, long rowCount, long totalRowCount, double durationMs);

        [LoggerMessage(EventId = 1307, Level = LogLevel.Error,
            Message = "External sort failed in {Phase} phase after {RowCount} rows, {DurationMs} ms")]
        public static partial void SortFailed(ILogger logger, Exception exception, string phase, long rowCount, double durationMs);

        [LoggerMessage(EventId = 1308, Level = LogLevel.Warning,
            Message = "External sort could not delete temp directory {FilePath}")]
        public static partial void CleanupFailed(ILogger logger, Exception exception, string filePath);
    }
}
