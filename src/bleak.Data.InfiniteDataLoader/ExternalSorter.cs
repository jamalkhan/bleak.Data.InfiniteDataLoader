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
/// when enumeration completes or the enumerator is disposed. Since 2.1 the sort is stable (equal keys keep
/// their input order), each record's key is computed once, run files are binary (no CSV re-parsing), the
/// next chunk is read while the previous one is sorted and written on a pool thread, and an input that fits
/// in one chunk never touches the disk. See <see cref="ExternalSortOptions"/>.
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
    /// <param name="delimiter">Unused since 2.1: spill files are binary. Kept for source compatibility.</param>
    /// <param name="quote">Unused since 2.1: spill files are binary. Kept for source compatibility.</param>
    public static IEnumerable<string[]> SortByLongKey(
        IEnumerable<string[]> records,
        Func<string[], long> keySelector,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        ILogger? logger = null)
        => SortByKey(records, keySelector, Comparer<long>.Default, new ExternalSortOptions { ChunkSize = chunkSize, TempDirectory = tempDirectory }, logger);

    /// <summary>Sorts <paramref name="records"/> ascending by a string key (ordinal by default). Supports
    /// composite keys (build a single delimited string) and non-numeric keys.</summary>
    /// <param name="delimiter">Unused since 2.1: spill files are binary. Kept for source compatibility.</param>
    /// <param name="quote">Unused since 2.1: spill files are binary. Kept for source compatibility.</param>
    public static IEnumerable<string[]> SortByStringKey(
        IEnumerable<string[]> records,
        Func<string[], string> keySelector,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        IComparer<string>? comparer = null,
        ILogger? logger = null)
        => SortByKey(records, keySelector, comparer ?? StringComparer.Ordinal, new ExternalSortOptions { ChunkSize = chunkSize, TempDirectory = tempDirectory }, logger);

    /// <summary>Sorts <paramref name="records"/> ascending by a string key with explicit <paramref name="options"/>.</summary>
    public static IEnumerable<string[]> SortByStringKey(
        IEnumerable<string[]> records,
        Func<string[], string> keySelector,
        ExternalSortOptions options,
        IComparer<string>? comparer = null,
        ILogger? logger = null)
        => SortByKey(records, keySelector, comparer ?? StringComparer.Ordinal, options, logger);

    /// <summary>
    /// Sorts <paramref name="records"/> ascending by an arbitrary key with the given comparer. The merge
    /// join only requires the same comparer to be used on every side, so any total order works.
    /// </summary>
    /// <param name="delimiter">Unused since 2.1: spill files are binary. Kept for source compatibility.</param>
    /// <param name="quote">Unused since 2.1: spill files are binary. Kept for source compatibility.</param>
    public static IEnumerable<string[]> SortByKey<TKey>(
        IEnumerable<string[]> records,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        ILogger? logger = null)
        => SortByKey(records, keySelector, comparer, new ExternalSortOptions { ChunkSize = chunkSize, TempDirectory = tempDirectory }, logger);

    /// <summary>
    /// Sorts <paramref name="records"/> ascending by an arbitrary key. The sort is stable: records with equal
    /// keys come out in input order. When the whole input fits in one chunk it is sorted in memory and
    /// nothing is written to disk.
    /// </summary>
    public static IEnumerable<string[]> SortByKey<TKey>(
        IEnumerable<string[]> records,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        ExternalSortOptions options,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.ChunkSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ChunkSize must be greater than zero.");
        }

        return SortIterator(records, keySelector, comparer, options, logger ?? NullLogger.Instance);
    }

    private static IEnumerable<string[]> SortIterator<TKey>(
        IEnumerable<string[]> records,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        ExternalSortOptions options,
        ILogger logger)
    {
        var sortStart = Stopwatch.GetTimestamp();
        var workDir = Path.Combine(options.TempDirectory ?? Path.GetTempPath(), WorkDirectoryPrefix + Guid.NewGuid().ToString("N"));
        var chunkSize = options.ChunkSize;
        var maxChunkBytes = options.MaxChunkBytes is > 0 ? options.MaxChunkBytes.Value : long.MaxValue;
        Log.SortStarted(logger, chunkSize);

        long total = 0;
        long merged = 0;
        long bytesSpilled = 0;
        var runs = new List<SpilledRun>();
        var completed = false;
        var failed = false;
        Task<SpilledRun>? pending = null;
        List<string[]>? inMemoryRows = null;
        List<TKey>? inMemoryKeys = null;
        int[]? inMemoryOrder = null;

        try
        {
            // -------- Phase 1: produce sorted runs --------
            // Phase 1 has no yield, so failures can be caught and logged here.
            try
            {
                var phaseStart = Stopwatch.GetTimestamp();
                double sortMs = 0, writeMs = 0;
                var rows = new List<string[]>(Math.Min(chunkSize, 1 << 16));
                var keys = new List<TKey>(Math.Min(chunkSize, 1 << 16));
                long chunkBytes = 0;
                var trackBytes = maxChunkBytes != long.MaxValue;

                void Collect(SpilledRun run)
                {
                    runs.Add(run);
                    bytesSpilled += run.Bytes;
                    sortMs += run.SortMs;
                    writeMs += run.WriteMs;
                }

                void WaitPending()
                {
                    if (pending is not null)
                    {
                        var done = pending;
                        pending = null;
                        Collect(done.GetAwaiter().GetResult());
                    }
                }

                void Spill(bool last)
                {
                    if (runs.Count == 0 && pending is null)
                    {
                        Directory.CreateDirectory(workDir);
                        WriteOwnerFile(workDir);
                        Log.WorkDirectoryCreated(logger, workDir);
                    }
                    var index = runs.Count + (pending is null ? 0 : 1);
                    if (options.BackgroundSpill && !last)
                    {
                        // Sort and write this chunk on a pool thread while the caller's input keeps being read
                        // into a fresh chunk. At most one chunk is in flight, so memory peaks at two chunks.
                        WaitPending();
                        var r = rows;
                        var k = keys;
                        pending = Task.Run(() => WriteSortedRun(r, k, comparer, workDir, index, logger));
                        rows = new List<string[]>(Math.Min(chunkSize, 1 << 16));
                        keys = new List<TKey>(Math.Min(chunkSize, 1 << 16));
                    }
                    else
                    {
                        WaitPending();
                        Collect(WriteSortedRun(rows, keys, comparer, workDir, index, logger));
                        rows.Clear();
                        keys.Clear();
                    }
                    chunkBytes = 0;
                }

                foreach (var rec in records)
                {
                    rows.Add(rec);
                    keys.Add(keySelector(rec));
                    total++;
                    if (trackBytes)
                    {
                        chunkBytes += EstimateBytes(rec);
                    }
                    if (rows.Count >= chunkSize || chunkBytes >= maxChunkBytes)
                    {
                        Spill(last: false);
                    }
                }

                if (runs.Count == 0 && pending is null)
                {
                    // Everything fit in one chunk: sort it in memory and skip the disk entirely.
                    var t = Stopwatch.GetTimestamp();
                    inMemoryOrder = SortedOrder(keys, comparer);
                    inMemoryRows = rows;
                    inMemoryKeys = keys;
                    sortMs = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
                    Log.SortedInMemory(logger, total, sortMs);
                }
                else
                {
                    if (rows.Count > 0)
                    {
                        Spill(last: true);
                    }
                    WaitPending();
                }

                Log.ChunkSortPhaseFinished(logger, ChunkSortPhase, runs.Count, total, bytesSpilled,
                    Stopwatch.GetElapsedTime(phaseStart).TotalMilliseconds, sortMs, writeMs);
            }
            catch (Exception ex)
            {
                failed = true;
                Log.SortFailed(logger, ex, ChunkSortPhase, total, Stopwatch.GetElapsedTime(sortStart).TotalMilliseconds);
                throw;
            }

            // -------- Phase 2: k-way merge (or the in-memory order) --------
            var mergeStart = Stopwatch.GetTimestamp();
            // Measuring the caller's time between records costs two timestamps per row, so only do it
            // when the Debug merge-phase line that reports it will be written.
            var timeConsumer = logger.IsEnabled(LogLevel.Debug);
            long consumerTicks = 0;

            var source = inMemoryOrder is not null
                ? InMemory(inMemoryRows!, inMemoryOrder)
                : Merge(runs, keySelector, comparer, options.MergeReadBufferSize);
            inMemoryKeys = null; // keys are not needed while yielding

            using (var merge = source.GetEnumerator())
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
            Log.MergePhaseFinished(logger, MergePhase, merged, runs.Count, mergeMs, mergeMs - consumerMs, consumerMs);

            var durationMs = Stopwatch.GetElapsedTime(sortStart).TotalMilliseconds;
            Log.SortCompleted(logger, total, runs.Count, bytesSpilled, durationMs, RecordsPerSecond(total, durationMs));
        }
        finally
        {
            if (pending is not null)
            {
                // Phase 1 failed while a chunk was still being written: let it finish before deleting its file.
                try
                {
                    pending.Wait();
                }
                catch (Exception)
                {
                    // Already failing; the original exception is the one that matters.
                }
            }
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

    /// <summary>A rough managed-heap size of one record: the array plus each string.</summary>
    private static long EstimateBytes(string[] record)
    {
        long bytes = 32 + 8L * record.Length;
        foreach (var field in record)
        {
            bytes += 24 + 2L * (field?.Length ?? 0);
        }
        return bytes;
    }

    /// <summary>The stable sort order of <paramref name="keys"/>: ties keep their input order.</summary>
    private static int[] SortedOrder<TKey>(List<TKey> keys, IComparer<TKey> comparer)
    {
        var order = new int[keys.Count];
        for (var i = 0; i < order.Length; i++)
        {
            order[i] = i;
        }
        var keyArray = keys;
        Array.Sort(order, (a, b) =>
        {
            var c = comparer.Compare(keyArray[a], keyArray[b]);
            return c != 0 ? c : a.CompareTo(b);
        });
        return order;
    }

    private static IEnumerable<string[]> InMemory(List<string[]> rows, int[] order)
    {
        for (var i = 0; i < order.Length; i++)
        {
            yield return rows[order[i]];
        }
    }

    private static IEnumerable<string[]> Merge<TKey>(
        List<SpilledRun> runs,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        int readBufferSize)
    {
        var readers = new RunReader[runs.Count];
        try
        {
            // Min-heap over run index, prioritized by the current record's key, then by run index so that equal
            // keys come out in input order (earlier runs hold earlier input).
            var heap = new PriorityQueue<int, (TKey Key, int Run)>(new RunKeyComparer<TKey>(comparer));
            for (var i = 0; i < runs.Count; i++)
            {
                readers[i] = new RunReader(runs[i], readBufferSize);
                if (readers[i].MoveNext())
                {
                    heap.Enqueue(i, (keySelector(readers[i].Current), i));
                }
            }

            while (heap.Count > 0)
            {
                var i = heap.Dequeue();
                var reader = readers[i];
                yield return reader.Current;
                if (reader.MoveNext())
                {
                    heap.Enqueue(i, (keySelector(reader.Current), i));
                }
            }
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader?.Dispose();
            }
        }
    }

    private sealed class RunKeyComparer<TKey>(IComparer<TKey> inner) : IComparer<(TKey Key, int Run)>
    {
        public int Compare((TKey Key, int Run) x, (TKey Key, int Run) y)
        {
            var c = inner.Compare(x.Key, y.Key);
            return c != 0 ? c : x.Run.CompareTo(y.Run);
        }
    }

    /// <summary>Reads one binary run file: per record, a 7-bit field count then each field as a
    /// length-prefixed UTF-8 string (the <see cref="BinaryWriter"/> string format).</summary>
    private sealed class RunReader : IDisposable
    {
        private readonly BinaryReader _reader;
        private long _remaining;

        public RunReader(SpilledRun run, int bufferSize)
        {
            var stream = new FileStream(run.Path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);
            _reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);
            _remaining = run.Rows;
        }

        public string[] Current { get; private set; } = Array.Empty<string>();

        public bool MoveNext()
        {
            if (_remaining <= 0)
            {
                return false;
            }
            _remaining--;
            var count = _reader.Read7BitEncodedInt();
            var record = new string[count];
            for (var i = 0; i < count; i++)
            {
                record[i] = _reader.ReadString();
            }
            Current = record;
            return true;
        }

        public void Dispose() => _reader.Dispose();
    }

    private readonly record struct SpilledRun(string Path, long Rows, long Bytes, double SortMs, double WriteMs);

    private static SpilledRun WriteSortedRun<TKey>(
        List<string[]> rows,
        List<TKey> keys,
        IComparer<TKey> comparer,
        string workDir,
        int index,
        ILogger logger)
    {
        var sortStart = Stopwatch.GetTimestamp();
        var order = SortedOrder(keys, comparer);
        var writeStart = Stopwatch.GetTimestamp();

        var path = Path.Combine(workDir, $"run_{index:D6}.bin");
        long bytes;
        using (var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 1 << 18))
        using (var writer = new BinaryWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), leaveOpen: true))
        {
            foreach (var i in order)
            {
                var rec = rows[i];
                writer.Write7BitEncodedInt(rec.Length);
                foreach (var field in rec)
                {
                    writer.Write(field ?? string.Empty);
                }
            }
            writer.Flush();
            bytes = stream.Position;
        }

        var sortMs = Stopwatch.GetElapsedTime(sortStart, writeStart).TotalMilliseconds;
        var writeMs = Stopwatch.GetElapsedTime(writeStart).TotalMilliseconds;
        Log.ChunkSpilled(logger, index, rows.Count, bytes, sortMs + writeMs, sortMs, writeMs);
        return new SpilledRun(path, rows.Count, bytes, sortMs, writeMs);
    }

    private const string WorkDirectoryPrefix = "extsort_";
    private const string OwnerFileName = "owner.pid";

    private static void WriteOwnerFile(string workDir)
    {
        try
        {
            File.WriteAllText(Path.Combine(workDir, OwnerFileName), Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        catch (IOException)
        {
            // Only used by CleanupStaleWorkDirectories; the sort itself does not need it.
        }
    }

    /// <summary>
    /// Deletes <c>extsort_*</c> working directories left behind in <paramref name="tempDirectory"/> (default: the
    /// system temp folder) by sorts whose process was killed before it could clean up. A directory is removed
    /// when the process recorded in it is no longer running, or, for directories from versions before 2.1 that
    /// record no owner, when it has not been modified for <paramref name="unownedMinimumAge"/> (default 24 hours).
    /// Returns the number of directories deleted.
    /// </summary>
    public static int CleanupStaleWorkDirectories(string? tempDirectory = null, TimeSpan? unownedMinimumAge = null, ILogger? logger = null)
    {
        logger ??= NullLogger.Instance;
        var root = tempDirectory ?? Path.GetTempPath();
        if (!Directory.Exists(root))
        {
            return 0;
        }
        var minimumAge = unownedMinimumAge ?? TimeSpan.FromHours(24);
        var deleted = 0;
        long bytes = 0;
        foreach (var dir in Directory.EnumerateDirectories(root, WorkDirectoryPrefix + "*"))
        {
            try
            {
                if (!IsStale(dir, minimumAge))
                {
                    continue;
                }
                long size = 0;
                foreach (var file in Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories))
                {
                    size += new FileInfo(file).Length;
                }
                Directory.Delete(dir, recursive: true);
                deleted++;
                bytes += size;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                Log.CleanupFailed(logger, ex, dir);
            }
        }
        if (deleted > 0)
        {
            Log.StaleDirectoriesDeleted(logger, deleted, bytes, root);
        }
        return deleted;
    }

    private static bool IsStale(string dir, TimeSpan unownedMinimumAge)
    {
        var ownerFile = Path.Combine(dir, OwnerFileName);
        if (File.Exists(ownerFile)
            && int.TryParse(File.ReadAllText(ownerFile).Trim(), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var pid))
        {
            if (pid == Environment.ProcessId)
            {
                return false;
            }
            try
            {
                using var process = Process.GetProcessById(pid);
                return process.HasExited;
            }
            catch (ArgumentException)
            {
                return true; // no such process
            }
            catch (InvalidOperationException)
            {
                return true;
            }
        }
        return DateTime.UtcNow - Directory.GetLastWriteTimeUtc(dir) >= unownedMinimumAge;
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

        [LoggerMessage(EventId = 1309, Level = LogLevel.Debug,
            Message = "External sort fit in one chunk: {RowCount} rows sorted in memory, nothing spilled, {DurationMs} ms")]
        public static partial void SortedInMemory(ILogger logger, long rowCount, double durationMs);

        [LoggerMessage(EventId = 1310, Level = LogLevel.Information,
            Message = "Deleted {DirectoryCount} stale external sort directories ({ByteCount} bytes) from {FilePath}")]
        public static partial void StaleDirectoriesDeleted(ILogger logger, int directoryCount, long byteCount, string filePath);
    }
}
