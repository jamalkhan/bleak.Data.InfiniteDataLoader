using System.Text;

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
/// </summary>
public static class ExternalSorter
{
    /// <summary>Sorts <paramref name="records"/> ascending by a 64-bit integer key.</summary>
    public static IEnumerable<string[]> SortByLongKey(
        IEnumerable<string[]> records,
        Func<string[], long> keySelector,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        Action<string>? log = null)
        => SortByKey(records, keySelector, Comparer<long>.Default, chunkSize, tempDirectory, delimiter, quote, log);

    /// <summary>Sorts <paramref name="records"/> ascending by a string key (ordinal by default). Supports
    /// composite keys (build a single delimited string) and non-numeric keys.</summary>
    public static IEnumerable<string[]> SortByStringKey(
        IEnumerable<string[]> records,
        Func<string[], string> keySelector,
        int chunkSize = 200_000,
        string? tempDirectory = null,
        char delimiter = ',',
        char quote = '"',
        Action<string>? log = null,
        IComparer<string>? comparer = null)
        => SortByKey(records, keySelector, comparer ?? StringComparer.Ordinal, chunkSize, tempDirectory, delimiter, quote, log);

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
        Action<string>? log = null)
    {
        if (chunkSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "chunkSize must be greater than zero.");
        }

        var workDir = Path.Combine(tempDirectory ?? Path.GetTempPath(), "extsort_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workDir);

        try
        {
            // -------- Phase 1: produce sorted runs --------
            var runFiles = new List<string>();
            var buffer = new List<string[]>(Math.Min(chunkSize, 1 << 16));
            long total = 0;
            foreach (var rec in records)
            {
                buffer.Add(rec);
                total++;
                if (buffer.Count >= chunkSize)
                {
                    runFiles.Add(WriteSortedRun(buffer, keySelector, comparer, workDir, runFiles.Count, delimiter, quote));
                    buffer.Clear();
                }
            }
            if (buffer.Count > 0)
            {
                runFiles.Add(WriteSortedRun(buffer, keySelector, comparer, workDir, runFiles.Count, delimiter, quote));
            }
            buffer.Clear();
            log?.Invoke($"sorted {total:N0} records into {runFiles.Count} run(s)");

            // -------- Phase 2: k-way merge --------
            foreach (var record in Merge(runFiles, keySelector, comparer, delimiter, quote))
            {
                yield return record;
            }
        }
        finally
        {
            TryDeleteDirectory(workDir);
        }
    }

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

    private static string WriteSortedRun<TKey>(
        List<string[]> buffer,
        Func<string[], TKey> keySelector,
        IComparer<TKey> comparer,
        string workDir,
        int index,
        char delimiter,
        char quote)
    {
        buffer.Sort((a, b) => comparer.Compare(keySelector(a), keySelector(b)));

        var path = Path.Combine(workDir, $"run_{index:D6}.csv");
        using var writer = new StreamWriter(path, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 1 << 20)
        {
            NewLine = "\n",
        };

        var sb = new StringBuilder();
        foreach (var rec in buffer)
        {
            sb.Clear();
            CsvWriter.AppendRecord(sb, rec, delimiter, quote);
            writer.WriteLine(sb.ToString());
        }
        return path;
    }

    private static void TryDeleteDirectory(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
        catch
        {
            // Best-effort cleanup; temp files will be reclaimed by the OS eventually.
        }
    }
}
