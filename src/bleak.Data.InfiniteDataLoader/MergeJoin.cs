using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// Streaming join/group helpers over key-sorted record sequences. They assume their inputs are
/// already ascending by key (e.g. the output of <see cref="ExternalSorter"/>) and hold only a single
/// key-group in memory at a time, so they scale to arbitrarily large inputs.
///
/// Logging: <see cref="OneToMany"/> takes an optional <see cref="ILogger"/> (recommended:
/// <c>loggerFactory.CreateLogger(typeof(MergeJoin))</c>) and writes one Debug summary per join. Keys and
/// record values are never logged; orphan rows go to the <c>onOrphan</c> data callback, not to the log.
/// </summary>
public static partial class MergeJoin
{
    /// <summary>
    /// One-to-many merge join: for each record on the "one" side, yields it together with all "many"
    /// records sharing its key. "Many" records whose key matches no "one" record (orphans) are passed
    /// to <paramref name="onOrphan"/> and otherwise dropped. Assumes both inputs are key-ascending and
    /// that "one" keys are unique.
    /// </summary>
    public static IEnumerable<(string[] One, List<string[]> Many)> OneToMany(
        IEnumerable<string[]> sortedOne,
        Func<string[], long> oneKey,
        IEnumerable<string[]> sortedMany,
        Func<string[], long> manyKey,
        Action<string[]>? onOrphan = null,
        ILogger? logger = null)
    {
        logger ??= NullLogger.Instance;
        var start = Stopwatch.GetTimestamp();
        long leftRows = 0, leftMatched = 0, rightRows = 0, rightOrphans = 0;
        var completed = false;

        try
        {
            using var ones = sortedOne.GetEnumerator();
            using var many = sortedMany.GetEnumerator();
            var manyHas = many.MoveNext();

            while (ones.MoveNext())
            {
                leftRows++;
                var key = oneKey(ones.Current);

                // Skip any "many" rows ordered before this key — they have no parent.
                while (manyHas && manyKey(many.Current) < key)
                {
                    rightRows++;
                    rightOrphans++;
                    onOrphan?.Invoke(many.Current);
                    manyHas = many.MoveNext();
                }

                var group = new List<string[]>();
                while (manyHas && manyKey(many.Current) == key)
                {
                    rightRows++;
                    group.Add(many.Current);
                    manyHas = many.MoveNext();
                }
                if (group.Count > 0)
                {
                    leftMatched++;
                }

                yield return (ones.Current, group);
            }

            // Any "many" rows left over after the last "one" row are orphans too.
            while (manyHas)
            {
                rightRows++;
                rightOrphans++;
                onOrphan?.Invoke(many.Current);
                manyHas = many.MoveNext();
            }
            completed = true;
        }
        finally
        {
            // Duration is wall-clock over the whole enumeration, so it includes producing the (lazy)
            // sorted inputs and the caller's own per-group work.
            var durationMs = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            if (completed)
            {
                Log.JoinFinished(logger, leftRows, leftMatched, leftRows - leftMatched, rightRows, rightRows - rightOrphans, rightOrphans, durationMs);
            }
            else
            {
                Log.JoinStoppedEarly(logger, leftRows, leftMatched, rightRows, rightOrphans, durationMs);
            }
        }
    }

    /// <summary>
    /// Groups a key-ascending sequence into consecutive runs sharing the same key. Each yielded list is
    /// one group. Assumes the input is sorted by <paramref name="key"/>.
    /// </summary>
    public static IEnumerable<List<string[]>> GroupByKey(
        IEnumerable<string[]> sorted,
        Func<string[], long> key)
    {
        using var e = sorted.GetEnumerator();
        var has = e.MoveNext();
        while (has)
        {
            var k = key(e.Current);
            var group = new List<string[]> { e.Current };
            has = e.MoveNext();
            while (has && key(e.Current) == k)
            {
                group.Add(e.Current);
                has = e.MoveNext();
            }
            yield return group;
        }
    }

    // EventIds 1400-1499.
    private static partial class Log
    {
        [LoggerMessage(EventId = 1400, Level = LogLevel.Debug,
            Message = "Merge join finished: {LeftRowCount} left rows ({MatchedCount} matched, {LeftOrphanCount} without children), {RightRowCount} right rows ({RightMatchedCount} matched, {RightOrphanCount} orphans), {DurationMs} ms")]
        public static partial void JoinFinished(ILogger logger, long leftRowCount, long matchedCount, long leftOrphanCount, long rightRowCount, long rightMatchedCount, long rightOrphanCount, double durationMs);

        [LoggerMessage(EventId = 1401, Level = LogLevel.Debug,
            Message = "Merge join stopped before the end of input: {LeftRowCount} left rows ({MatchedCount} matched), {RightRowCount} right rows ({RightOrphanCount} orphans), {DurationMs} ms")]
        public static partial void JoinStoppedEarly(ILogger logger, long leftRowCount, long matchedCount, long rightRowCount, long rightOrphanCount, double durationMs);
    }
}
