using bleak.Data.InfiniteDataLoader;
using Microsoft.Extensions.Logging;

namespace bleak.Data.InfiniteDataLoader.Tests;

[TestClass]
public class LoggingTests
{
    // A recognisable record value that must never appear in any log line at Debug or above.
    private const string SecretValue = "jane.doe@example.com";

    private string _dir = null!;

    [TestInitialize]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "logging_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    private static long Key0(string[] r) => long.Parse(r[0]);

    private static void AssertNoRecordValues(CapturingLoggerFactory factory)
    {
        foreach (var text in factory.TextAtDebugOrAbove())
        {
            Assert.IsFalse(text.Contains(SecretValue, StringComparison.Ordinal), $"Record value leaked into log: {text}");
        }
    }

    private static CapturedLog Single(CapturingLoggerFactory factory, int eventId)
    {
        var matches = factory.Entries.Where(e => e.EventId.Id == eventId).ToList();
        Assert.HasCount(1, matches, $"Expected exactly one event {eventId}.");
        return matches[0];
    }

    private static void AssertDuration(CapturedLog entry)
    {
        Assert.IsTrue(entry.Properties.TryGetValue("DurationMs", out var d), "DurationMs missing");
        Assert.IsInstanceOfType<double>(d);
        Assert.IsGreaterThanOrEqualTo(0.0, (double)d!);
    }

    [TestMethod]
    public void ExternalSorter_logs_completion_with_duration_and_counters_but_no_values()
    {
        using var factory = new CapturingLoggerFactory();
        var input = Enumerable.Range(0, 7).Reverse().Select(i => new[] { i.ToString(), SecretValue }).ToList();

        var sorted = ExternalSorter.SortByLongKey(input, Key0, chunkSize: 3, logger: factory.CreateLogger(typeof(ExternalSorter))).ToList();
        Assert.HasCount(7, sorted);

        Single(factory, 1300); // started
        var completed = Single(factory, 1301);
        Assert.AreEqual(LogLevel.Information, completed.Level);
        AssertDuration(completed);
        Assert.AreEqual(7L, completed.Properties["RowCount"]);
        Assert.AreEqual(3, completed.Properties["ChunkCount"]);
        Assert.IsGreaterThan(0L, (long)completed.Properties["ByteCount"]!);

        // Debug: one line per phase, each with DurationMs. Trace: one line per spilled chunk.
        AssertDuration(Single(factory, 1303));
        AssertDuration(Single(factory, 1304));
        Assert.HasCount(3, factory.Entries.Where(e => e.EventId.Id == 1305 && e.Level == LogLevel.Trace).ToList());

        AssertNoRecordValues(factory);
    }

    [TestMethod]
    public void ExternalSorter_logs_early_stop_instead_of_completion()
    {
        using var factory = new CapturingLoggerFactory();
        var input = Enumerable.Range(0, 5).Select(i => new[] { i.ToString() }).ToList();

        _ = ExternalSorter.SortByLongKey(input, Key0, chunkSize: 2, logger: factory.CreateLogger(typeof(ExternalSorter))).Take(2).ToList();

        Assert.IsFalse(factory.Entries.Any(e => e.EventId.Id == 1301));
        Single(factory, 1306);
    }

    [TestMethod]
    public void CsvSplitter_logs_completion_and_one_debug_line_per_part_but_no_values()
    {
        using var factory = new CapturingLoggerFactory();
        var input = Path.Combine(_dir, "people.csv");
        File.WriteAllLines(input, new[] { "id,email" }.Concat(Enumerable.Range(1, 5).Select(i => $"{i},{SecretValue}")));

        var result = CsvSplitter.Split(input, Path.Combine(_dir, "out"), rowsPerFile: 2, loggerFactory: factory);
        Assert.HasCount(3, result.Files);

        var completed = Single(factory, 1201);
        Assert.AreEqual(LogLevel.Information, completed.Level);
        AssertDuration(completed);
        Assert.AreEqual(3, completed.Properties["FileCount"]);
        Assert.AreEqual(5L, completed.Properties["RowCount"]);
        Assert.AreEqual("people.csv", completed.Properties["FileName"]);

        var parts = factory.Entries.Where(e => e.EventId.Id == 1203).ToList();
        Assert.HasCount(3, parts);
        Assert.IsTrue(parts.All(p => p.Level == LogLevel.Debug));
        CollectionAssert.AreEqual(new[] { 2, 2, 1 }, parts.Select(p => (int)p.Properties["RowCount"]!).ToArray());

        // The reader the splitter opens logs under its own category, with totals on dispose.
        var readerClosed = Single(factory, 1001);
        Assert.AreEqual(typeof(CsvReader).FullName, readerClosed.Category);
        Assert.AreEqual(5L, readerClosed.Properties["RowCount"]);
        Assert.IsGreaterThan(0L, (long)readerClosed.Properties["ByteCount"]!);

        AssertNoRecordValues(factory);
    }

    [TestMethod]
    public void MergeJoin_logs_debug_summary_and_still_reports_orphans_to_callback()
    {
        using var factory = new CapturingLoggerFactory();
        var ones = new[] { new[] { "1", SecretValue }, new[] { "2", SecretValue }, new[] { "4", SecretValue } };
        var many = new[] { new[] { "0", SecretValue }, new[] { "1", SecretValue }, new[] { "1", SecretValue }, new[] { "3", SecretValue }, new[] { "9", SecretValue } };
        var orphans = new List<string[]>();

        var result = MergeJoin.OneToMany(ones, Key0, many, Key0, orphans.Add, factory.CreateLogger(typeof(MergeJoin))).ToList();

        Assert.HasCount(3, result);
        Assert.HasCount(3, orphans); // 0, 3, 9

        var summary = Single(factory, 1400);
        Assert.AreEqual(LogLevel.Debug, summary.Level);
        AssertDuration(summary);
        Assert.AreEqual(3L, summary.Properties["LeftRowCount"]);
        Assert.AreEqual(1L, summary.Properties["MatchedCount"]);
        Assert.AreEqual(2L, summary.Properties["LeftOrphanCount"]);
        Assert.AreEqual(5L, summary.Properties["RightRowCount"]);
        Assert.AreEqual(3L, summary.Properties["RightOrphanCount"]);

        AssertNoRecordValues(factory);
    }

    [TestMethod]
    public void CsvReader_logs_nothing_per_row()
    {
        using var factory = new CapturingLoggerFactory();
        var input = Path.Combine(_dir, "rows.csv");
        File.WriteAllLines(input, new[] { "id,email" }.Concat(Enumerable.Range(1, 100).Select(i => $"{i},{SecretValue}")));

        using (var reader = new CsvReader(input, logger: factory.CreateLogger<CsvReader>()))
        {
            Assert.AreEqual(100, reader.ReadRecords().Count());
        }

        // Exactly open + close, regardless of row count.
        CollectionAssert.AreEqual(new[] { 1000, 1001 }, factory.Entries.Select(e => e.EventId.Id).ToArray());
        AssertDuration(factory.Entries[1]);
        AssertNoRecordValues(factory);
    }
}
