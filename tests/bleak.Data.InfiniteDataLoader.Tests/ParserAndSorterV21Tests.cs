using System.Text;
using bleak.Data.InfiniteDataLoader;

namespace bleak.Data.InfiniteDataLoader.Tests;

[TestClass]
public class BufferedParserEquivalenceTests
{
    /// <summary>The 2.0 character-at-a-time parser, kept verbatim as the reference grammar.</summary>
    private static IEnumerable<string[]> ReferenceParse(TextReader reader, char delimiter = ',', char quote = '"')
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
                    if (reader.Peek() == quote) { reader.Read(); field.Append(quote); }
                    else { inQuotes = false; }
                }
                else { field.Append(c); }
                continue;
            }
            if (c == quote) { inQuotes = true; recordHasContent = true; }
            else if (c == delimiter) { fields.Add(field.ToString()); field.Clear(); recordHasContent = true; }
            else if (c == '\r' || c == '\n')
            {
                if (c == '\r' && reader.Peek() == '\n') { reader.Read(); }
                if (recordHasContent || field.Length > 0 || fields.Count > 0)
                {
                    fields.Add(field.ToString()); field.Clear();
                    yield return fields.ToArray(); fields.Clear();
                }
                recordHasContent = false;
            }
            else { field.Append(c); recordHasContent = true; }
        }
        if (recordHasContent || field.Length > 0 || fields.Count > 0)
        {
            fields.Add(field.ToString());
            yield return fields.ToArray();
        }
    }

    private static string RandomCsv(Random rng, int length, char delimiter)
    {
        // Heavy on the characters that drive the state machine.
        var alphabet = new[] { 'a', 'b', 'Z', '1', ' ', delimiter, delimiter, '"', '"', '\r', '\n', '\n', 'é', '\uD83D', '\uDE00' };
        var sb = new StringBuilder(length);
        for (var i = 0; i < length; i++)
        {
            sb.Append(alphabet[rng.Next(alphabet.Length)]);
        }
        return sb.ToString();
    }

    [TestMethod]
    public void Matches_the_reference_parser_on_random_input()
    {
        var rng = new Random(12345);
        for (var trial = 0; trial < 400; trial++)
        {
            var delimiter = trial % 3 == 0 ? '\t' : ',';
            // Some inputs are larger than the 64 KB block so records straddle block boundaries.
            var length = trial % 20 == 0 ? 200_000 + rng.Next(5000) : rng.Next(0, 300);
            var text = RandomCsv(rng, length, delimiter);

            var expected = ReferenceParse(new StringReader(text), delimiter).ToList();
            var actual = CsvReader.ParseAll(new StringReader(text), delimiter).ToList();

            Assert.AreEqual(expected.Count, actual.Count, $"trial {trial}: record count");
            for (var i = 0; i < expected.Count; i++)
            {
                CollectionAssert.AreEqual(expected[i], actual[i], $"trial {trial}: record {i}");
            }
        }
    }

    [TestMethod]
    public void Header_and_rows_share_one_buffer()
    {
        // The header is read first; the rows must continue exactly where it stopped.
        var sb = new StringBuilder("id,name\n");
        for (var i = 0; i < 50_000; i++)
        {
            sb.Append(i).Append(",\"n,").Append(i).Append("\"\r\n");
        }
        using var reader = new CsvReader(new StringReader(sb.ToString()));
        CollectionAssert.AreEqual(new[] { "id", "name" }, reader.Headers.ToArray());
        var rows = reader.ReadRecords().ToList();
        Assert.AreEqual(50_000, rows.Count);
        CollectionAssert.AreEqual(new[] { "0", "n,0" }, rows[0]);
        CollectionAssert.AreEqual(new[] { "49999", "n,49999" }, rows[^1]);
    }

    [TestMethod]
    public void Field_spanning_block_boundary_is_intact()
    {
        var big = new string('x', 100_000);
        var text = $"a,b\n\"{big}\"\"q\",z\n";
        var rows = CsvReader.ParseAll(new StringReader(text)).ToList();
        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(big + "\"q", rows[1][0]);
        Assert.AreEqual("z", rows[1][1]);
    }
}

[TestClass]
public class ExternalSorterV21Tests
{
    private static List<string[]> Input(int n, int keyMod)
    {
        var rng = new Random(7);
        var list = new List<string[]>(n);
        for (var i = 0; i < n; i++)
        {
            list.Add(new[] { rng.Next(keyMod).ToString("D5"), i.ToString(), i % 11 == 0 ? "has,comma \"q\"\nnl" : "" });
        }
        return list;
    }

    [TestMethod]
    [DataRow(1_000_000)] // one chunk: in memory
    [DataRow(97)]        // many runs: spilled and merged
    public void Sort_is_stable(int chunkSize)
    {
        var input = Input(5_000, 50);
        var sorted = ExternalSorter.SortByStringKey(input, r => r[0], new ExternalSortOptions { ChunkSize = chunkSize }).ToList();

        // Reference: LINQ OrderBy is a stable sort.
        var expected = input.OrderBy(r => r[0], StringComparer.Ordinal).ToList();
        Assert.AreEqual(expected.Count, sorted.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            CollectionAssert.AreEqual(expected[i], sorted[i], $"row {i}");
        }
    }

    [TestMethod]
    public void Byte_cap_spills_before_the_row_cap_and_background_spill_matches_foreground()
    {
        var input = Input(3_000, 100);
        var a = ExternalSorter.SortByStringKey(input, r => r[0],
            new ExternalSortOptions { ChunkSize = 1_000_000, MaxChunkBytes = 20_000, BackgroundSpill = true }).ToList();
        var b = ExternalSorter.SortByStringKey(input, r => r[0],
            new ExternalSortOptions { ChunkSize = 64, BackgroundSpill = false }).ToList();
        Assert.AreEqual(a.Count, b.Count);
        for (var i = 0; i < a.Count; i++)
        {
            CollectionAssert.AreEqual(b[i], a[i]);
        }
    }

    [TestMethod]
    public void Input_that_fits_in_one_chunk_creates_no_work_directory()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "sorttest_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            var sorted = ExternalSorter.SortByStringKey(Input(100, 10), r => r[0],
                new ExternalSortOptions { TempDirectory = tempRoot }).ToList();
            Assert.AreEqual(100, sorted.Count);
            Assert.AreEqual(0, Directory.GetDirectories(tempRoot).Length);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void Early_stop_deletes_the_work_directory()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "sorttest_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            var first = ExternalSorter.SortByStringKey(Input(1_000, 10), r => r[0],
                new ExternalSortOptions { ChunkSize = 10, TempDirectory = tempRoot }).Take(3).ToList();
            Assert.AreEqual(3, first.Count);
            Assert.AreEqual(0, Directory.GetDirectories(tempRoot).Length);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }

    [TestMethod]
    public void Cleanup_removes_dead_and_old_unowned_directories_only()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "sorttest_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            var dead = Directory.CreateDirectory(Path.Combine(tempRoot, "extsort_dead")).FullName;
            File.WriteAllText(Path.Combine(dead, "owner.pid"), int.MaxValue.ToString()); // no such process
            var live = Directory.CreateDirectory(Path.Combine(tempRoot, "extsort_live")).FullName;
            File.WriteAllText(Path.Combine(live, "owner.pid"), Environment.ProcessId.ToString());
            var oldUnowned = Directory.CreateDirectory(Path.Combine(tempRoot, "extsort_old")).FullName;
            Directory.SetLastWriteTimeUtc(oldUnowned, DateTime.UtcNow.AddDays(-3));
            var newUnowned = Directory.CreateDirectory(Path.Combine(tempRoot, "extsort_new")).FullName;
            var unrelated = Directory.CreateDirectory(Path.Combine(tempRoot, "something_else")).FullName;
            Directory.SetLastWriteTimeUtc(unrelated, DateTime.UtcNow.AddDays(-3));

            var deleted = ExternalSorter.CleanupStaleWorkDirectories(tempRoot);

            Assert.AreEqual(2, deleted);
            Assert.IsFalse(Directory.Exists(dead));
            Assert.IsTrue(Directory.Exists(live));
            Assert.IsFalse(Directory.Exists(oldUnowned));
            Assert.IsTrue(Directory.Exists(newUnowned));
            Assert.IsTrue(Directory.Exists(unrelated));
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }
}
