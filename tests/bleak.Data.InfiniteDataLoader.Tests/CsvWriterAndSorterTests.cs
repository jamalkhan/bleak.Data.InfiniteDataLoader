using bleak.Data.InfiniteDataLoader;

namespace bleak.Data.InfiniteDataLoader.Tests;

[TestClass]
public class CsvWriterTests
{
    [TestMethod]
    public void Leaves_plain_fields_unquoted()
    {
        Assert.AreEqual("a,b,c", CsvWriter.FormatRecord(new[] { "a", "b", "c" }));
    }

    [TestMethod]
    public void Quotes_only_fields_needing_it_and_round_trips()
    {
        var original = new[] { "plain", "has,comma", "has\"quote", "has\nnewline" };
        var line = CsvWriter.FormatRecord(original);

        // Re-parse the formatted line and confirm we get the original fields back.
        var parsed = CsvReader.ParseAll(new StringReader(line)).Single();
        CollectionAssert.AreEqual(original, parsed);
    }
}

[TestClass]
public class ExternalSorterTests
{
    private static long KeyAt0(string[] r) => long.Parse(r[0]);

    [TestMethod]
    public void Sorts_by_key_across_multiple_runs()
    {
        // chunkSize=2 forces several runs + a k-way merge.
        var input = new List<string[]>
        {
            new[] { "5", "e" },
            new[] { "3", "c" },
            new[] { "9", "i" },
            new[] { "1", "a" },
            new[] { "7", "g" },
            new[] { "2", "b" },
            new[] { "8", "h" },
        };

        var sorted = ExternalSorter.SortByLongKey(input, KeyAt0, chunkSize: 2).ToList();

        Assert.AreEqual(input.Count, sorted.Count);
        CollectionAssert.AreEqual(new[] { 1L, 2, 3, 5, 7, 8, 9 }, sorted.Select(KeyAt0).ToArray());
    }

    [TestMethod]
    public void Preserves_field_content_with_embedded_delimiters_through_temp_files()
    {
        var input = new List<string[]>
        {
            new[] { "2", "has,comma" },
            new[] { "1", "has\"quote and\nnewline" },
        };

        var sorted = ExternalSorter.SortByLongKey(input, KeyAt0, chunkSize: 1).ToList();

        Assert.AreEqual("1", sorted[0][0]);
        Assert.AreEqual("has\"quote and\nnewline", sorted[0][1]);
        Assert.AreEqual("has,comma", sorted[1][1]);
    }

    [TestMethod]
    public void Cleans_up_its_temp_directory_after_enumeration()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "sorttest_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);
        try
        {
            var input = new List<string[]> { new[] { "2", "b" }, new[] { "1", "a" } };
            _ = ExternalSorter.SortByLongKey(input, KeyAt0, chunkSize: 1, tempDirectory: tempRoot).ToList();

            // No leftover extsort_* working directories.
            Assert.AreEqual(0, Directory.GetDirectories(tempRoot, "extsort_*").Length);
        }
        finally
        {
            Directory.Delete(tempRoot, recursive: true);
        }
    }
}
