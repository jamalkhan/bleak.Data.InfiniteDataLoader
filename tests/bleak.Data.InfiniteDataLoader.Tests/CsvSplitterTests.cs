using bleak.Data.InfiniteDataLoader;

namespace bleak.Data.InfiniteDataLoader.Tests;

[TestClass]
public class CsvSplitterTests
{
    private string _dir = null!;

    [TestInitialize]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "csvsplitter_" + Guid.NewGuid().ToString("N"));
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

    private string WriteInput(string name, IEnumerable<string> lines)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllLines(path, lines);
        return path;
    }

    [TestMethod]
    public void Splits_data_rows_into_parts_repeating_the_header()
    {
        var input = WriteInput("customer.csv", new[]
        {
            "id,name",
            "1,a", "2,b", "3,c", "4,d", "5,e",
        });
        var outFolder = Path.Combine(_dir, "split");

        var result = CsvSplitter.Split(input, outFolder, rowsPerFile: 2);

        Assert.AreEqual(5, result.Rows);
        Assert.AreEqual(3, result.Files.Count); // 2 + 2 + 1

        // Every part carries the header and at most rowsPerFile data rows.
        CollectionAssert.AreEqual(new[] { "id,name", "1,a", "2,b" }, File.ReadAllLines(result.Files[0]));
        CollectionAssert.AreEqual(new[] { "id,name", "3,c", "4,d" }, File.ReadAllLines(result.Files[1]));
        CollectionAssert.AreEqual(new[] { "id,name", "5,e" }, File.ReadAllLines(result.Files[2]));
    }

    [TestMethod]
    public void Exact_multiple_does_not_emit_an_empty_trailing_part()
    {
        var input = WriteInput("customer.csv", new[] { "id", "1", "2", "3", "4" });
        var outFolder = Path.Combine(_dir, "split");

        var result = CsvSplitter.Split(input, outFolder, rowsPerFile: 2);

        Assert.AreEqual(4, result.Rows);
        Assert.AreEqual(2, result.Files.Count);
    }

    [TestMethod]
    public void Part_file_names_are_stem_based_zero_padded_and_keep_extension()
    {
        var input = WriteInput("customers_001.csv", new[] { "id", "1", "2" });
        var outFolder = Path.Combine(_dir, "split");

        var result = CsvSplitter.Split(input, outFolder, rowsPerFile: 1);

        CollectionAssert.AreEqual(
            new[] { "customers_001_part0001.csv", "customers_001_part0002.csv" },
            result.Files.Select(Path.GetFileName).ToArray());
    }

    [TestMethod]
    public void Preserves_rows_with_embedded_delimiters_and_newlines()
    {
        // Row 2's second field contains a comma and a newline inside quotes — a single logical row.
        var input = WriteInput("q.csv", new[]
        {
            "id,note",
            "1,\"a,b\"",
            "2,\"line1",
            "line2\"",
            "3,plain",
        });
        var outFolder = Path.Combine(_dir, "split");

        var result = CsvSplitter.Split(input, outFolder, rowsPerFile: 10);

        Assert.AreEqual(3, result.Rows); // three logical rows despite the embedded newline
        Assert.AreEqual(1, result.Files.Count);

        // Round-trip the written part back through the parser and confirm the fields survived.
        using var reader = new CsvReader(result.Files[0]);
        var rows = reader.ReadRecords().ToList();
        CollectionAssert.AreEqual(new[] { "id", "note" }, reader.Headers.ToArray());
        CollectionAssert.AreEqual(new[] { "1", "a,b" }, rows[0]);
        CollectionAssert.AreEqual(new[] { "2", "line1\nline2" }, rows[1]);
        CollectionAssert.AreEqual(new[] { "3", "plain" }, rows[2]);
    }

    [TestMethod]
    public void Empty_input_writes_no_parts()
    {
        var input = WriteInput("empty.csv", new[] { "id,name" }); // header only, no data
        var outFolder = Path.Combine(_dir, "split");

        var result = CsvSplitter.Split(input, outFolder, rowsPerFile: 2);

        Assert.AreEqual(0, result.Rows);
        Assert.AreEqual(0, result.Files.Count);
    }

    [TestMethod]
    public void Rejects_non_positive_rows_per_file()
    {
        var input = WriteInput("customer.csv", new[] { "id", "1" });
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => CsvSplitter.Split(input, Path.Combine(_dir, "split"), rowsPerFile: 0));
    }

    [TestMethod]
    public void Rejects_missing_input()
    {
        Assert.ThrowsExactly<FileNotFoundException>(
            () => CsvSplitter.Split(Path.Combine(_dir, "nope.csv"), Path.Combine(_dir, "split"), rowsPerFile: 2));
    }
}
