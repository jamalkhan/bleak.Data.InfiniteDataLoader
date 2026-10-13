using System.Text;
using bleak.Data.InfiniteDataLoader;

namespace bleak.Data.InfiniteDataLoader.Tests;

[TestClass]
public class CsvReaderTests
{
    private static List<string[]> ReadAll(string content, out IReadOnlyList<string> headers)
    {
        using var reader = new CsvReader(new StringReader(content));
        headers = reader.Headers;
        return reader.ReadRecords().ToList();
    }

    [TestMethod]
    public void Parses_headers_and_simple_rows()
    {
        var rows = ReadAll("a,b,c\n1,2,3\n4,5,6\n", out var headers);

        CollectionAssert.AreEqual(new[] { "a", "b", "c" }, headers.ToArray());
        Assert.AreEqual(2, rows.Count);
        CollectionAssert.AreEqual(new[] { "1", "2", "3" }, rows[0]);
        CollectionAssert.AreEqual(new[] { "4", "5", "6" }, rows[1]);
    }

    [TestMethod]
    public void Handles_trailing_row_without_newline()
    {
        var rows = ReadAll("a,b\n1,2", out _);
        Assert.AreEqual(1, rows.Count);
        CollectionAssert.AreEqual(new[] { "1", "2" }, rows[0]);
    }

    [TestMethod]
    public void Preserves_empty_fields_as_empty_strings()
    {
        var rows = ReadAll("a,b,c\n1,,3\n", out _);
        CollectionAssert.AreEqual(new[] { "1", "", "3" }, rows[0]);
    }

    [TestMethod]
    public void Quoted_field_may_contain_delimiter()
    {
        var rows = ReadAll("a,b\n\"x,y\",z\n", out _);
        CollectionAssert.AreEqual(new[] { "x,y", "z" }, rows[0]);
    }

    [TestMethod]
    public void Quoted_field_may_contain_newline()
    {
        var rows = ReadAll("a,b\n\"line1\nline2\",z\n", out _);
        CollectionAssert.AreEqual(new[] { "line1\nline2", "z" }, rows[0]);
    }

    [TestMethod]
    public void Doubled_quote_is_an_escaped_quote()
    {
        var rows = ReadAll("a\n\"she said \"\"hi\"\"\"\n", out _);
        CollectionAssert.AreEqual(new[] { "she said \"hi\"" }, rows[0]);
    }

    [TestMethod]
    public void Handles_crlf_line_endings()
    {
        var rows = ReadAll("a,b\r\n1,2\r\n3,4\r\n", out var headers);
        CollectionAssert.AreEqual(new[] { "a", "b" }, headers.ToArray());
        Assert.AreEqual(2, rows.Count);
        CollectionAssert.AreEqual(new[] { "3", "4" }, rows[1]);
    }

    [TestMethod]
    public void Skips_blank_lines_between_records()
    {
        var rows = ReadAll("a,b\n1,2\n\n3,4\n", out _);
        Assert.AreEqual(2, rows.Count);
    }

    [TestMethod]
    public void IndexOf_is_case_insensitive_and_minus_one_when_absent()
    {
        using var reader = new CsvReader(new StringReader("Customer_Key,Brand\n1,B2\n"));
        Assert.AreEqual(0, reader.IndexOf("customer_key"));
        Assert.AreEqual(1, reader.IndexOf("BRAND"));
        Assert.AreEqual(-1, reader.IndexOf("missing"));
    }

    [TestMethod]
    public void Detects_and_strips_utf8_bom_from_a_file()
    {
        var path = Path.GetTempFileName();
        try
        {
            var bytes = new List<byte>();
            bytes.AddRange(Encoding.UTF8.GetPreamble()); // UTF-8 BOM
            bytes.AddRange(Encoding.UTF8.GetBytes("customer_key,brand\n1001,B2\n"));
            File.WriteAllBytes(path, bytes.ToArray());

            using var reader = new CsvReader(path);
            Assert.AreEqual("customer_key", reader.Headers[0]); // BOM not glued to first header
            Assert.AreEqual(0, reader.IndexOf("customer_key"));
            var rows = reader.ReadRecords().ToList();
            CollectionAssert.AreEqual(new[] { "1001", "B2" }, rows[0]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [TestMethod]
    public void Empty_source_yields_empty_headers_and_no_rows()
    {
        var rows = ReadAll("", out var headers);
        Assert.AreEqual(0, headers.Count);
        Assert.AreEqual(0, rows.Count);
    }
}
