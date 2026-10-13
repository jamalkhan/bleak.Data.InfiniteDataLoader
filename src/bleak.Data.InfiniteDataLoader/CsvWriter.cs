using System.Text;

namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// Minimal RFC 4180 row formatter. Used to round-trip parsed records to/from the temp files that
/// <see cref="ExternalSorter"/> produces, so embedded delimiters, quotes, and newlines survive a sort.
/// </summary>
public static class CsvWriter
{
    /// <summary>Formats one record as a delimited line (no trailing newline), quoting fields only when required.</summary>
    public static string FormatRecord(IReadOnlyList<string> fields, char delimiter = ',', char quote = '"')
    {
        var sb = new StringBuilder();
        AppendRecord(sb, fields, delimiter, quote);
        return sb.ToString();
    }

    /// <summary>Appends one formatted record to <paramref name="sb"/> (no trailing newline).</summary>
    public static void AppendRecord(StringBuilder sb, IReadOnlyList<string> fields, char delimiter = ',', char quote = '"')
    {
        for (var i = 0; i < fields.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(delimiter);
            }
            AppendField(sb, fields[i] ?? string.Empty, delimiter, quote);
        }
    }

    private static void AppendField(StringBuilder sb, string value, char delimiter, char quote)
    {
        var mustQuote = value.IndexOf(delimiter) >= 0
            || value.IndexOf(quote) >= 0
            || value.IndexOf('\r') >= 0
            || value.IndexOf('\n') >= 0;

        if (!mustQuote)
        {
            sb.Append(value);
            return;
        }

        sb.Append(quote);
        foreach (var c in value)
        {
            if (c == quote)
            {
                sb.Append(quote); // escape by doubling
            }
            sb.Append(c);
        }
        sb.Append(quote);
    }
}
