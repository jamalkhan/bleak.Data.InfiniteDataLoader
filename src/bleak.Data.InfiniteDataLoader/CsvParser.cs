using System.Buffers;

namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// The buffered RFC 4180 state machine behind <see cref="CsvReader"/>. It reads the source in blocks and
/// scans each block with a vectorized search for the next special character (delimiter, quote, CR, LF),
/// copying the plain runs between them in bulk. One instance owns its read buffer, so the header and the
/// data rows of a file must be read through the same instance.
///
/// The accepted grammar is exactly the one of the original character-at-a-time parser: a quote anywhere
/// in a field enters quoted mode, a doubled quote inside quoted mode is a literal quote, CR, LF and CRLF all
/// end a record, and blank lines between records are skipped.
/// </summary>
internal sealed class CsvParser
{
    private const int BlockSize = 64 * 1024;

    private readonly TextReader _reader;
    private readonly char _delimiter;
    private readonly char _quote;
    private readonly SearchValues<char> _specials;
    private readonly char[] _buffer = new char[BlockSize];
    private readonly List<string> _fields = new();
    private char[] _field = new char[256];
    private int _fieldLength;
    private int _position;
    private int _length;
    private bool _eof;

    public CsvParser(TextReader reader, char delimiter, char quote)
    {
        _reader = reader;
        _delimiter = delimiter;
        _quote = quote;
        _specials = SearchValues.Create(new[] { delimiter, quote, '\r', '\n' });
    }

    /// <summary>Reads the next record, or returns null at the end of the input.</summary>
    public string[]? ReadRecord()
    {
        var inQuotes = false;
        var recordHasContent = false;

        while (true)
        {
            if (_position >= _length && !Fill())
            {
                break;
            }

            if (inQuotes)
            {
                var span = _buffer.AsSpan(_position, _length - _position);
                var q = span.IndexOf(_quote);
                if (q < 0)
                {
                    Append(span);
                    _position = _length;
                    continue;
                }
                Append(span[..q]);
                _position += q + 1;
                // A doubled quote is an escaped literal quote.
                if (Peek() == _quote)
                {
                    _position++;
                    Append(_quote);
                }
                else
                {
                    inQuotes = false;
                }
                continue;
            }

            var rest = _buffer.AsSpan(_position, _length - _position);
            var i = rest.IndexOfAny(_specials);
            if (i < 0)
            {
                Append(rest);
                recordHasContent = true;
                _position = _length;
                continue;
            }
            if (i > 0)
            {
                Append(rest[..i]);
                recordHasContent = true;
            }

            var c = rest[i];
            _position += i + 1;
            if (c == _quote)
            {
                inQuotes = true;
                recordHasContent = true;
            }
            else if (c == _delimiter)
            {
                _fields.Add(TakeField());
                recordHasContent = true;
            }
            else
            {
                // CR, LF or CRLF ends the record.
                if (c == '\r' && Peek() == '\n')
                {
                    _position++;
                }
                if (recordHasContent || _fieldLength > 0 || _fields.Count > 0)
                {
                    return TakeRecord();
                }
                // A blank line between records: ignore it.
                recordHasContent = false;
            }
        }

        // A trailing record with no terminating newline.
        if (recordHasContent || _fieldLength > 0 || _fields.Count > 0)
        {
            return TakeRecord();
        }
        return null;
    }

    private string[] TakeRecord()
    {
        _fields.Add(TakeField());
        var record = _fields.ToArray();
        _fields.Clear();
        return record;
    }

    private string TakeField()
    {
        if (_fieldLength == 0)
        {
            return string.Empty;
        }
        var s = new string(_field, 0, _fieldLength);
        _fieldLength = 0;
        return s;
    }

    /// <summary>The next character without consuming it, or -1 at the end of the input.</summary>
    private int Peek()
    {
        if (_position >= _length && !Fill())
        {
            return -1;
        }
        return _buffer[_position];
    }

    private bool Fill()
    {
        if (_eof)
        {
            return false;
        }
        _length = _reader.Read(_buffer, 0, _buffer.Length);
        _position = 0;
        if (_length <= 0)
        {
            _length = 0;
            _eof = true;
            return false;
        }
        return true;
    }

    private void Append(char c)
    {
        if (_fieldLength == _field.Length)
        {
            Array.Resize(ref _field, _field.Length * 2);
        }
        _field[_fieldLength++] = c;
    }

    private void Append(ReadOnlySpan<char> chars)
    {
        if (chars.IsEmpty)
        {
            return;
        }
        var needed = _fieldLength + chars.Length;
        if (needed > _field.Length)
        {
            Array.Resize(ref _field, Math.Max(needed, _field.Length * 2));
        }
        chars.CopyTo(_field.AsSpan(_fieldLength));
        _fieldLength = needed;
    }
}
