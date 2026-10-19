using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace bleak.Data.InfiniteDataLoader.Tests;

/// <summary>One captured log call: level, event, rendered message and structured properties.</summary>
public sealed record CapturedLog(
    string Category,
    LogLevel Level,
    EventId EventId,
    string Message,
    IReadOnlyDictionary<string, object?> Properties,
    Exception? Exception);

/// <summary>Test-only ILoggerFactory that records every log call at every level.</summary>
public sealed class CapturingLoggerFactory : ILoggerFactory
{
    private readonly ConcurrentQueue<CapturedLog> _entries = new();

    public IReadOnlyList<CapturedLog> Entries => _entries.ToArray();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, _entries);

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    /// <summary>Messages and property values at Debug or above, for checking that no record values leak.</summary>
    public IEnumerable<string> TextAtDebugOrAbove()
        => Entries
            .Where(e => e.Level >= LogLevel.Debug)
            .SelectMany(e => e.Properties.Values.Select(v => v?.ToString() ?? string.Empty).Prepend(e.Message));

    private sealed class CapturingLogger(string category, ConcurrentQueue<CapturedLog> sink) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var properties = new Dictionary<string, object?>();
            if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                foreach (var (key, value) in pairs)
                {
                    properties[key] = value;
                }
            }
            sink.Enqueue(new CapturedLog(category, logLevel, eventId, formatter(state, exception), properties, exception));
        }
    }
}
