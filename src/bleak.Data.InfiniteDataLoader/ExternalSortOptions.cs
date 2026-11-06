namespace bleak.Data.InfiniteDataLoader;

/// <summary>Tuning for <see cref="ExternalSorter"/>. Every property has a safe default.</summary>
public sealed class ExternalSortOptions
{
    /// <summary>Maximum records per in-memory chunk before it is sorted and spilled. Default 200,000.</summary>
    public int ChunkSize { get; init; } = 200_000;

    /// <summary>
    /// Optional cap on the estimated managed size of one chunk, in bytes. A chunk is spilled when it reaches
    /// either this or <see cref="ChunkSize"/>, so wide records spill sooner. Null (default) means rows only.
    /// With <see cref="BackgroundSpill"/> on, up to two chunks are in memory at once.
    /// </summary>
    public long? MaxChunkBytes { get; init; }

    /// <summary>Folder for the sort's working directory. Null (default) means the system temp folder.</summary>
    public string? TempDirectory { get; init; }

    /// <summary>When true (default), a full chunk is sorted and written on a pool thread while the next chunk
    /// is read, so reading the input overlaps with sorting and disk writes.</summary>
    public bool BackgroundSpill { get; init; } = true;

    /// <summary>Read buffer per run file during the merge, in bytes. Default 64 KB.</summary>
    public int MergeReadBufferSize { get; init; } = 64 * 1024;
}
