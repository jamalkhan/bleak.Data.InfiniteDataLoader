namespace bleak.Data.InfiniteDataLoader;

/// <summary>
/// Streaming join/group helpers over key-sorted record sequences. They assume their inputs are
/// already ascending by key (e.g. the output of <see cref="ExternalSorter"/>) and hold only a single
/// key-group in memory at a time, so they scale to arbitrarily large inputs.
/// </summary>
public static class MergeJoin
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
        Action<string[]>? onOrphan = null)
    {
        using var ones = sortedOne.GetEnumerator();
        using var many = sortedMany.GetEnumerator();
        var manyHas = many.MoveNext();

        while (ones.MoveNext())
        {
            var key = oneKey(ones.Current);

            // Skip any "many" rows ordered before this key — they have no parent.
            while (manyHas && manyKey(many.Current) < key)
            {
                onOrphan?.Invoke(many.Current);
                manyHas = many.MoveNext();
            }

            var group = new List<string[]>();
            while (manyHas && manyKey(many.Current) == key)
            {
                group.Add(many.Current);
                manyHas = many.MoveNext();
            }

            yield return (ones.Current, group);
        }

        // Any "many" rows left over after the last "one" row are orphans too.
        while (manyHas)
        {
            onOrphan?.Invoke(many.Current);
            manyHas = many.MoveNext();
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
}
