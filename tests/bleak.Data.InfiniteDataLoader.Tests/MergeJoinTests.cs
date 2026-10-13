using bleak.Data.InfiniteDataLoader;

namespace bleak.Data.InfiniteDataLoader.Tests;

[TestClass]
public class MergeJoinTests
{
    private static long Key0(string[] r) => long.Parse(r[0]);

    [TestMethod]
    public void OneToMany_groups_children_under_each_parent()
    {
        var ones = new[]
        {
            new[] { "1", "alice" },
            new[] { "2", "bob" },
            new[] { "3", "carol" },
        };
        var many = new[]
        {
            new[] { "1", "B2" },
            new[] { "1", "B1" },
            new[] { "3", "B4" },
        };

        var result = MergeJoin.OneToMany(ones, Key0, many, Key0).ToList();

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual(2, result[0].Many.Count);          // customer 1 has B2, B1
        Assert.AreEqual(0, result[1].Many.Count);          // customer 2 has none
        Assert.AreEqual(1, result[2].Many.Count);          // customer 3 has B4
        Assert.AreEqual("B4", result[2].Many[0][1]);
    }

    [TestMethod]
    public void OneToMany_reports_orphans_with_no_parent()
    {
        var ones = new[] { new[] { "2", "bob" } };
        var many = new[]
        {
            new[] { "1", "orphan-before" },
            new[] { "2", "match" },
            new[] { "3", "orphan-after" },
        };

        var orphans = new List<string[]>();
        var result = MergeJoin.OneToMany(ones, Key0, many, Key0, orphans.Add).ToList();

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(1, result[0].Many.Count);
        Assert.AreEqual(2, orphans.Count);
        Assert.IsTrue(orphans.Any(o => o[1] == "orphan-before"));
        Assert.IsTrue(orphans.Any(o => o[1] == "orphan-after"));
    }

    [TestMethod]
    public void GroupByKey_splits_consecutive_runs()
    {
        var rows = new[]
        {
            new[] { "1", "a" },
            new[] { "1", "b" },
            new[] { "2", "c" },
            new[] { "3", "d" },
            new[] { "3", "e" },
        };

        var groups = MergeJoin.GroupByKey(rows, Key0).ToList();

        Assert.AreEqual(3, groups.Count);
        Assert.AreEqual(2, groups[0].Count);
        Assert.AreEqual(1, groups[1].Count);
        Assert.AreEqual(2, groups[2].Count);
    }
}
