# bleak.Data.InfiniteDataLoader

[![NuGet](https://img.shields.io/nuget/v/bleak.Data.InfiniteDataLoader.svg)](https://www.nuget.org/packages/bleak.Data.InfiniteDataLoader)

Streaming CSV reader, writer and splitter, external sort, and merge join that work on files larger than RAM.

Usage, the logging reference and upgrade notes are in the package README:
[src/bleak.Data.InfiniteDataLoader/README.md](src/bleak.Data.InfiniteDataLoader/README.md).

## Install

```sh
dotnet add package bleak.Data.InfiniteDataLoader
```

## Requirements

- .NET 10
- `Microsoft.Extensions.Logging.Abstractions` 10.0.x (pulled in automatically)

## Build and test

```sh
dotnet build
dotnet test
```

## Releasing

CI (`.github/workflows/ci.yml`) builds and tests every push and pull request. Publishing to nuget.org runs only
when a `v*` tag is pushed. The package version comes from the tag (`v2.1.0` → `2.1.0`), so keep `<Version>` in the
csproj in step with it:

```sh
git tag -a v2.1.0 -m "v2.1.0" && git push origin v2.1.0
```

## License

[The Unlicense](LICENSE) (public domain).
