using Kangaroo.Core;

namespace Kangaroo.Benchmarks;

/// <summary>
/// A no-op converter that returns the string representation of the input.
/// Used to isolate pipeline overhead from actual conversion cost.
/// </summary>
internal sealed class NullConverter<T> : IKangarooConverter<T, string>
{
    public string Convert(T data) => data!.ToString()!;
}
