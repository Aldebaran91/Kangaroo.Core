using Kangaroo.Core;

namespace Kangaroo.Benchmarks;

/// <summary>
/// A no-op export worker used to eliminate export I/O overhead from benchmarks.
/// </summary>
internal sealed class NullExportWorker<T> : IKangarooExportWorker<T>
{
    public void Export(T[] input) { }
}
