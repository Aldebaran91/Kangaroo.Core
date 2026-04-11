using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for the "direct export" pattern where each item is processed
/// immediately upon being added (no buffering).
///
/// Kangaroo (MaxStoredObjects = 1) is compared against:
///   • TPL Dataflow ActionBlock with sequential execution (MaxDegreeOfParallelism = 1)
///   • A plain delegate call loop (zero-overhead baseline for per-item dispatch cost)
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class DirectExportBenchmarks
{
    [Params(100, 1_000)]
    public int N;

    private string[] _data = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = Enumerable.Range(0, N).Select(i => $"item-{i}").ToArray();
    }

    // -----------------------------------------------------------------
    // Kangaroo – MaxStoredObjects = 1 triggers a synchronous export per item
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo direct export (MaxStoredObjects=1)")]
    public void Kangaroo_DirectExport()
    {
        var store = new KangarooStore<string>(new KangarooSettings(maxStoredObjects: 1));
        store.AddExporter(new NullExportWorker<string>());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);
    }

    // -----------------------------------------------------------------
    // TPL Dataflow – ActionBlock processes each item as it arrives,
    // sequential order preserved (MaxDegreeOfParallelism = 1)
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow ActionBlock (sequential)")]
    public async Task Dataflow_ActionBlock_Sequential()
    {
        var actionBlock = new ActionBlock<string>(
            static _ => { },
            new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

        for (int i = 0; i < N; i++)
            actionBlock.Post(_data[i]);

        actionBlock.Complete();
        await actionBlock.Completion.ConfigureAwait(false);
    }

    // -----------------------------------------------------------------
    // Plain loop – direct delegate dispatch with no queuing infrastructure
    // -----------------------------------------------------------------
    [Benchmark(Description = "Plain delegate loop (no queue)")]
    public void PlainDelegate_Loop()
    {
        Action<string[]> export = static _ => { };
        for (int i = 0; i < N; i++)
            export(new[] { _data[i] });
    }
}
