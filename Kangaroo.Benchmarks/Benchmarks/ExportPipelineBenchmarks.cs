using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing full pipeline performance: enqueue N items then drain /
/// export them all at once.
///
/// Kangaroo (manual export) is compared against:
///   • TPL Dataflow  – BatchBlock{N} linked to an ActionBlock that receives the batch
///   • Manual drain  – ConcurrentQueue filled then drained with a while-loop export
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class ExportPipelineBenchmarks
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
    // Kangaroo – add N items then trigger a manual (batch) export
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo batch export")]
    public void Kangaroo_BatchExport()
    {
        var store = new KangarooStore<string>();
        store.AddExporter(new NullExportWorker<string>());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);

        store.StartManualExport();
    }

    // -----------------------------------------------------------------
    // Kangaroo – size-triggered export (store flushes automatically at N)
    // -----------------------------------------------------------------
    [Benchmark(Description = "Kangaroo size-triggered export")]
    public void Kangaroo_SizeTriggeredExport()
    {
        var store = new KangarooStore<string>(new KangarooSettings(maxStoredObjects: (uint)N));
        store.AddExporter(new NullExportWorker<string>());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);
    }

    // -----------------------------------------------------------------
    // TPL Dataflow – BatchBlock<N> → ActionBlock, post N items then complete
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow batch (BatchBlock→ActionBlock)")]
    public async Task Dataflow_BatchExport()
    {
        var batchBlock = new BatchBlock<string>(N);
        var actionBlock = new ActionBlock<string[]>(static _ => { });
        batchBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

        for (int i = 0; i < N; i++)
            batchBlock.Post(_data[i]);

        batchBlock.Complete();
        await actionBlock.Completion.ConfigureAwait(false);
    }

    // -----------------------------------------------------------------
    // Manual – ConcurrentQueue + drain loop (no abstraction overhead)
    // -----------------------------------------------------------------
    [Benchmark(Description = "ConcurrentQueue manual drain")]
    public void ConcurrentQueue_ManualDrain()
    {
        var queue = new ConcurrentQueue<string>();

        for (int i = 0; i < N; i++)
            queue.Enqueue(_data[i]);

        // Drain and "process"
        var buffer = new List<string>(N);
        while (queue.TryDequeue(out var item))
            buffer.Add(item);

        // Simulate export call
        _ = buffer.ToArray();
    }
}
