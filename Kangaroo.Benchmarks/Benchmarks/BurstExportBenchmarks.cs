using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks measuring bursty workload patterns where data arrives in waves
/// and is periodically flushed. Exercises Kangaroo's size-triggered export mode
/// with multiple automatic flushes during a single run.
///
/// Compared against:
///   - TPL Dataflow BatchBlock with periodic completion / relinking
///   - Channel with manual threshold-based drain loops
///   - ConcurrentQueue with manual threshold-based drain loops
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class BurstExportBenchmarks
{
    [Params(100, 500)]
    public int BatchSize;

    [Params(10, 50)]
    public int BatchCount;

    private string[] _data = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = Enumerable.Range(0, BatchSize * BatchCount)
            .Select(i => $"item-{i}").ToArray();
    }

    // -----------------------------------------------------------------
    // Kangaroo – size-triggered auto-export fires every BatchSize items
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo size-triggered bursts")]
    public void Kangaroo_SizeTriggeredBursts()
    {
        var store = new KangarooStore<string>(
            new KangarooSettings(maxStoredObjects: (uint)BatchSize));
        store.AddExporter(new NullExportWorker<string>());

        int total = BatchSize * BatchCount;
        for (int i = 0; i < total; i++)
            store.AddData(_data[i]);
    }

    // -----------------------------------------------------------------
    // Kangaroo – manual export called explicitly after each burst
    // -----------------------------------------------------------------
    [Benchmark(Description = "Kangaroo manual burst export")]
    public void Kangaroo_ManualBurstExport()
    {
        var store = new KangarooStore<string>();
        store.AddExporter(new NullExportWorker<string>());

        int total = BatchSize * BatchCount;
        for (int i = 0; i < total; i++)
        {
            store.AddData(_data[i]);
            if ((i + 1) % BatchSize == 0)
                store.StartManualExport();
        }
    }

    // -----------------------------------------------------------------
    // Dataflow – BatchBlock auto-batches, linked to ActionBlock
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow BatchBlock bursts")]
    public async Task Dataflow_BatchBlockBursts()
    {
        var batchBlock = new BatchBlock<string>(BatchSize);
        var actionBlock = new ActionBlock<string[]>(static _ => { });
        batchBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

        int total = BatchSize * BatchCount;
        for (int i = 0; i < total; i++)
            batchBlock.Post(_data[i]);

        batchBlock.Complete();
        await actionBlock.Completion.ConfigureAwait(false);
    }

    // -----------------------------------------------------------------
    // Channel – manual threshold drain every BatchSize items
    // -----------------------------------------------------------------
    [Benchmark(Description = "Channel threshold drain")]
    public void Channel_ThresholdDrain()
    {
        var channel = Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });
        Action<string[]> export = static _ => { };

        int total = BatchSize * BatchCount;
        int count = 0;
        for (int i = 0; i < total; i++)
        {
            channel.Writer.TryWrite(_data[i]);
            count++;

            if (count >= BatchSize)
            {
                var buffer = new List<string>(BatchSize);
                while (buffer.Count < count && channel.Reader.TryRead(out var item))
                    buffer.Add(item);
                export(buffer.ToArray());
                count = 0;
            }
        }
    }

    // -----------------------------------------------------------------
    // ConcurrentQueue – manual threshold drain every BatchSize items
    // -----------------------------------------------------------------
    [Benchmark(Description = "ConcurrentQueue threshold drain")]
    public void ConcurrentQueue_ThresholdDrain()
    {
        var queue = new ConcurrentQueue<string>();
        Action<string[]> export = static _ => { };

        int total = BatchSize * BatchCount;
        int count = 0;
        for (int i = 0; i < total; i++)
        {
            queue.Enqueue(_data[i]);
            count++;

            if (count >= BatchSize)
            {
                var buffer = new List<string>(BatchSize);
                while (buffer.Count < count && queue.TryDequeue(out var item))
                    buffer.Add(item);
                export(buffer.ToArray());
                count = 0;
            }
        }
    }
}
