using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks measuring fan-out performance when multiple export handlers
/// process the same data. Kangaroo natively supports registering multiple
/// uncategorized exporters that each receive the full batch.
///
/// Compared against:
///   - TPL Dataflow BroadcastBlock → multiple ActionBlocks
///   - Channel with manual fan-out to multiple consumers
///   - ConcurrentQueue drain with sequential multi-handler dispatch
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class MultiExporterFanOutBenchmarks
{
    [Params(2, 5)]
    public int ExporterCount;

    [Params(1_000, 10_000)]
    public int N;

    private string[] _data = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = Enumerable.Range(0, N).Select(i => $"item-{i}").ToArray();
    }

    // -----------------------------------------------------------------
    // Kangaroo – multiple exporters registered, batch export fans out
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo multi-exporter fan-out")]
    public void Kangaroo_MultiExporterFanOut()
    {
        var store = new KangarooStore<string>();
        for (int e = 0; e < ExporterCount; e++)
            store.AddExporter(new NullExportWorker<string>());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);

        store.StartManualExport();
    }

    // -----------------------------------------------------------------
    // Kangaroo – multi-exporter in direct-export mode (per item)
    // -----------------------------------------------------------------
    [Benchmark(Description = "Kangaroo multi-exporter direct")]
    public void Kangaroo_MultiExporterDirect()
    {
        var store = new KangarooStore<string>(new KangarooSettings(maxStoredObjects: 1));
        for (int e = 0; e < ExporterCount; e++)
            store.AddExporter(new NullExportWorker<string>());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);
    }

    // -----------------------------------------------------------------
    // Dataflow – BroadcastBlock → N ActionBlocks
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow BroadcastBlock fan-out")]
    public async Task Dataflow_BroadcastFanOut()
    {
        var broadcast = new BroadcastBlock<string>(static x => x);
        var actions = new ActionBlock<string>[ExporterCount];

        for (int e = 0; e < ExporterCount; e++)
        {
            actions[e] = new ActionBlock<string>(static _ => { });
            broadcast.LinkTo(actions[e], new DataflowLinkOptions { PropagateCompletion = true });
        }

        for (int i = 0; i < N; i++)
            broadcast.Post(_data[i]);

        broadcast.Complete();
        await Task.WhenAll(actions.Select(a => a.Completion)).ConfigureAwait(false);
    }

    // -----------------------------------------------------------------
    // Channel – single producer, N consumers reading the same data
    // (simulated via cloning into N separate channels)
    // -----------------------------------------------------------------
    [Benchmark(Description = "Channel cloned fan-out")]
    public void Channel_ClonedFanOut()
    {
        var channels = new Channel<string>[ExporterCount];
        for (int e = 0; e < ExporterCount; e++)
            channels[e] = Channel.CreateUnbounded<string>(
                new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });

        // Fan-out: write each item to every channel
        for (int i = 0; i < N; i++)
        {
            var item = _data[i];
            for (int e = 0; e < ExporterCount; e++)
                channels[e].Writer.TryWrite(item);
        }

        // Drain all channels
        for (int e = 0; e < ExporterCount; e++)
        {
            channels[e].Writer.Complete();
            while (channels[e].Reader.TryRead(out _)) { }
        }
    }

    // -----------------------------------------------------------------
    // ConcurrentQueue – single queue, drain once, dispatch to N handlers
    // -----------------------------------------------------------------
    [Benchmark(Description = "ConcurrentQueue sequential fan-out")]
    public void ConcurrentQueue_SequentialFanOut()
    {
        var queue = new ConcurrentQueue<string>();

        for (int i = 0; i < N; i++)
            queue.Enqueue(_data[i]);

        // Drain
        var buffer = new List<string>(N);
        while (queue.TryDequeue(out var item))
            buffer.Add(item);

        var array = buffer.ToArray();

        // Fan-out: dispatch to N handlers
        Action<string[]> handler = static _ => { };
        for (int e = 0; e < ExporterCount; e++)
            handler(array);
    }
}
