using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks measuring category-based routing throughput. Kangaroo natively
/// supports Enum-based category dispatch to route data to the correct handler.
///
/// Compared against manual routing implementations using:
///   - ConcurrentQueue per category with dictionary lookup
///   - Channel per category with dictionary lookup
///   - TPL Dataflow with ActionBlock per category linked via predicates
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class CategoryRoutingBenchmarks
{
    private enum LogLevel { Debug, Info, Warning, Error }

    [Params(1_000, 10_000)]
    public int N;

    private (string Data, LogLevel Category)[] _taggedData = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var levels = Enum.GetValues<LogLevel>();
        _taggedData = Enumerable.Range(0, N)
            .Select(i => ($"msg-{i}", levels[i % levels.Length]))
            .ToArray();
    }

    // -----------------------------------------------------------------
    // Kangaroo – AddData with category, one exporter per category,
    // then manual batch export
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo category routing + export")]
    public void Kangaroo_CategoryRouting()
    {
        var store = new KangarooStore<string>();
        foreach (LogLevel level in Enum.GetValues<LogLevel>())
            store.AddExporter(new NullExportWorker<string>(), level);

        for (int i = 0; i < N; i++)
            store.AddData(_taggedData[i].Data, _taggedData[i].Category);

        store.StartManualExport();
    }

    // -----------------------------------------------------------------
    // Kangaroo – direct export mode (MaxStoredObjects=1) with categories
    // -----------------------------------------------------------------
    [Benchmark(Description = "Kangaroo category direct export")]
    public void Kangaroo_CategoryDirectExport()
    {
        var store = new KangarooStore<string>(new KangarooSettings(maxStoredObjects: 1));
        foreach (LogLevel level in Enum.GetValues<LogLevel>())
            store.AddExporter(new NullExportWorker<string>(), level);

        for (int i = 0; i < N; i++)
            store.AddData(_taggedData[i].Data, _taggedData[i].Category);
    }

    // -----------------------------------------------------------------
    // ConcurrentQueue – dictionary of queues, one per category
    // -----------------------------------------------------------------
    [Benchmark(Description = "ConcurrentQueue dictionary routing")]
    public void ConcurrentQueue_DictionaryRouting()
    {
        var queues = new Dictionary<LogLevel, ConcurrentQueue<string>>();
        foreach (LogLevel level in Enum.GetValues<LogLevel>())
            queues[level] = new ConcurrentQueue<string>();

        for (int i = 0; i < N; i++)
            queues[_taggedData[i].Category].Enqueue(_taggedData[i].Data);

        // Drain all queues
        foreach (var (_, queue) in queues)
        {
            var buffer = new List<string>();
            while (queue.TryDequeue(out var item))
                buffer.Add(item);
            _ = buffer.ToArray();
        }
    }

    // -----------------------------------------------------------------
    // Channel – dictionary of channels, one per category
    // -----------------------------------------------------------------
    [Benchmark(Description = "Channel dictionary routing")]
    public void Channel_DictionaryRouting()
    {
        var channels = new Dictionary<LogLevel, Channel<string>>();
        foreach (LogLevel level in Enum.GetValues<LogLevel>())
            channels[level] = Channel.CreateUnbounded<string>(
                new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });

        for (int i = 0; i < N; i++)
            channels[_taggedData[i].Category].Writer.TryWrite(_taggedData[i].Data);

        // Drain all channels
        foreach (var (_, channel) in channels)
        {
            channel.Writer.Complete();
            var buffer = new List<string>();
            while (channel.Reader.TryRead(out var item))
                buffer.Add(item);
            _ = buffer.ToArray();
        }
    }

    // -----------------------------------------------------------------
    // Dataflow – one ActionBlock per category, BroadcastBlock with
    // predicate-based linking to dispatch
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow BroadcastBlock predicate routing")]
    public async Task Dataflow_PredicateRouting()
    {
        var broadcast = new BroadcastBlock<(string Data, LogLevel Category)>(static x => x);
        var actions = new Dictionary<LogLevel, ActionBlock<(string Data, LogLevel Category)>>();

        foreach (LogLevel level in Enum.GetValues<LogLevel>())
        {
            var action = new ActionBlock<(string Data, LogLevel Category)>(static _ => { });
            actions[level] = action;
            var captured = level;
            broadcast.LinkTo(action,
                new DataflowLinkOptions { PropagateCompletion = true },
                item => item.Category == captured);
        }

        for (int i = 0; i < N; i++)
            broadcast.Post(_taggedData[i]);

        broadcast.Complete();
        await Task.WhenAll(actions.Values.Select(a => a.Completion)).ConfigureAwait(false);
    }
}
