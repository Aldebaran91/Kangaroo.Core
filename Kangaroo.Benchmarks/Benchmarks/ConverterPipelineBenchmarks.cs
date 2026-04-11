using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks for the full Filter → Convert → Export pipeline.
/// Exercises KangarooExporter{T,U} with a predicate filter and type converter,
/// measuring the overhead of the abstraction compared to manual pipelines.
///
/// Compared against:
///   - TPL Dataflow TransformBlock → ActionBlock with filter
///   - Channel + manual LINQ pipeline (filter, convert, export)
///   - ConcurrentQueue + manual drain with filter and conversion
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class ConverterPipelineBenchmarks
{
    [Params(1_000, 10_000)]
    public int N;

    private int[] _data = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = Enumerable.Range(0, N).ToArray();
    }

    // -----------------------------------------------------------------
    // Kangaroo – KangarooExporter<int, string> with filter + converter,
    // queued in batch mode then exported
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo filter+convert pipeline")]
    public void Kangaroo_FilterConvertPipeline()
    {
        var store = new KangarooStore<int>();
        store.AddExporter(new IntToStringExporter());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);

        store.StartManualExport();
    }

    // -----------------------------------------------------------------
    // Kangaroo – same pipeline in direct-export mode (per item)
    // -----------------------------------------------------------------
    [Benchmark(Description = "Kangaroo filter+convert direct")]
    public void Kangaroo_FilterConvertDirect()
    {
        var store = new KangarooStore<int>(new KangarooSettings(maxStoredObjects: 1));
        store.AddExporter(new IntToStringExporter());

        for (int i = 0; i < N; i++)
            store.AddData(_data[i]);
    }

    // -----------------------------------------------------------------
    // Dataflow – TransformBlock (filter+convert) → ActionBlock
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow TransformBlock pipeline")]
    public async Task Dataflow_TransformPipeline()
    {
        var transform = new TransformManyBlock<int, string>(
            static x => x % 2 == 0 ? [x.ToString()] : [],
            new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

        var action = new ActionBlock<string>(static _ => { });

        transform.LinkTo(action, new DataflowLinkOptions { PropagateCompletion = true });

        for (int i = 0; i < N; i++)
            transform.Post(_data[i]);

        transform.Complete();
        await action.Completion.ConfigureAwait(false);
    }

    // -----------------------------------------------------------------
    // Channel – write all, then read+filter+convert+export
    // -----------------------------------------------------------------
    [Benchmark(Description = "Channel manual filter+convert")]
    public void Channel_ManualFilterConvert()
    {
        var channel = Channel.CreateUnbounded<int>(
            new UnboundedChannelOptions { SingleWriter = true, SingleReader = true });

        for (int i = 0; i < N; i++)
            channel.Writer.TryWrite(_data[i]);

        channel.Writer.Complete();

        var buffer = new List<string>();
        while (channel.Reader.TryRead(out var item))
        {
            if (item % 2 == 0)
                buffer.Add(item.ToString());
        }

        _ = buffer.ToArray();
    }

    // -----------------------------------------------------------------
    // ConcurrentQueue – enqueue, drain, filter, convert, export
    // -----------------------------------------------------------------
    [Benchmark(Description = "ConcurrentQueue manual filter+convert")]
    public void ConcurrentQueue_ManualFilterConvert()
    {
        var queue = new ConcurrentQueue<int>();

        for (int i = 0; i < N; i++)
            queue.Enqueue(_data[i]);

        var buffer = new List<string>();
        while (queue.TryDequeue(out var item))
        {
            if (item % 2 == 0)
                buffer.Add(item.ToString());
        }

        _ = buffer.ToArray();
    }

    // ===== Helpers =====

    /// <summary>
    /// Concrete KangarooExporter: filters even numbers, converts int → string.
    /// </summary>
    private sealed class IntToStringExporter : KangarooExporter<int, string>
    {
        public override Predicate<int> Filter { get; set; } = static x => x % 2 == 0;
        public override IKangarooConverter<int, string> Converter { get; set; } = new IntToStringConverter();
        public override IKangarooExportWorker<string> Worker { get; set; } = new NullExportWorker<string>();
    }

    private sealed class IntToStringConverter : IKangarooConverter<int, string>
    {
        public string Convert(int data) => data.ToString();
    }
}
