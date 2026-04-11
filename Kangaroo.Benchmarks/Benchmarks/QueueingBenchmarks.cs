using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing the throughput of adding items to Kangaroo vs.
/// equivalent producer-side structures: TPL Dataflow BufferBlock, a raw
/// System.Threading.Channels unbounded channel, and ConcurrentQueue.
///
/// No export is triggered in this suite – only the enqueue path is measured.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class QueueingBenchmarks
{
    [Params(100, 1_000, 10_000)]
    public int N;

    private string[] _data = null!;

    // Kangaroo store with no export worker registered (unbounded queue mode)
    private KangarooStore<string> _kangarooStore = null!;

    // TPL Dataflow
    private BufferBlock<string> _bufferBlock = null!;

    // Raw Channel
    private Channel<string> _channel = null!;
    private ChannelWriter<string> _channelWriter = null!;

    // ConcurrentQueue
    private ConcurrentQueue<string> _concurrentQueue = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = Enumerable.Range(0, N).Select(i => $"item-{i}").ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _kangarooStore = new KangarooStore<string>();
        _bufferBlock = new BufferBlock<string>();
        _channel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleWriter = false, SingleReader = false });
        _channelWriter = _channel.Writer;
        _concurrentQueue = new ConcurrentQueue<string>();
    }

    [Benchmark(Baseline = true, Description = "Kangaroo AddData")]
    public void Kangaroo_AddData()
    {
        for (int i = 0; i < N; i++)
            _kangarooStore.AddData(_data[i]);
    }

    [Benchmark(Description = "Dataflow BufferBlock.Post")]
    public void Dataflow_BufferBlock_Post()
    {
        for (int i = 0; i < N; i++)
            _bufferBlock.Post(_data[i]);
    }

    [Benchmark(Description = "Channel TryWrite")]
    public void Channel_TryWrite()
    {
        for (int i = 0; i < N; i++)
            _channelWriter.TryWrite(_data[i]);
    }

    [Benchmark(Description = "ConcurrentQueue Enqueue")]
    public void ConcurrentQueue_Enqueue()
    {
        for (int i = 0; i < N; i++)
            _concurrentQueue.Enqueue(_data[i]);
    }
}
