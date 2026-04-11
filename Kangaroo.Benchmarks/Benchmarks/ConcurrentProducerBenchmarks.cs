using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Kangaroo;
using Kangaroo.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Kangaroo.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks measuring enqueue throughput under multi-threaded contention.
/// Multiple producer threads race to push items concurrently, stressing
/// the thread-safety guarantees of each data structure.
///
/// Kangaroo (unbounded queue mode) is compared against:
///   - ConcurrentQueue (lock-free enqueue)
///   - System.Threading.Channels unbounded channel
///   - TPL Dataflow BufferBlock
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class ConcurrentProducerBenchmarks
{
    [Params(4, 8)]
    public int ProducerCount;

    [Params(1_000, 10_000)]
    public int ItemsPerProducer;

    private string[][] _producerData = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _producerData = new string[ProducerCount][];
        for (int p = 0; p < ProducerCount; p++)
            _producerData[p] = Enumerable.Range(0, ItemsPerProducer)
                .Select(i => $"producer-{p}-item-{i}").ToArray();
    }

    // -----------------------------------------------------------------
    // Kangaroo – multiple threads calling AddData concurrently
    // -----------------------------------------------------------------
    [Benchmark(Baseline = true, Description = "Kangaroo concurrent AddData")]
    public void Kangaroo_ConcurrentAddData()
    {
        var store = new KangarooStore<string>();

        Parallel.For(0, ProducerCount, p =>
        {
            var data = _producerData[p];
            for (int i = 0; i < ItemsPerProducer; i++)
                store.AddData(data[i]);
        });
    }

    // -----------------------------------------------------------------
    // ConcurrentQueue – lock-free concurrent enqueue
    // -----------------------------------------------------------------
    [Benchmark(Description = "ConcurrentQueue concurrent Enqueue")]
    public void ConcurrentQueue_ConcurrentEnqueue()
    {
        var queue = new ConcurrentQueue<string>();

        Parallel.For(0, ProducerCount, p =>
        {
            var data = _producerData[p];
            for (int i = 0; i < ItemsPerProducer; i++)
                queue.Enqueue(data[i]);
        });
    }

    // -----------------------------------------------------------------
    // Channel – concurrent TryWrite from multiple threads
    // -----------------------------------------------------------------
    [Benchmark(Description = "Channel concurrent TryWrite")]
    public void Channel_ConcurrentTryWrite()
    {
        var channel = Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleWriter = false, SingleReader = false });
        var writer = channel.Writer;

        Parallel.For(0, ProducerCount, p =>
        {
            var data = _producerData[p];
            for (int i = 0; i < ItemsPerProducer; i++)
                writer.TryWrite(data[i]);
        });
    }

    // -----------------------------------------------------------------
    // Dataflow – concurrent Post to BufferBlock
    // -----------------------------------------------------------------
    [Benchmark(Description = "Dataflow BufferBlock concurrent Post")]
    public void Dataflow_BufferBlock_ConcurrentPost()
    {
        var bufferBlock = new BufferBlock<string>();

        Parallel.For(0, ProducerCount, p =>
        {
            var data = _producerData[p];
            for (int i = 0; i < ItemsPerProducer; i++)
                bufferBlock.Post(data[i]);
        });
    }
}
