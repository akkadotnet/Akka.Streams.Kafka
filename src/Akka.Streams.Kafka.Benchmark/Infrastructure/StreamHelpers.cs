using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Akka.Streams.Kafka.Benchmark.Infrastructure;

public static class StreamHelpers
{
    /// <summary>
    /// Creates a sink that counts messages and completes when either:
    /// 1. The specified number of messages have been processed
    /// 2. The upstream completes
    /// 3. An error occurs
    /// </summary>
    public static Sink<T, Task<Done>> CreateCountingSink<T>(int stopAt)
    {
        return Flow.Create<T>()
            .Take(stopAt)
            .WatchTermination((_, task) => task)
            .To(Sink.Ignore<T>());
    }

    /// <summary>
    /// Log our process every <see cref="percentageFrequency"/>% through the <see cref="maxMessageCount"/>
    /// </summary>
    public static Flow<T, T, NotUsed> ProgressLogger<T>(int maxMessageCount, double percentageFrequency)
    {
        var nThMessage = (int)(maxMessageCount * percentageFrequency / 100);

        return Flow.Create<T>()
            .Via(new LogEveryNthElement<T>(nThMessage, i => $"{i}/{maxMessageCount} messages processed"));
    }

    /// <summary>
    /// Creates a flow that controls demand. Basically designed to stop a stream from automatically
    /// running as soon as it is materialized during Setup. This will add a small amount of overhead
    /// to our benchmarks, but it will also make sure that most of the interesting stuff happens during
    /// the benchmark itself.
    /// </summary>
    public static Flow<T, T, NotUsed> CreateDemandControlFlow<T>(Task startSignal)
    {
        return Flow.Create<T>()
            .SelectAsync(1, async elem =>
            {
                if (!startSignal.IsCompleted)
                    await startSignal;
                
                return elem;
            });
    }
}