using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;
using BenchmarkDotNet.Attributes;
using static Akka.Streams.Kafka.Benchmark.Infrastructure.StreamHelpers;

namespace Akka.Streams.Kafka.Benchmark.Infrastructure;

public abstract class KafkaConsumerBenchmark<TMessage> : KafkaBenchmarkBase
{
    // GlobalSetup
    public override async Task SetupAsync()
    {
        await base.SetupAsync();
            
        ActorSystem.Log.Info($"Populating data for Topic: {TopicName} [{TestMessageCount} messages]");
        
        // Populate test data if needed (for consumer benchmarks)
        await PopulateTestDataAsync();
        
        ActorSystem.Log.Info($"Test messages populated.");
    }
        
    protected virtual Task PopulateTestDataAsync() => 
        GenerateTestDataStringsAsync();

    protected virtual (IControl control, Task<Done> completionTask) SetupConsumer()
    {
        var source = CreateSource();
        
        var (control, completionTask) = source
            .Via(CreateDemandControlFlow<TMessage>(DemandControl!.Task)) // block demand until the benchmark is ready
            .Via(Flow.Create<TMessage>().CompletionTimeout(CompletionTimeout)) // fail the stream if it doesn't complete in time
            .Via(ProgressLogger<TMessage>(TestMessageCount, 0.05)) // log every 5% of the stream
            .ToMaterialized(CreateCountingSink<TMessage>(TestMessageCount), Keep.Both)
            .Run(ActorSystem.Materializer());
        
        return (control, completionTask);
    }

    protected abstract Source<TMessage, IControl> CreateSource();

    // IterationSetup (need to re-create the consumer)
    public override void IterationSetup()
    {
        base.IterationSetup();
            
        var (control, completionTask) = SetupConsumer();
        CompletionTask = completionTask;
        StreamControl = control;
    }
}