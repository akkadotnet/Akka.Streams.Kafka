using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using BenchmarkDotNet.Attributes;
using Confluent.Kafka;
using static Akka.Streams.Kafka.Benchmark.Infrastructure.StreamHelpers;

namespace Akka.Streams.Kafka.Benchmark.Infrastructure;

public abstract class KafkaConsumerBenchmark<TMessage> : KafkaBenchmarkBase
{
    // GlobalSetup
    public override async Task SetupAsync()
    {
        await base.SetupAsync();
            
        // Populate test data if needed (for consumer benchmarks)
        await PopulateTestDataAsync();
    }
        
    protected virtual Task PopulateTestDataAsync() => 
        GenerateTestDataStringsAsync();

    protected virtual Task<(IControl control, Task<Done> completionTask)> SetupConsumerAsync()
    {
        var source = CreateSource();
        
        var (control, completionTask) = source
            .Via(CreateDemandControlFlow<TMessage>(DemandControl!.Task)) // block demand until the benchmark is ready
            .Via(Flow.Create<TMessage>().CompletionTimeout(CompletionTimeout)) // fail the stream if it doesn't complete in time
            .ToMaterialized(CreateCountingSink<TMessage>(TestMessageCount), Keep.Both)
            .Run(ActorSystem.Materializer());
        
        return Task.FromResult((control, completionTask));
    }

    protected abstract Source<TMessage, IControl> CreateSource();

    // IterationSetup (need to re-create the consumer)
    public override async Task IterationSetupAsync()
    {
        await base.IterationSetupAsync();
            
        var (control, completionTask) = await SetupConsumerAsync();
        CompletionTask = completionTask;
        StreamControl = control;
    }

    [Benchmark]
    public virtual Task ConsumeMessageAsync()
    {
        StartDemand();
        return CompletionTask!;
    }
}