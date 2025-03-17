using System.Threading.Tasks;
using Akka.Streams.Kafka.Helpers;
using BenchmarkDotNet.Attributes;

namespace Akka.Streams.Kafka.Benchmark.Infrastructure;

public abstract class KafkaConsumerBenchmark : KafkaBenchmarkBase
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
        
    protected abstract Task<(IControl control, Task<Done> completionTask)> SetupConsumerAsync();

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