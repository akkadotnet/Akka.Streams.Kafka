using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class CommittingSpec : KafkaIntegrationTests
{
    public CommittingSpec(ITestOutputHelper output, KafkaFixture fixture) 
        : base(nameof(CommittingSpec), output, fixture)
    {
    }
}