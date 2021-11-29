using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Testkit.Fixture
{
    [CollectionDefinition(Name)]
    public sealed class KafkaSpecsFixture : ICollectionFixture<KafkaFixture>
    {
        public const string Name = "KafkaSpecs";
    }
    
    public class KafkaFixture : KafkaFixtureBase
    {
        public KafkaFixture(IMessageSink sink) : base(1, 3, 1, new MessageSinkLogger(sink))
        {
        }
    }
}