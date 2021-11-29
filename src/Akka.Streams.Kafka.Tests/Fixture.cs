using Akka.Streams.Kafka.Testkit.Fixture;
using Xunit;

namespace Akka.Streams.Kafka.Tests
{
    [CollectionDefinition(Name)]
    public sealed class KafkaSpecsFixture : ICollectionFixture<KafkaFixture>
    {
        public const string Name = "KafkaSpecs";
    }
}