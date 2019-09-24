using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Settings;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class SourceWithOffsetContextIntegrationTests : KafkaIntegrationTests
    {
        public SourceWithOffsetContextIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(SourceWithOffsetContextIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task SourceWithOffsetContext_should_work()
        {
            var topic = CreateTopic(1);
            var settings = CreateConsumerSettings<string>(CreateGroup(1));
            
            var task = KafkaConsumer.SourceWithOffsetContext(settings, Subscriptions.Topics(topic))
                .SelectAsync(10, message => Task.FromResult(Done.Instance))
                .Via()
        }
    }
}