using Akka.Configuration;
using Akka.Streams.Kafka.Settings;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Kafka.Tests
{
    public class ConfigSettingsSpec
    {
        [Fact]
        public void ConsumerSettings_must_handleNestedKafkaClientsProperties()
        {
            var conf = ConfigurationFactory.ParseString(@"
akka.kafka.consumer.kafka-clients {{
    bootstrap.servers = ""localhost:9092""
    bootstrap.foo = baz
    foo = bar
    client.id = client1
}}
            ").WithFallback(KafkaExtensions.DefaultSettings).GetConfig("akka.kafka.consumer");

            var settings = ConsumerSettings<string, string>.Create(conf, null, null);
            settings.GetProperty("bootstrap.servers").Should().Be("localhost:9092");
            settings.GetProperty("client.id").Should().Be("client1");
            settings.GetProperty("foo").Should().Be("bar");
            settings.GetProperty("bootstrap.foo").Should().Be("baz");
            settings.GetProperty("enable.auto.commit").Should().Be("false");
        }
        
        [Fact]
        public void ProducerSettings_must_handleNestedKafkaClientsProperties()
        {
            var conf = ConfigurationFactory.ParseString(@"
akka.kafka.producer.kafka-clients {{
    bootstrap.servers = ""localhost:9092""
    bootstrap.foo = baz
    foo = bar
    client.id = client1
}}
            ").WithFallback(KafkaExtensions.DefaultSettings).GetConfig("akka.kafka.producer");

            var settings = ProducerSettings<string, string>.Create(conf, null, null);
            settings.GetProperty("bootstrap.servers").Should().Be("localhost:9092");
            settings.GetProperty("client.id").Should().Be("client1");
            settings.GetProperty("foo").Should().Be("bar");
            settings.GetProperty("bootstrap.foo").Should().Be("baz");
        }
        
    }
}
