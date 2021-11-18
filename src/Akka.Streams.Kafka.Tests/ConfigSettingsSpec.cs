using Akka.Configuration;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
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
        public void ConsumerSettings_must_beAbleToMergeConsumerConfig()
        {
            var conf = KafkaExtensions.DefaultSettings.GetConfig("akka.kafka.consumer");
            var settings = ConsumerSettings<string, string>.Create(conf, null, null);
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                GroupId = "group1",
                ClientId = "client1"
            };

            settings = settings.WithConsumerConfig(config);
            settings.GetProperty("bootstrap.servers").Should().Be("localhost:9092");
            settings.GetProperty("auto.offset.reset").Should().Be("latest");
            settings.GetProperty("enable.auto.commit").Should().Be("True");
            settings.GetProperty("group.id").Should().Be("group1");
            settings.GetProperty("client.id").Should().Be("client1");
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
        
        [Fact]
        public void ProducerSettings_must_beAbleToMergeProducerConfig()
        {
            var conf = KafkaExtensions.DefaultSettings.GetConfig("akka.kafka.producer");
            var settings = ProducerSettings<string, string>.Create(conf, null, null);
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "client1", 
                EnableIdempotence = true
            };

            settings = settings.WithProducerConfig(config);
            settings.GetProperty("bootstrap.servers").Should().Be("localhost:9092");
            settings.GetProperty("client.id").Should().Be("client1");
            settings.GetProperty("enable.idempotence").Should().Be("True");
        }
        
    }
}
