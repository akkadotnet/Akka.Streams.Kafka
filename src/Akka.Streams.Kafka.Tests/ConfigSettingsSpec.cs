using System;
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

        [Fact]
        public void Missing_ConnectionChecker_config_must_return_Disabled()
        {
            var conf = ConfigurationFactory.ParseString(@"
{ 
  kafka-clients : {
    enable.auto.commit : false
  }

  akka : {
    kafka : {
      consumer : {
        poll-interval : 50ms
        poll-timeout : 50ms
        stop-timeout : 30s
        commit-timeout : 15s
        commit-time-warning : 1s
        commit-refresh-interval : infinite
        buffer-size : 128
        use-dispatcher : akka.kafka.default-dispatcher
        kafka-clients : {
          enable : {
            auto : {
              commit : false
            }
          }
          bootstrap : {
            servers : ""localhost:9092""
          }
          client : {
            id : client-1
          }
          group : {
            id : group-1
          }
        }
        wait-close-partition : 500ms
        position-timeout : 5s
        offset-for-times-timeout : 5s
        metadata-request-timeout : 5s
        eos-draining-check-interval : 30ms
        partition-handler-warning : 5s
      }
    }
  }
}");
            var consumerSettings = ConsumerSettings<Null, string>
                .Create(conf, null, Deserializers.Utf8)
                .WithBootstrapServers("localhost:9092")
                .WithDispatcher("")
                .WithGroupId("group1");

            consumerSettings.ConnectionCheckerSettings.Enabled.Should().Be(false);
            consumerSettings.ConnectionCheckerSettings.MaxRetries.Should().Be(3);
            consumerSettings.ConnectionCheckerSettings.CheckInterval.Should().Be(TimeSpan.FromSeconds(15));
            consumerSettings.ConnectionCheckerSettings.Factor.Should().Be(2.0);
        }
        
        [Fact]
        public void CommitterSettings_must_loadDefaultValues()
        {
            // Get the default committer settings from reference.conf
            var conf = KafkaExtensions.DefaultSettings.GetConfig("akka.kafka.committer");
            var settings = CommitterSettings.Create(conf);
            
            // Verify default values match those in reference.conf
            settings.MaxBatch.Should().Be(1000);
            settings.MaxInterval.Should().Be(TimeSpan.FromSeconds(10));
            settings.Parallelism.Should().Be(100);
            settings.When.Should().BeOfType<CommitWhen.OffsetFirstObserved>();
        }
        
        [Fact]
        public void CommitterSettings_must_overrideDefaultValues()
        {
            // Create custom HOCON configuration with overridden values
            var conf = ConfigurationFactory.ParseString(@"
akka.kafka.committer {
    max-batch = 500
    max-interval = 5s
    parallelism = 4
    when = ""next-offset-observed""
}
            ").WithFallback(KafkaExtensions.DefaultSettings).GetConfig("akka.kafka.committer");
            
            var settings = CommitterSettings.Create(conf);
            
            // Verify overridden values
            settings.MaxBatch.Should().Be(500);
            settings.MaxInterval.Should().Be(TimeSpan.FromSeconds(5));
            settings.Parallelism.Should().Be(4);
            settings.When.Should().BeOfType<CommitWhen.NextOffsetObserved>();
            
            // Test the fluent API for modifying settings
            var modifiedSettings = settings
                .WithMaxBatch(200)
                .WithMaxInterval(TimeSpan.FromSeconds(2))
                .WithParallelism(8)
                .WithCommitWhen(CommitWhen.OffsetFirstObserved.Instance);
                
            modifiedSettings.MaxBatch.Should().Be(200);
            modifiedSettings.MaxInterval.Should().Be(TimeSpan.FromSeconds(2));
            modifiedSettings.Parallelism.Should().Be(8);
            modifiedSettings.When.Should().BeOfType<CommitWhen.OffsetFirstObserved>();
        }
    }
}
