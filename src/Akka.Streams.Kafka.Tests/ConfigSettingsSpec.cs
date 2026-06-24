// -----------------------------------------------------------------------
//  <copyright file="ConfigSettingsSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Xunit;

namespace Akka.Streams.Kafka.Tests;

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
        Assert.Equal("localhost:9092", settings.GetProperty("bootstrap.servers"));
        Assert.Equal("client1", settings.GetProperty("client.id"));
        Assert.Equal("bar", settings.GetProperty("foo"));
        Assert.Equal("baz", settings.GetProperty("bootstrap.foo"));
        Assert.Equal("false", settings.GetProperty("enable.auto.commit"));
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
        Assert.Equal("localhost:9092", settings.GetProperty("bootstrap.servers"));
        Assert.Equal("latest", settings.GetProperty("auto.offset.reset"));
        Assert.Equal("True", settings.GetProperty("enable.auto.commit"));
        Assert.Equal("group1", settings.GetProperty("group.id"));
        Assert.Equal("client1", settings.GetProperty("client.id"));
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
        Assert.Equal("localhost:9092", settings.GetProperty("bootstrap.servers"));
        Assert.Equal("client1", settings.GetProperty("client.id"));
        Assert.Equal("bar", settings.GetProperty("foo"));
        Assert.Equal("baz", settings.GetProperty("bootstrap.foo"));
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
        Assert.Equal("localhost:9092", settings.GetProperty("bootstrap.servers"));
        Assert.Equal("client1", settings.GetProperty("client.id"));
        Assert.Equal("True", settings.GetProperty("enable.idempotence"));
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

        Assert.False(consumerSettings.ConnectionCheckerSettings.Enabled);
        Assert.Equal(3, consumerSettings.ConnectionCheckerSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(15), consumerSettings.ConnectionCheckerSettings.CheckInterval);
        Assert.Equal(2.0, consumerSettings.ConnectionCheckerSettings.Factor);
    }

    [Fact]
    public void CommitterSettings_must_loadDefaultValues()
    {
        // Get the default committer settings from reference.conf
        var conf = KafkaExtensions.DefaultSettings.GetConfig("akka.kafka.committer");
        var settings = CommitterSettings.Create(conf);

        // Verify default values match those in reference.conf
        Assert.Equal(1000, settings.MaxBatch);
        Assert.Equal(TimeSpan.FromSeconds(10), settings.MaxInterval);
        Assert.Equal(100, settings.Parallelism);
        Assert.True((settings.When) is CommitWhen.OffsetFirstObserved);
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
        Assert.Equal(500, settings.MaxBatch);
        Assert.Equal(TimeSpan.FromSeconds(5), settings.MaxInterval);
        Assert.Equal(4, settings.Parallelism);
        Assert.True((settings.When) is CommitWhen.NextOffsetObserved);

        // Test the fluent API for modifying settings
        var modifiedSettings = settings
            .WithMaxBatch(200)
            .WithMaxInterval(TimeSpan.FromSeconds(2))
            .WithParallelism(8)
            .WithCommitWhen(CommitWhen.OffsetFirstObserved.Instance);

        Assert.Equal(200, modifiedSettings.MaxBatch);
        Assert.Equal(TimeSpan.FromSeconds(2), modifiedSettings.MaxInterval);
        Assert.Equal(8, modifiedSettings.Parallelism);
        Assert.True((modifiedSettings.When) is CommitWhen.OffsetFirstObserved);
    }
}