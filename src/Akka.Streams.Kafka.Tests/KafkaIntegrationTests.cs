using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Tests
{
    [Collection(KafkaSpecsFixture.Name)]
    public abstract class KafkaIntegrationTests : Akka.TestKit.Xunit2.TestKit
    {
        private readonly KafkaFixture _fixture;
        protected IMaterializer Materializer { get; }

        public KafkaIntegrationTests(string actorSystemName, ITestOutputHelper output, KafkaFixture fixture) 
            : base(Default(), actorSystemName, output)
        {
            _fixture = fixture;
            Materializer = Sys.Materializer();
            
            Sys.Log.Info("Starting test: " + output.GetCurrentTestName());
        }
        
        private string Uuid { get; } = Guid.NewGuid().ToString();
        
        protected string CreateTopic(int number) => $"topic-{number}-{Uuid}";
        protected string CreateGroup(int number) => $"group-{number}-{Uuid}";

        protected ProducerSettings<Null, string> ProducerSettings => BuildProducerSettings<Null, string>();
        
        protected ProducerSettings<TKey, TValue> BuildProducerSettings<TKey, TValue>()
        {
            return ProducerSettings<TKey, TValue>.Create(Sys, null, null).WithBootstrapServers(_fixture.KafkaServer);
        }

        protected CommitterSettings CommitterSettings
        {
            get => CommitterSettings.Create(Sys);
        }
        
        protected ConsumerSettings<TKey, TValue> CreateConsumerSettings<TKey, TValue>(string group)
        {
            return ConsumerSettings<TKey, TValue>.Create(Sys, null, null)
                .WithBootstrapServers(_fixture.KafkaServer)
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }

        protected ConsumerSettings<Null, TValue> CreateConsumerSettings<TValue>(string group)
        {
            return ConsumerSettings<Null, TValue>.Create(Sys, null, null)
                .WithBootstrapServers(_fixture.KafkaServer)
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithProperty("auto.offset.reset", "earliest")
                .WithProperty("enable.auto.commit", "false")
                .WithGroupId(group);
        }
        
        protected async Task ProduceStrings<TKey>(string topic, IEnumerable<int> range, ProducerSettings<TKey, string> producerSettings)
        {
            await Source
                .From(range)
                .Select(elem => new ProducerRecord<TKey, string>(topic, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), Materializer);
        }
        
        protected async Task ProduceStrings<TKey>(Func<int, TopicPartition> partitionSelector, IEnumerable<int> range, ProducerSettings<TKey, string> producerSettings)
        {
            await Source
                .From(range)
                .Select(elem => new ProducerRecord<TKey, string>(partitionSelector(elem), elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), Materializer);
        }
        
        protected async Task ProduceStrings<TKey>(TopicPartition topicPartition, IEnumerable<int> range, ProducerSettings<TKey, string> producerSettings)
        {
            await Source
                .From(range)
                .Select(elem => new ProducerRecord<TKey, string>(topicPartition, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(producerSettings), Materializer);
        }

        /// <summary>
        /// Asserts that task will finish successfully until specified timeout.
        /// Throws task exception if task failes
        /// </summary>
        protected void AssertTaskCompletesWithin(TimeSpan timeout, Task task, bool assertIsSuccessful = true)
        {
            AwaitCondition(() => task.IsCompleted, timeout, $"task should complete within {timeout} timeout");
            
            if (assertIsSuccessful)
                task.IsCompletedSuccessfully.Should().Be(true, "task should compete successfully");
        }
        
        /// <summary>
        /// Asserts that task will finish successfully until specified timeout.
        /// Throws task exception if task failes
        /// </summary>
        protected TResult AssertTaskCompletesWithin<TResult>(TimeSpan timeout, Task<TResult> task, bool assertIsSuccessful = true)
        {
            AwaitCondition(() => task.IsCompleted, timeout, $"task should complete within {timeout} timeout");
            
            if (assertIsSuccessful)
                task.IsCompletedSuccessfully.Should().Be(true, "task should compete successfully");

            return task.Result;
        }

        protected async Task GivenInitializedTopic(string topic)
        {
            var builder = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _fixture.KafkaServer
            });
            using (var client = builder.Build())
            {
                await client.CreateTopicsAsync(new[] {new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = KafkaFixture.KafkaPartitions,
                    ReplicationFactor = KafkaFixture.KafkaReplicationFactor
                }});
            }
        }
        
        protected async Task GivenInitializedTopic(TopicPartition topicPartition)
        {
            var builder = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _fixture.KafkaServer
            });
            using (var client = builder.Build())
            {
                await client.CreateTopicsAsync(new[] {new TopicSpecification
                {
                    Name = topicPartition.Topic,
                    NumPartitions = KafkaFixture.KafkaPartitions,
                    ReplicationFactor = KafkaFixture.KafkaReplicationFactor
                }});
            }
        }
        
        protected (IControl, TestSubscriber.Probe<TValue>) CreateExternalPlainSourceProbe<TValue>(IActorRef consumer, IManualSubscription sub)
        {
            return KafkaConsumer
                .PlainExternalSource<Null, TValue>(consumer, sub)
                .Select(c => c.Value)
                .ToMaterialized(this.SinkProbe<TValue>(), Keep.Both)
                .Run(Materializer);
        }

        private static Config Default()
        {
            var config = ConfigurationFactory.ParseString("akka.loglevel = DEBUG");
            //var config = ConfigurationFactory.ParseString("akka{}");

            if (TestsConfiguration.UseFileLogging)
            {
                config = config.WithFallback(
                    ConfigurationFactory.ParseString("akka.loggers = [\"Akka.Streams.Kafka.Tests.Logging.SimpleFileLoggerActor, Akka.Streams.Kafka.Tests\"]"));
            }
            
            return config.WithFallback(KafkaExtensions.DefaultSettings);
        }
    }
}