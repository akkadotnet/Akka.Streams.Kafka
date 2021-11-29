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
using Akka.Streams.Kafka.Testkit;
using Akka.Streams.Kafka.Testkit.Fixture;
using Akka.Streams.Kafka.Testkit.Internal;
using Akka.Streams.TestKit;
using Akka.Util;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Tests
{
    [Collection(KafkaSpecsFixture.Name)]
    public abstract class KafkaIntegrationTests : Akka.TestKit.Xunit2.TestKit, IAsyncLifetime
    {
        protected const long MessageLogInterval = 500L;

        protected const int Partition0 = 0;

        protected Flow<int, int, NotUsed> LogSentMessages()
            => Flow.Create<int>().Select(i =>
            {
                if(i % MessageLogInterval == 0)
                    Log.Info("Sent [{0}] messages so far", i);
                return i;
            });

        protected Flow<int, int, NotUsed> LogReceivedMessages()
            => Flow.Create<int>().Select(i =>
            {
                if(i % MessageLogInterval == 0)
                    Log.Info("Received [{0}] messages so far", i);
                return i;
            });
        
        protected Flow<int, int, NotUsed> LogReceivedMessages(TopicPartition tp)
            => Flow.Create<int>().Select(i =>
            {
                if(i % MessageLogInterval == 0)
                    Log.Info("{0}: Received [{1}] messages so far", tp, i);
                return i;
            });

        protected async Task StopRandomBrokerAsync(int msgCount)
        {
            var broker = Fixture.Brokers[ThreadLocalRandom.Current.Next(Fixture.Brokers.Count)];
            Log.Warning(
                "Stopping one Kafka container with network aliases [{0}], container id [{1}], after [{2}] messages", 
                broker.ContainerName,
                broker.ContainerId,
                msgCount);
            await broker.StopAsync();
        }

        private IAdminClient _adminClient;
        public KafkaFixtureBase Fixture { get; protected set; }
        
        protected IMaterializer Materializer { get; }
        protected KafkaTestkitSettings Settings { get; }

        public KafkaIntegrationTests(string actorSystemName, ITestOutputHelper output, KafkaFixtureBase fixture = null) 
            : base(Default(), actorSystemName, output)
        {
            Fixture = fixture;
            Materializer = Sys.Materializer();
            Sys.Settings.InjectTopLevelFallback(KafkaTestKit.DefaultConfig);
            Settings = new KafkaTestkitSettings(Sys);

            Log.Info("Starting test: " + output.GetCurrentTestName());
        }
        
        private string Uuid { get; } = Guid.NewGuid().ToString();
        
        protected string CreateTopicName(int number) => $"topic-{number}-{Uuid}";
        protected string CreateGroupId(int number) => $"group-{number}-{Uuid}";

        public async Task InitializeAsync()
        {
            if (!(Fixture is KafkaFixture))
                await Fixture.InitializeAsync();

            _adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = Fixture.BootstrapServer
            }).Build();
        }

        public async Task DisposeAsync()
        {
            if (!(Fixture is KafkaFixture))
                await Fixture.DisposeAsync();
            _adminClient?.Dispose();
        }
        
        protected ProducerSettings<Null, string> ProducerSettings => BuildProducerSettings<Null, string>();
        
        protected ProducerSettings<TKey, TValue> BuildProducerSettings<TKey, TValue>()
        {
            return ProducerSettings<TKey, TValue>.Create(Sys, null, null).WithBootstrapServers(Fixture.BootstrapServer);
        }

        protected CommitterSettings CommitterSettings
        {
            get => CommitterSettings.Create(Sys);
        }
        
        protected ConsumerSettings<TKey, TValue> CreateConsumerSettings<TKey, TValue>(string group)
        {
            return ConsumerSettings<TKey, TValue>.Create(Sys, null, null)
                .WithBootstrapServers(Fixture.BootstrapServer)
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }

        protected ConsumerSettings<Null, TValue> CreateConsumerSettings<TValue>(string group)
        {
            return ConsumerSettings<Null, TValue>.Create(Sys, null, null)
                .WithBootstrapServers(Fixture.BootstrapServer)
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithProperty("auto.offset.reset", "earliest")
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
            await _adminClient.CreateTopicsAsync(new[] {new TopicSpecification
            {
                Name = topic,
                NumPartitions = Fixture.PartitionCount,
                ReplicationFactor = (short)Fixture.ReplicationFactor
            }});
        }
        
        protected async Task GivenInitializedTopic(TopicPartition topicPartition)
        {
            await _adminClient.CreateTopicsAsync(new[] {new TopicSpecification
            {
                Name = topicPartition.Topic,
                NumPartitions = Fixture.PartitionCount,
                ReplicationFactor = (short)Fixture.ReplicationFactor
            }});
        }
        
        protected (IControl, TestSubscriber.Probe<TValue>) CreateExternalPlainSourceProbe<TValue>(IActorRef consumer, IManualSubscription sub)
        {
            return KafkaConsumer
                .PlainExternalSource<Null, TValue>(consumer, sub, true)
                .Select(c => c.Message.Value)
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