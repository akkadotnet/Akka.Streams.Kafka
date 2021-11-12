using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Akka.Streams.Kafka.TestKit.Internal
{
    public abstract class KafkaTestKit : Akka.TestKit.Xunit2.TestKit
    {
        private static readonly AtomicCounter _topicCounter = new AtomicCounter();

        private static ImmutableDictionary<string, string> CreateReplicationFactorBrokerProps(int replicationFactor)
            => new Dictionary<string, string>
            {
                ["offsets.topic.replication.factor"] = replicationFactor.ToString(),
                ["transaction.state.log.replication.factor"] = replicationFactor.ToString(),
                ["transaction.state.log.min.isr"] = replicationFactor.ToString()
            }.ToImmutableDictionary();

        protected const string DefaultKey = "key";

        protected readonly ISerializer<string> StringSerializer = Serializers.Utf8;
        protected readonly IDeserializer<string> StringDeserializer = Deserializers.Utf8;

        protected ProducerSettings<string, string> ProducerDefaults()
            => ProducerDefaults(StringSerializer, StringSerializer);

        protected ProducerSettings<TKey, TValue> ProducerDefaults<TKey, TValue>(
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer)
            => ProducerSettings<TKey, TValue>.Create(Sys, keySerializer, valueSerializer)
                .WithBootstrapServers(BootstrapServers);

        protected ConsumerSettings<string, string> ConsumerDefaults()
            => ConsumerDefaults(StringDeserializer, StringDeserializer);

        protected ConsumerSettings<TKey, TValue> ConsumerDefaults<TKey, TValue>(
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer)
            => ConsumerSettings<TKey, TValue>.Create(Sys, keyDeserializer, valueDeserializer)
                .WithBootstrapServers(BootstrapServers)
                .WithProperty("auto.offset.reset", "earliest");

        private Lazy<CommitterSettings> CommitterDefaultsInstance => new Lazy<CommitterSettings>(
            () => CommitterSettings.Create(Sys));

        protected CommitterSettings CommitterSettings => CommitterDefaultsInstance.Value;

        private static int NextNumber => _topicCounter.IncrementAndGet();

        /// <summary>
        /// Returns a unique topic name
        /// </summary>
        protected string CreateTopicName(int suffix) => $"topic-{suffix}-{NextNumber}";

        /// <summary>
        /// Returns a unique group id with a default suffix
        /// </summary>
        protected string CreateGroupId() => CreateGroupId(0);

        /// <summary>
        /// Returns a unique group id with a given suffix
        /// </summary>
        protected string CreateGroupId(int suffix) => $"group-{suffix}-{NextNumber}";

        /// <summary>
        /// Returns a unique transactional id with a given suffix
        /// </summary>
        protected string CreateTransactionalId(int suffix) => $"transactionalId-{suffix}-{NextNumber}";


        protected abstract ActorSystem System { get; }
        protected abstract string BootstrapServers { get; }

        protected KafkaTestKitSettings Settings { get; }

        private Lazy<Dictionary<string, string>> AdminDefaults => new Lazy<Dictionary<string, string>>(
            () => new Dictionary<string, string>
            {
                ["bootstrap.servers"] = BootstrapServers
            });

        private IAdminClient _adminClient;

        protected IAdminClient AdminClient
        {
            get
            {
                if (_adminClient == null)
                    throw new Exception(
                        "Admin client not created, be sure to call SetupAdminClient() and CleanupAdminClient()");
                return _adminClient;
            }
        }

        /// <summary>
        /// Create internal admin clients.
        /// Gives access to `adminClient`,
        /// be sure to call `cleanUpAdminClient` after the tests are done.
        /// </summary>
        protected void SetupAdminClient()
        {
            if (_adminClient == null)
            {
                _adminClient = new AdminClientBuilder(AdminDefaults.Value).Build();
            }
        }

        /// <summary>
        /// Close internal admin client instances.
        /// </summary>
        protected void CleanupAdminClient()
        {
            _adminClient?.Dispose();
            _adminClient = null;
        }

        protected string CreateTopic()
            => CreateTopic(0, 1, 1, new Dictionary<string, string>());

        protected string CreateTopic(int suffix)
            => CreateTopic(suffix, 1, 1, new Dictionary<string, string>());

        protected string CreateTopic(int suffix, int partitions)
            => CreateTopic(suffix, partitions, 1, new Dictionary<string, string>());

        protected string CreateTopic(int suffix, int partitions, int replication)
            => CreateTopic(suffix, partitions, replication, new Dictionary<string, string>());

        protected string CreateTopic(
            int suffix,
            int partitions,
            int replication,
            Dictionary<string, string> config)
        {
            var topicName = CreateTopicName(suffix);
            AdminClient.CreateTopicsAsync(
                new[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,
                        NumPartitions = partitions,
                        ReplicationFactor = (short) replication,
                        Configs = config
                    }
                }).Wait(TimeSpan.FromSeconds(10));
            return topicName;
        }

        protected void SleepMillis(int ms, string msg)
        {
            Log.Debug($"Sleeping {ms} ms {msg}");
            Thread.Sleep(ms);
        }

        protected void SleepSeconds(int s, string msg)
        {
            Log.Debug($"Sleeping {s} s {msg}");
            Thread.Sleep(s * 1000);
        }
    }
}
