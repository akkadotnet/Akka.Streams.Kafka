using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Configuration;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Testkit.Internal
{
    public abstract class KafkaTestKit : Akka.TestKit.Xunit2.TestKit
    {
        public new static Config DefaultConfig { get; } = 
            ConfigurationFactory.FromResource<KafkaTestKit>("Akka.Streams.Kafka.Testkit.Resources.reference.conf");
        
        private static readonly AtomicCounter TopicCounter = new AtomicCounter(); 
        
        public const string DefaultKey = "key";
        
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

        private readonly Lazy<CommitterSettings> _committerDefaultsInstance;
        protected CommitterSettings CommitterDefaults => _committerDefaultsInstance.Value;

        private int NextNumber => TopicCounter.IncrementAndGet();
        
        private string Uuid { get; } = Guid.NewGuid().ToString();
        
        /// <summary>
        /// Return a unique topic name
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        protected string CreateTopicName(int suffix) => $"topic-{suffix}-{Uuid}";
        
        /// <summary>
        /// Return a unique group id with a given suffix
        /// </summary>
        /// <param name="suffix"></param>
        /// <returns></returns>
        protected string CreateGroupId(int suffix) => $"group-{suffix}-{Uuid}";

        protected string CreateTransactionalId(int suffix) => $"transactionalId-{suffix}-{Uuid}";
        
        protected KafkaTestkitSettings Settings { get; }
        
        private readonly Lazy<Dictionary<string, string>> _adminDefaults;
        private IAdminClient _adminClient;
        protected IAdminClient AdminClient
        {
            get
            {
                if (_adminClient == null)
                    throw new Exception(
                        "admin client not created, be sure to call setupAdminClient() and cleanupAdminClient()");
                return _adminClient;
            }
        }

        protected void SetUpAdminClient()
        {
            if (_adminClient == null)
                _adminClient = new AdminClientBuilder(_adminDefaults.Value).Build();
        }

        protected void CleanUpAdminClient()
        {
            _adminClient?.Dispose();
            _adminClient = null;
        }
        
        protected Task<string> CreateTopic()
            => CreateTopic(0, 1, 1, new Dictionary<string, string>(), new CreateTopicsOptions());
        
        protected Task<string> CreateTopic(int suffix)
            => CreateTopic(suffix, 1, 1, new Dictionary<string, string>(), new CreateTopicsOptions());
        
        protected Task<string> CreateTopic(int suffix, int partitions)
            => CreateTopic(suffix, partitions, 1, new Dictionary<string, string>(), new CreateTopicsOptions());
        protected Task<string> CreateTopic(int suffix, int partitions, int replication)
            => CreateTopic(suffix, partitions, replication, new Dictionary<string, string>(), new CreateTopicsOptions());
        
        protected Task<string> CreateTopic(int suffix, int partitions, int replication, Dictionary<string, string> config)
            => CreateTopic(suffix, partitions, replication, config, new CreateTopicsOptions());

        protected async Task<string> CreateTopic(int suffix, int partitions, int replication, Dictionary<string, string> config, CreateTopicsOptions options)
        {
            var topicName = CreateTopicName(suffix);
            await _adminClient.CreateTopicsAsync(new[] {new TopicSpecification
            {
                Name = topicName,
                NumPartitions = partitions,
                ReplicationFactor = (short)replication, 
                Configs = config
            }}, options);
            
            return topicName;
        }

        protected void SleepMillis(int ms, string msg)
        {
            Log.Debug($"Sleeping {ms} ms: {msg}");
            Thread.Sleep(ms);
        }

        protected void SleepSeconds(int s, string msg)
        {
            Log.Debug($"Sleeping {s} s: {msg}");
            Thread.Sleep(s * 1000);
        }

        protected abstract string BootstrapServers { get; }

        
        private static Config Config()
        {
            //var config = ConfigurationFactory.ParseString("akka.loglevel = DEBUG");
            return ConfigurationFactory.ParseString("akka{}")
                .WithFallback(DefaultConfig)
                .WithFallback(KafkaExtensions.DefaultSettings);
        }
        
        protected KafkaTestKit(string config, string actorSystemName = null, ITestOutputHelper output = null) 
            : this(config != null ? ConfigurationFactory.ParseString(config) : null, 
                actorSystemName,
                output)
        { }

        protected KafkaTestKit(Config config, string actorSystemName = null, ITestOutputHelper output = null)
            : this(
                config != null ? ActorSystemSetup.Empty.WithSetup(BootstrapSetup.Create().WithConfig(config)) : null, 
                actorSystemName, 
                output)
        { }

        protected KafkaTestKit(ActorSystemSetup setup, string actorSystemName = null, ITestOutputHelper output = null)
            : base(setup ?? ActorSystemSetup.Empty, actorSystemName, output)
        {
            Sys.Settings.InjectTopLevelFallback(Config());
            Settings = new KafkaTestkitSettings(Sys);
            _committerDefaultsInstance = new Lazy<CommitterSettings>(() => CommitterSettings.Create(Sys));
            _adminDefaults = new Lazy<Dictionary<string, string>>(() => new Dictionary<string, string>
            {
                ["bootstrap.servers"] = BootstrapServers
            });
        }

    }
}