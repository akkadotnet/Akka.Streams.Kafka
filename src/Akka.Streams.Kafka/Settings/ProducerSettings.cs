using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Internal;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Settings
{
    public sealed class ProducerSettings<TKey, TValue>
    {
        [Obsolete("Use the ctor with enrichAsync and producerFactory instead")]
        public ProducerSettings(
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer,
            int parallelism, 
            string dispatcherId,
            TimeSpan flushTimeout,
            TimeSpan eosCommitInterval,
            IImmutableDictionary<string, string> properties)
        : this(keySerializer, valueSerializer, parallelism, dispatcherId, flushTimeout, flushTimeout, properties, null, null, true)
        {}
        
        public ProducerSettings(
            ISerializer<TKey> keySerializer,
            ISerializer<TValue> valueSerializer,
            int parallelism, 
            string dispatcherId,
            TimeSpan flushTimeout,
            TimeSpan eosCommitInterval,
            IImmutableDictionary<string, string> properties,
            Func<ProducerSettings<TKey, TValue>, Task<ProducerSettings<TKey, TValue>>> enrichAsync,
            Func<ProducerSettings<TKey, TValue>, IProducer<TKey, TValue>> producerFactory,
            bool closeProducerOnStop)
        {
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;
            Parallelism = parallelism;
            DispatcherId = dispatcherId;
            FlushTimeout = flushTimeout;
            EosCommitInterval = eosCommitInterval;
            Properties = properties;
            EnrichAsync = enrichAsync;
            ProducerFactory = producerFactory;
            CloseProducerOnStop = closeProducerOnStop;
        }

        public Func<ProducerSettings<TKey, TValue>, Task<ProducerSettings<TKey, TValue>>> EnrichAsync { get; }
        
        [Obsolete("Use CreateKafkaProducer(), CreateKafkaProducerAsync(), or CreateKafkaProducerCompletionStage() to get a new Kafka IProducer")]
        public Func<ProducerSettings<TKey, TValue>, IProducer<TKey, TValue>> ProducerFactory { get; }
        
        public bool CloseProducerOnStop { get; }
        
        public ISerializer<TKey> KeySerializer { get; }
        public ISerializer<TValue> ValueSerializer { get; }
        public int Parallelism { get; }
        public string DispatcherId { get; }
        public TimeSpan FlushTimeout { get; }
        /// <summary>
        /// The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
        /// </summary>
        public TimeSpan EosCommitInterval { get; }
        public IImmutableDictionary<string, string> Properties { get; }

        /// <summary>
        /// Gets property value by key
        /// </summary>
        public object this[string propertyKey] => this.Properties.GetValueOrDefault(propertyKey);
        
        public string GetProperty(string key) => Properties.GetValueOrDefault(key, null);
        
        public ProducerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers) =>
            WithProperty("bootstrap.servers", bootstrapServers);

        public ProducerSettings<TKey, TValue> WithProperty(string key, string value) =>
            Copy(properties: Properties.SetItem(key, value));

        public ProducerSettings<TKey, TValue> WithProducerConfig(ProducerConfig config)
            => WithProperties(config);
        
        public ProducerSettings<TKey, TValue> WithProperties(IEnumerable<KeyValuePair<string, string>> properties)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, string>();
            builder.AddRange(Properties);
            foreach (var kvp in properties)
            {
                builder.AddOrSet(kvp.Key, kvp.Value);
            }
            return Copy(properties: builder.ToImmutable());
        }   
        
        /// <summary>
        /// The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
        /// </summary>
        public ProducerSettings<TKey, TValue> WithEosCommitInterval(TimeSpan eosCommitInterval) =>
            Copy(eosCommitInterval: eosCommitInterval);

        public ProducerSettings<TKey, TValue> WithParallelism(int parallelism) =>
            Copy(parallelism: parallelism);

        public ProducerSettings<TKey, TValue> WithDispatcher(string dispatcherId) =>
            Copy(dispatcherId: dispatcherId);

        public ProducerSettings<TKey, TValue> WithCloseProducerOnStop(bool closeProducerOnStop) =>
            Copy(closeProducerOnStop: closeProducerOnStop);
        
        public ProducerSettings<TKey, TValue> WithEnrichAsync(Func<ProducerSettings<TKey, TValue>, Task<ProducerSettings<TKey, TValue>>> enrichAsync) =>
            Copy(enrichAsync: enrichAsync);
        
        public ProducerSettings<TKey, TValue> WithProducerFactory(Func<ProducerSettings<TKey, TValue>, IProducer<TKey, TValue>> producerFactory) =>
            Copy(producerFactory: producerFactory);
        
        public ProducerSettings<TKey, TValue> WithProducer(IProducer<TKey, TValue> producer) =>
            Copy(producerFactory: _ => producer, closeProducerOnStop: false );
        
        private ProducerSettings<TKey, TValue> Copy(
            ISerializer<TKey> keySerializer = null,
            ISerializer<TValue> valueSerializer = null,
            int? parallelism = null,
            string dispatcherId = null,
            TimeSpan? flushTimeout = null,
            TimeSpan? eosCommitInterval = null,
            IImmutableDictionary<string, string> properties = null,
            Func<ProducerSettings<TKey, TValue>, Task<ProducerSettings<TKey, TValue>>> enrichAsync = null,
            Func<ProducerSettings<TKey, TValue>, IProducer<TKey, TValue>> producerFactory = null,
            bool? closeProducerOnStop = null) =>
            new ProducerSettings<TKey, TValue>(
                keySerializer: keySerializer ?? KeySerializer,
                valueSerializer: valueSerializer ?? ValueSerializer,
                parallelism: parallelism ?? Parallelism,
                dispatcherId: dispatcherId ?? DispatcherId,
                flushTimeout: flushTimeout ?? FlushTimeout,
                eosCommitInterval: eosCommitInterval ?? EosCommitInterval,
                properties: properties ?? Properties,
                enrichAsync: enrichAsync ?? EnrichAsync,
                producerFactory: producerFactory ?? ProducerFactory,
                closeProducerOnStop: closeProducerOnStop ?? CloseProducerOnStop);

        public const string ConfigPath = "akka.kafka.producer";
        
        public static ProducerSettings<TKey, TValue> Create(ActorSystem system, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            if (system == null) throw new ArgumentNullException(nameof(system));
            return Create(system.Settings.Config.GetConfig(ConfigPath), keySerializer, valueSerializer);
        }

        public static ProducerSettings<TKey, TValue> Create(Akka.Configuration.Config config, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            if (config == null) throw new ArgumentNullException(nameof(config), "Kafka config for Akka.NET producer was not provided");
            
            var properties = config.GetConfig("kafka-clients").ParseKafkaClientsProperties();

            return new ProducerSettings<TKey, TValue>(
                keySerializer: keySerializer,
                valueSerializer: valueSerializer,
                parallelism: config.GetInt("parallelism", 100),
                dispatcherId: config.GetString("use-dispatcher", "akka.kafka.default-dispatcher"),
                flushTimeout: config.GetTimeSpan("flush-timeout", TimeSpan.FromSeconds(2)),
                eosCommitInterval: config.GetTimeSpan("eos-commit-interval", TimeSpan.FromMilliseconds(100)),
                properties: properties,
                enrichAsync: null,
                producerFactory: null,
                closeProducerOnStop: config.GetBoolean("close-on-producer-stop", false));
        }

        public Confluent.Kafka.IProducer<TKey, TValue> CreateKafkaProducer(Action<IProducer<TKey, TValue>, Error> producerErrorHandler = null)
        {
            return new Confluent.Kafka.ProducerBuilder<TKey, TValue>(Properties)
                .SetKeySerializer(KeySerializer)
                .SetValueSerializer(ValueSerializer)
                .SetErrorHandler((p, error) => producerErrorHandler?.Invoke(p, error))
                .Build();
        }
    }
}
