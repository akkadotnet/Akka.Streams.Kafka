using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Actor;
using Confluent.Kafka;
using Config = Hocon.Config;

namespace Akka.Streams.Kafka.Settings
{
    public sealed class ProducerSettings<TKey, TValue>
    {
        public ProducerSettings(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, int parallelism, 
                                string dispatcherId, TimeSpan flushTimeout, TimeSpan eosCommitInterval,
                                IImmutableDictionary<string, string> properties)
        {
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;
            Parallelism = parallelism;
            DispatcherId = dispatcherId;
            FlushTimeout = flushTimeout;
            EosCommitInterval = eosCommitInterval;
            Properties = properties;
        }

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

        public ProducerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers) =>
            WithProperty("bootstrap.servers", bootstrapServers);

        public ProducerSettings<TKey, TValue> WithProperty(string key, string value) =>
            Copy(properties: Properties.SetItem(key, value));
        
        /// <summary>
        /// The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
        /// </summary>
        public ProducerSettings<TKey, TValue> WithEosCommitInterval(TimeSpan eosCommitInterval) =>
            Copy(eosCommitInterval: eosCommitInterval);

        public ProducerSettings<TKey, TValue> WithParallelism(int parallelism) =>
            Copy(parallelism: parallelism);

        public ProducerSettings<TKey, TValue> WithDispatcher(string dispatcherId) =>
            Copy(dispatcherId: dispatcherId);

        private ProducerSettings<TKey, TValue> Copy(
            ISerializer<TKey> keySerializer = null,
            ISerializer<TValue> valueSerializer = null,
            int? parallelism = null,
            string dispatcherId = null,
            TimeSpan? flushTimeout = null,
            TimeSpan? eosCommitInterval = null,
            IImmutableDictionary<string, string> properties = null) =>
            new ProducerSettings<TKey, TValue>(
                keySerializer: keySerializer ?? this.KeySerializer,
                valueSerializer: valueSerializer ?? this.ValueSerializer,
                parallelism: parallelism ?? this.Parallelism,
                dispatcherId: dispatcherId ?? this.DispatcherId,
                flushTimeout: flushTimeout ?? this.FlushTimeout,
                eosCommitInterval: eosCommitInterval ?? this.EosCommitInterval,
                properties: properties ?? this.Properties);

        public static ProducerSettings<TKey, TValue> Create(ActorSystem system, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            if (system == null) throw new ArgumentNullException(nameof(system));

            var config = system.Settings.Config.GetConfig("akka.kafka.producer");
            return Create(config, keySerializer, valueSerializer);
        }

        public static ProducerSettings<TKey, TValue> Create(Config config, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            if (config == null) throw new ArgumentNullException(nameof(config), "Kafka config for Akka.NET producer was not provided");

            return new ProducerSettings<TKey, TValue>(
                keySerializer: keySerializer,
                valueSerializer: valueSerializer,
                parallelism: config.GetInt("parallelism", 100),
                dispatcherId: config.GetString("use-dispatcher", "akka.kafka.default-dispatcher"),
                flushTimeout: config.GetTimeSpan("flush-timeout", TimeSpan.FromSeconds(2)),
                eosCommitInterval: config.GetTimeSpan("eos-commit-interval", TimeSpan.FromMilliseconds(100)),
                properties: ImmutableDictionary<string, string>.Empty);
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
