using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Actor;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Settings
{
    public sealed class ConsumerSettings<TKey, TValue>
    {
        public static ConsumerSettings<TKey, TValue> Create(ActorSystem system, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            var config = system.Settings.Config.GetConfig("akka.kafka.consumer");
            return Create(config, keyDeserializer, valueDeserializer);
        }

        public static ConsumerSettings<TKey, TValue> Create(Config config, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            if (config == null) throw new ArgumentNullException(nameof(config), "Kafka config for Akka.NET consumer was not provided");

            return new ConsumerSettings<TKey, TValue>(
                keyDeserializer: keyDeserializer,
                valueDeserializer: valueDeserializer,
                pollInterval: config.GetTimeSpan("poll-interval", TimeSpan.FromMilliseconds(50)),
                pollTimeout: config.GetTimeSpan("poll-timeout", TimeSpan.FromMilliseconds(50)),
                commitTimeout: config.GetTimeSpan("commit-timeout", TimeSpan.FromMilliseconds(50)),
                stopTimeout: config.GetTimeSpan("stop-timeout", TimeSpan.FromMilliseconds(50)),
                bufferSize: config.GetInt("buffer-size", 50),
                dispatcherId: config.GetString("use-dispatcher", "akka.kafka.default-dispatcher"),
                properties: ImmutableDictionary<string, string>.Empty);
        }

        public object this[string propertyKey] => this.Properties.GetValueOrDefault(propertyKey);

        public IDeserializer<TKey> KeyDeserializer { get; }
        public IDeserializer<TValue> ValueDeserializer { get; }
        public TimeSpan PollInterval { get; }
        public TimeSpan PollTimeout { get; }
        public TimeSpan CommitTimeout { get; }
        public TimeSpan StopTimeout { get; }
        public int BufferSize { get; }
        public string DispatcherId { get; }
        public IImmutableDictionary<string, string> Properties { get; }

        public ConsumerSettings(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer, TimeSpan pollInterval, 
                                TimeSpan pollTimeout, TimeSpan commitTimeout, TimeSpan stopTimeout, int bufferSize, 
                                string dispatcherId, IImmutableDictionary<string, string> properties)
        {
            KeyDeserializer = keyDeserializer;
            ValueDeserializer = valueDeserializer;
            PollInterval = pollInterval;
            PollTimeout = pollTimeout;
            StopTimeout = stopTimeout;
            CommitTimeout = commitTimeout;
            BufferSize = bufferSize;
            DispatcherId = dispatcherId;
            Properties = properties;
        }

        public ConsumerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers) =>
            Copy(properties: Properties.SetItem("bootstrap.servers", bootstrapServers));

        public ConsumerSettings<TKey, TValue> WithClientId(string clientId) =>
            Copy(properties: Properties.SetItem("client.id", clientId));

        public ConsumerSettings<TKey, TValue> WithGroupId(string groupId) =>
            Copy(properties: Properties.SetItem("group.id", groupId));

        public ConsumerSettings<TKey, TValue> WithProperty(string key, string value) =>
            Copy(properties: Properties.SetItem(key, value));

        public ConsumerSettings<TKey, TValue> WithPollInterval(TimeSpan pollInterval) => Copy(pollInterval: pollInterval);

        public ConsumerSettings<TKey, TValue> WithPollTimeout(TimeSpan pollTimeout) => Copy(pollTimeout: pollTimeout);
        
        public ConsumerSettings<TKey, TValue> WithCommitTimeout(TimeSpan commitTimeout) => Copy(commitTimeout: commitTimeout);
        public ConsumerSettings<TKey, TValue> WithStopTimeout(TimeSpan stopTimeout) => Copy(stopTimeout: stopTimeout);

        public ConsumerSettings<TKey, TValue> WithDispatcher(string dispatcherId) => Copy(dispatcherId: dispatcherId);
        
        public string GroupId => Properties.ContainsKey("group.id") ? Properties["group.id"] : null;

        private ConsumerSettings<TKey, TValue> Copy(
            IDeserializer<TKey> keyDeserializer = null,
            IDeserializer<TValue> valueDeserializer = null,
            TimeSpan? pollInterval = null,
            TimeSpan? pollTimeout = null,
            TimeSpan? commitTimeout = null,
            TimeSpan? stopTimeout = null,
            int? bufferSize = null,
            string dispatcherId = null,
            IImmutableDictionary<string, string> properties = null) =>
            new ConsumerSettings<TKey, TValue>(
                keyDeserializer: keyDeserializer ?? this.KeyDeserializer,
                valueDeserializer: valueDeserializer ?? this.ValueDeserializer,
                pollInterval: pollInterval ?? this.PollInterval,
                pollTimeout: pollTimeout ?? this.PollTimeout,
                commitTimeout: commitTimeout ?? this.CommitTimeout,
                stopTimeout: stopTimeout ?? this.StopTimeout,
                bufferSize: bufferSize ?? this.BufferSize,
                dispatcherId: dispatcherId ?? this.DispatcherId,
                properties: properties ?? this.Properties);

        public Confluent.Kafka.IConsumer<TKey, TValue> CreateKafkaConsumer(Action<IConsumer<TKey, TValue>, Error> consumeErrorHandler = null,
                                                                           Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionAssignedHandler = null,
                                                                           Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionRevokedHandler = null)
        { 
            return new Confluent.Kafka.ConsumerBuilder<TKey, TValue>(this.Properties)
                .SetKeyDeserializer(this.KeyDeserializer)
                .SetValueDeserializer(this.ValueDeserializer)
                .SetErrorHandler((c, e) => consumeErrorHandler?.Invoke(c, e))
                .SetPartitionsAssignedHandler((c, partitions) => partitionAssignedHandler?.Invoke(c, partitions))
                .SetPartitionsRevokedHandler((c, partitions) => partitionRevokedHandler?.Invoke(c, partitions))
                .Build();
        }
    }
}
