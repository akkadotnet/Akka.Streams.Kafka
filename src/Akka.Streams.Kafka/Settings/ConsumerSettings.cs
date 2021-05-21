using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Settings
{
    /// <summary>
    /// Consumer settings
    /// </summary>
    /// <typeparam name="TKey">Message key type</typeparam>
    /// <typeparam name="TValue">Message value tyoe</typeparam>
    public sealed class ConsumerSettings<TKey, TValue>
    {
        /// <summary>
        /// Creates consumer settings
        /// </summary>
        /// <param name="system">Actor system for stage materialization</param>
        /// <param name="keyDeserializer">Key deserializer</param>
        /// <param name="valueDeserializer">Value deserializer</param>
        /// <returns>Consumer settings</returns>
        public static ConsumerSettings<TKey, TValue> Create(ActorSystem system, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            var config = system.Settings.Config.GetConfig("akka.kafka.consumer");
            return Create(config, keyDeserializer, valueDeserializer);
        }

        /// <summary>
        /// Creates consumer settings
        /// </summary>
        /// <param name="config">Config to load properties from</param>
        /// <param name="keyDeserializer">Key deserializer</param>
        /// <param name="valueDeserializer">Value deserializer</param>
        /// <returns>Consumer settings</returns>
        /// <exception cref="ArgumentNullException">Thrown when kafka config for Akka.NET is not provided</exception>
        public static ConsumerSettings<TKey, TValue> Create(Akka.Configuration.Config config, IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config), "Kafka config for Akka.NET consumer was not provided");

            return new ConsumerSettings<TKey, TValue>(
                keyDeserializer: keyDeserializer,
                valueDeserializer: valueDeserializer,
                pollInterval: config.GetTimeSpan("poll-interval", TimeSpan.FromMilliseconds(50)),
                pollTimeout: config.GetTimeSpan("poll-timeout", TimeSpan.FromMilliseconds(50)),
                partitionHandlerWarning: config.GetTimeSpan("partition-handler-warning", TimeSpan.FromSeconds(5)),
                commitTimeWarning: config.GetTimeSpan("commit-time-warning", TimeSpan.FromSeconds(1)),
                commitTimeout: config.GetTimeSpan("commit-timeout", TimeSpan.FromSeconds(15)),
                commitRefreshInterval: config.GetTimeSpan("commit-refresh-interval", Timeout.InfiniteTimeSpan, allowInfinite: true),
                stopTimeout: config.GetTimeSpan("stop-timeout", TimeSpan.FromSeconds(30)),
                positionTimeout: config.GetTimeSpan("position-timeout", TimeSpan.FromSeconds(5)),
                waitClosePartition: config.GetTimeSpan("wait-close-partition", TimeSpan.FromSeconds(1)),
                bufferSize: config.GetInt("buffer-size", 50),
                drainingCheckInterval: config.GetTimeSpan("eos-draining-check-interval", TimeSpan.FromMilliseconds(30)),
                dispatcherId: config.GetString("use-dispatcher", "akka.kafka.default-dispatcher"),
                autoCreateTopicsEnabled: config.GetBoolean("allow.auto.create.topics", true),
                properties: ImmutableDictionary<string, string>.Empty);
        }

        /// <summary>
        /// Gets property value by key
        /// </summary>
        public object this[string propertyKey] => this.Properties.GetValueOrDefault(propertyKey);

        /// <summary>
        /// Key deserializer
        /// </summary>
        public IDeserializer<TKey> KeyDeserializer { get; }
        /// <summary>
        /// Value deserializer
        /// </summary>
        public IDeserializer<TValue> ValueDeserializer { get; }
        /// <summary>
        /// Set the interval from one scheduled poll to the next.
        /// </summary>
        public TimeSpan PollInterval { get; }
        /// <summary>
        /// Set the maximum duration a poll to the Kafka broker is allowed to take.
        /// </summary>
        public TimeSpan PollTimeout { get; }
        /// <summary>
        /// When partition assigned events handling takes more then this timeout, the warning will be logged
        /// </summary>
        public TimeSpan PartitionHandlerWarning { get; }
        /// <summary>
        /// Time to wait for pending requests when a partition is closed.
        /// </summary>
        public TimeSpan WaitClosePartition { get; }
        /// <summary>
        /// When offset committing takes more then this timeout, the warning will be logged
        /// </summary>
        public TimeSpan CommitTimeWarning { get; }
        /// <summary>
        /// If offset commit requests are not completed within this timeout <see cref="CommitTimeoutException"/> will be thrown
        /// </summary>
        public TimeSpan CommitTimeout { get; }
        /// <summary>
        /// If set to a finite duration, the consumer will re-send the last committed offsets periodically for all assigned partitions.
        /// Set it to TimeSpan.Zero to switch it off
        /// </summary>
        public TimeSpan CommitRefreshInterval { get; }
        /// <summary>
        /// Check interval for TransactionalProducer when finishing transaction before shutting down consumer
        /// </summary>
        public TimeSpan DrainingCheckInterval { get; }
        /// <summary>
        /// The stage will await outstanding offset commit requests before shutting down,
        /// but if that takes longer than this timeout it will stop forcefully.
        /// </summary>
        public TimeSpan StopTimeout { get; }
        /// <summary>
        /// Limits the blocking on Kafka consumer position calls
        /// </summary>
        public TimeSpan PositionTimeout { get; }
        public int BufferSize { get; }
        /// <summary>
        /// Fully qualified config path which holds the dispatcher configuration to be used by the consuming actor. Some blocking may occur.
        /// </summary>
        public string DispatcherId { get; }
        /// <summary>
        /// Allow automatic topic creation on the broker when subscribing to or assigning a topic.
        /// </summary>
        /// <remarks>
        /// See more here: https://kafka.apache.org/documentation/#allow.auto.create.topics
        /// Additionally, due to https://github.com/confluentinc/confluent-kafka-dotnet/issues/1366 ,
        /// when set to `true` and topic is not created by Confluent driver, consuming error will be ignored
        /// (like if no message to consume)
        /// </remarks>
        public bool AutoCreateTopicsEnabled { get; }
        /// <summary>
        /// Configuration properties
        /// </summary>
        public IImmutableDictionary<string, string> Properties { get; }

        public ConsumerSettings(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer, TimeSpan pollInterval,
                                TimeSpan pollTimeout, TimeSpan commitTimeout, TimeSpan commitRefreshInterval, TimeSpan stopTimeout,
                                TimeSpan positionTimeout, TimeSpan commitTimeWarning, TimeSpan partitionHandlerWarning,
                                TimeSpan waitClosePartition, TimeSpan drainingCheckInterval, bool autoCreateTopicsEnabled,
                                int bufferSize, string dispatcherId, IImmutableDictionary<string, string> properties)
        {
            KeyDeserializer = keyDeserializer;
            ValueDeserializer = valueDeserializer;
            PollInterval = pollInterval;
            PollTimeout = pollTimeout;
            PositionTimeout = positionTimeout;
            StopTimeout = stopTimeout;
            PartitionHandlerWarning = partitionHandlerWarning;
            CommitTimeWarning = commitTimeWarning;
            CommitTimeout = commitTimeout;
            CommitRefreshInterval = commitRefreshInterval;
            BufferSize = bufferSize;
            DispatcherId = dispatcherId;
            Properties = properties;
            WaitClosePartition = waitClosePartition;
            DrainingCheckInterval = drainingCheckInterval;
            AutoCreateTopicsEnabled = autoCreateTopicsEnabled;
        }

        /// <summary>
        /// Sets kafka server IPs
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers) =>
            Copy(properties: Properties.SetItem("bootstrap.servers", bootstrapServers));

        /// <summary>
        /// Sets client id to be used
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithClientId(string clientId) =>
            Copy(properties: Properties.SetItem("client.id", clientId));

        /// <summary>
        /// Sets consumer group Id
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithGroupId(string groupId) =>
            Copy(properties: Properties.SetItem("group.id", groupId));

        /// <summary>
        /// Sets property with given key to specified value
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithProperty(string key, string value) =>
            Copy(properties: Properties.SetItem(key, value));

        public ConsumerSettings<TKey, TValue> WithProperties(IDictionary<string, string> properties)
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
        /// Set the interval from one scheduled poll to the next.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPollInterval(TimeSpan pollInterval) => Copy(pollInterval: pollInterval);
        /// <summary>
        /// Set the maximum duration a poll to the Kafka broker is allowed to take.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPollTimeout(TimeSpan pollTimeout) => Copy(pollTimeout: pollTimeout);
        /// <summary>
        /// If offset commit requests are not completed within this timeout <see cref="CommitTimeoutException"/> will be thrown
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCommitTimeout(TimeSpan commitTimeout) => Copy(commitTimeout: commitTimeout);
        /// <summary>
        /// If commits take longer than this time a warning is logged
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCommitTimeWarning(TimeSpan commitTimeWarning) => Copy(commitTimeWarning: commitTimeWarning);
        /// <summary>
        /// When partition assigned events handling takes more then this timeout, the warning will be logged
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPartitionHandlerWarning(TimeSpan partitionHandlerWarning) => Copy(partitionHandlerWarning: partitionHandlerWarning);
        /// <summary>
        /// Time to wait for pending requests when a partition is closed.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithWaitClosePartition(TimeSpan waitClosePartition) => Copy(waitClosePartition: waitClosePartition);
        /// <summary>
        /// Allows topic auto-creation when constumer is subscribing or assigning to the topic.
        /// </summary>
        /// <remarks>
        /// When set, and still getting error from broker, consumer will assume that no message was produced yet
        /// </remarks>
        public ConsumerSettings<TKey, TValue> WithAutoCreateTopicsEnabled(bool autoCreateTopicsEnabled) => Copy(autoCreateTopicsEnabled: autoCreateTopicsEnabled);

        /// <summary>
        /// If set to a finite duration, the consumer will re-send the last committed offsets periodically for all assigned partitions.
        /// Set it to TimeSpan.Zero to switch it off
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCommitRefreshInterval(TimeSpan commitRefreshInterval)
        {
            return Copy(commitRefreshInterval: commitRefreshInterval == TimeSpan.Zero ? Timeout.InfiniteTimeSpan : commitRefreshInterval);
        }

        /// <summary>
        /// The stage will await outstanding offset commit requests before shutting down,
        /// but if that takes longer than this timeout it will stop forcefully.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithStopTimeout(TimeSpan stopTimeout) => Copy(stopTimeout: stopTimeout);

        /// <summary>
        ///  Limits the blocking on Kafka consumer position calls.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPositionTimeout(TimeSpan positionTimeout) => Copy(positionTimeout: positionTimeout);

        /// <summary>
        /// Fully qualified config path which holds the dispatcher configuration to be used by the consuming actor. Some blocking may occur.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithDispatcher(string dispatcherId) => Copy(dispatcherId: dispatcherId);

        /// <summary>
        /// Check interval for TransactionalProducer when finishing transaction before shutting down consumer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithDrainingCheckInterval(TimeSpan drainingCheckInterval) => Copy(drainingCheckInterval: drainingCheckInterval);

        /// <summary>
        /// Sets key deserializer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> keyDeserializer) => Copy(keyDeserializer: keyDeserializer);

        /// <summary>
        /// Sets value deserializer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> valueDeserializer) => Copy(valueDeserializer: valueDeserializer);


        /// <summary>
        /// Assigned consumer group Id, or null
        /// </summary>
        public string GroupId => Properties.ContainsKey("group.id") ? Properties["group.id"] : null;

        private ConsumerSettings<TKey, TValue> Copy(
            IDeserializer<TKey> keyDeserializer = null,
            IDeserializer<TValue> valueDeserializer = null,
            TimeSpan? pollInterval = null,
            TimeSpan? pollTimeout = null,
            TimeSpan? commitTimeout = null,
            TimeSpan? partitionHandlerWarning = null,
            TimeSpan? drainingCheckInterval = null,
            TimeSpan? commitTimeWarning = null,
            TimeSpan? commitRefreshInterval = null,
            TimeSpan? stopTimeout = null,
            TimeSpan? positionTimeout = null,
            TimeSpan? waitClosePartition = null,
            bool? autoCreateTopicsEnabled = null,
            int? bufferSize = null,
            string dispatcherId = null,
            IImmutableDictionary<string, string> properties = null) =>
            new ConsumerSettings<TKey, TValue>(
                keyDeserializer: keyDeserializer ?? this.KeyDeserializer,
                valueDeserializer: valueDeserializer ?? this.ValueDeserializer,
                pollInterval: pollInterval ?? this.PollInterval,
                pollTimeout: pollTimeout ?? this.PollTimeout,
                commitTimeout: commitTimeout ?? this.CommitTimeout,
                partitionHandlerWarning: partitionHandlerWarning ?? this.PartitionHandlerWarning,
                commitTimeWarning: commitTimeWarning ?? this.CommitTimeWarning,
                commitRefreshInterval: commitRefreshInterval ?? this.CommitRefreshInterval,
                stopTimeout: stopTimeout ?? this.StopTimeout,
                waitClosePartition: waitClosePartition ?? this.WaitClosePartition,
                positionTimeout: positionTimeout ?? this.PositionTimeout,
                bufferSize: bufferSize ?? this.BufferSize,
                drainingCheckInterval: drainingCheckInterval ?? this.DrainingCheckInterval,
                dispatcherId: dispatcherId ?? this.DispatcherId,
                autoCreateTopicsEnabled: autoCreateTopicsEnabled ?? this.AutoCreateTopicsEnabled,
                properties: properties ?? this.Properties);

        /// <summary>
        /// Creates new kafka consumer, using event handlers provided
        /// </summary>
        public Confluent.Kafka.IConsumer<TKey, TValue> CreateKafkaConsumer(Action<IConsumer<TKey, TValue>, Error> consumeErrorHandler = null,
                                                                           Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionAssignedHandler = null,
                                                                           Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionRevokedHandler = null,
                                                                           Action<IConsumer<TKey, TValue>, string> statisticHandler = null)
        {
            return new Confluent.Kafka.ConsumerBuilder<TKey, TValue>(this.Properties)
                .SetKeyDeserializer(this.KeyDeserializer)
                .SetValueDeserializer(this.ValueDeserializer)
                .SetErrorHandler((c, e) => consumeErrorHandler?.Invoke(c, e))
                .SetPartitionsAssignedHandler((c, partitions) => partitionAssignedHandler?.Invoke(c, partitions))
                .SetPartitionsRevokedHandler((c, partitions) => partitionRevokedHandler?.Invoke(c, partitions))
                .SetStatisticsHandler((c, json) => statisticHandler?.Invoke(c, json))
                .Build();
        }
    }
}
