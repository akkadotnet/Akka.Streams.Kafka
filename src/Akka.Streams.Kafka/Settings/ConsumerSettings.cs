using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.Streams.Kafka.Internal;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Util.Internal;
using Confluent.Kafka;
using Newtonsoft.Json;
using Error = Confluent.Kafka.Error;

namespace Akka.Streams.Kafka.Settings
{
    public static class ConsumerSettings
    {
        internal const string ConfigPath = "akka.kafka.consumer";
    }

    /// <summary>
    /// Consumer settings
    /// </summary>
    /// <typeparam name="TKey">Message key type</typeparam>
    /// <typeparam name="TValue">Message value tyoe</typeparam>
    public sealed record ConsumerSettings<TKey, TValue>
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
            if (config == null) throw new ArgumentNullException(nameof(config), "Kafka config for Akka.NET consumer was not provided");
            
            var properties = config.GetConfig("kafka-clients").ParseKafkaClientsProperties();

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
                metadataRequestTimeout: config.GetTimeSpan("metadata-request-timeout", TimeSpan.FromSeconds(5)),
                drainingCheckInterval: config.GetTimeSpan("eos-draining-check-interval", TimeSpan.FromMilliseconds(30)),
                dispatcherId: config.GetString("use-dispatcher", "akka.kafka.default-dispatcher"),
                autoCreateTopicsEnabled: config.GetBoolean("allow.auto.create.topics", true),
                properties: properties,
                connectionCheckerSettings: ConnectionCheckerSettings.Create(config.GetConfig(ConnectionCheckerSettings.ConfigPath)),
                consumerFactory: null );
        }

        /// <summary>
        /// Gets property value by key
        /// </summary>
        public object? this[string propertyKey] => this.Properties.GetValueOrDefault(propertyKey);

        /// <summary>
        /// Key deserializer
        /// </summary>
        public IDeserializer<TKey> KeyDeserializer { get; init; } = null!;
        /// <summary>
        /// Value deserializer
        /// </summary>
        public IDeserializer<TValue> ValueDeserializer { get; init; } = null!;
        /// <summary>
        /// Set the interval from one scheduled poll to the next.
        /// </summary>
        public TimeSpan PollInterval { get; init; }
        /// <summary>
        /// Set the maximum duration a poll to the Kafka broker is allowed to take.
        /// </summary>
        public TimeSpan PollTimeout { get; init; }
        /// <summary>
        /// When partition assigned events handling takes more then this timeout, the warning will be logged
        /// </summary>
        public TimeSpan PartitionHandlerWarning { get; init; }
        /// <summary>
        /// Time to wait for pending requests when a partition is closed.
        /// </summary>
        public TimeSpan WaitClosePartition { get; init; }
        /// <summary>
        /// When offset committing takes more then this timeout, the warning will be logged
        /// </summary>
        public TimeSpan CommitTimeWarning { get; init; }
        /// <summary>
        /// If offset commit requests are not completed within this timeout <see cref="CommitTimeoutException"/> will be thrown
        /// </summary>
        public TimeSpan CommitTimeout { get; init; }
        /// <summary>
        /// If set to a finite duration, the consumer will re-send the last committed offsets periodically for all assigned partitions.
        /// Set it to TimeSpan.Zero to switch it off
        /// </summary>
        public TimeSpan CommitRefreshInterval { get; init; }
        /// <summary>
        /// Check interval for TransactionalProducer when finishing transaction before shutting down consumer
        /// </summary>
        public TimeSpan DrainingCheckInterval { get; init; }
        /// <summary>
        /// The stage will await outstanding offset commit requests before shutting down,
        /// but if that takes longer than this timeout it will stop forcefully.
        /// </summary>
        public TimeSpan StopTimeout { get; init; }
        /// <summary>
        /// Limits the blocking on Kafka consumer position calls
        /// </summary>
        public TimeSpan PositionTimeout { get; init; }
        public int BufferSize { get; init; }
        /// <summary>
        /// Fully qualified config path which holds the dispatcher configuration to be used by the consuming actor. Some blocking may occur.
        /// </summary>
        public string DispatcherId { get; init; } = null!;
        /// <summary>
        /// Allow automatic topic creation on the broker when subscribing to or assigning a topic.
        /// </summary>
        /// <remarks>
        /// See more here: https://kafka.apache.org/documentation/#allow.auto.create.topics
        /// Additionally, due to https://github.com/confluentinc/confluent-kafka-dotnet/issues/1366 ,
        /// when set to `true` and topic is not created by Confluent driver, consuming error will be ignored
        /// (like if no message to consume)
        /// </remarks>
        public bool AutoCreateTopicsEnabled { get; init; }
        /// <summary>
        /// Configuration properties
        /// </summary>
        public IImmutableDictionary<string, string> Properties { get; init; } = null!;

        public TimeSpan MetadataRequestTimeout { get; init; }

        public ConnectionCheckerSettings ConnectionCheckerSettings { get; init; } = null!;
        
        [JsonIgnore]
        public Func<ConsumerSettings<TKey, TValue>, IConsumer<TKey, TValue>>? ConsumerFactory { get; init; }

        [Obsolete("Please use ctor with consumerFactory parameter")]
        public ConsumerSettings(
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer,
            TimeSpan pollInterval,
            TimeSpan pollTimeout,
            TimeSpan commitTimeout,
            TimeSpan commitRefreshInterval,
            TimeSpan stopTimeout,
            TimeSpan positionTimeout,
            TimeSpan commitTimeWarning,
            TimeSpan partitionHandlerWarning,
            TimeSpan waitClosePartition,
            TimeSpan metadataRequestTimeout,
            TimeSpan drainingCheckInterval,
            bool autoCreateTopicsEnabled,
            int bufferSize, string dispatcherId,
            IImmutableDictionary<string, string> properties,
            ConnectionCheckerSettings connectionCheckerSettings)
            => new ConsumerSettings<TKey, TValue>(
                keyDeserializer, valueDeserializer, pollInterval, pollTimeout, commitTimeout, commitRefreshInterval,
                stopTimeout, positionTimeout, commitTimeWarning, partitionHandlerWarning, waitClosePartition,
                metadataRequestTimeout, drainingCheckInterval, autoCreateTopicsEnabled, bufferSize, dispatcherId,
                properties, connectionCheckerSettings, null);

        public ConsumerSettings(
            IDeserializer<TKey> keyDeserializer, 
            IDeserializer<TValue> valueDeserializer, 
            TimeSpan pollInterval, 
            TimeSpan pollTimeout, 
            TimeSpan commitTimeout, 
            TimeSpan commitRefreshInterval, 
            TimeSpan stopTimeout, 
            TimeSpan positionTimeout, 
            TimeSpan commitTimeWarning, 
            TimeSpan partitionHandlerWarning,
            TimeSpan waitClosePartition, 
            TimeSpan metadataRequestTimeout,
            TimeSpan drainingCheckInterval, 
            bool autoCreateTopicsEnabled,
            int bufferSize, string dispatcherId, 
            IImmutableDictionary<string, string> properties,
            ConnectionCheckerSettings connectionCheckerSettings,
            Func<ConsumerSettings<TKey, TValue>, IConsumer<TKey, TValue>>? consumerFactory = null)
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
            MetadataRequestTimeout = metadataRequestTimeout;
            DrainingCheckInterval = drainingCheckInterval;
            AutoCreateTopicsEnabled = autoCreateTopicsEnabled;
            ConnectionCheckerSettings = connectionCheckerSettings;
            ConsumerFactory = consumerFactory;
        }

        public string? GetProperty(string key) => Properties.GetValueOrDefault(key);

        /// <summary>
        /// Sets kafka server IPs
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers) =>
            this with { Properties = Properties.SetItem("bootstrap.servers", bootstrapServers) };

        /// <summary>
        /// Sets client id to be used
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithClientId(string clientId) =>
            this with { Properties = Properties.SetItem("client.id", clientId) };

        /// <summary>
        /// Sets consumer group Id
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithGroupId(string groupId) =>
            this with { Properties = Properties.SetItem("group.id", groupId) };

        /// <summary>
        /// Sets property with given key to specified value
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithProperty(string key, string value) =>
            this with { Properties = Properties.SetItem(key, value) };

        public ConsumerSettings<TKey, TValue> WithConsumerConfig(ConsumerConfig config)
            => WithProperties(config);

        public ConsumerSettings<TKey, TValue> WithProperties(IEnumerable<KeyValuePair<string, string>> properties)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, string>();
            builder.AddRange(Properties);
            foreach (var kvp in properties)
            {
                builder[kvp.Key] = kvp.Value;
            }
            return this with { Properties = builder.ToImmutable() };
        }

        /// <summary>
        /// Set the interval from one scheduled poll to the next.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPollInterval(TimeSpan pollInterval) => 
            this with { PollInterval = pollInterval };

        /// <summary>
        /// Set the maximum duration a poll to the Kafka broker is allowed to take.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPollTimeout(TimeSpan pollTimeout) => 
            this with { PollTimeout = pollTimeout };

        /// <summary>
        /// If offset commit requests are not completed within this timeout <see cref="CommitTimeoutException"/> will be thrown
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCommitTimeout(TimeSpan commitTimeout) => 
            this with { CommitTimeout = commitTimeout };

        /// <summary>
        /// If commits take longer than this time a warning is logged
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCommitTimeWarning(TimeSpan commitTimeWarning) => 
            this with { CommitTimeWarning = commitTimeWarning };

        /// <summary>
        /// When partition assigned events handling takes more then this timeout, the warning will be logged
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPartitionHandlerWarning(TimeSpan partitionHandlerWarning) => 
            this with { PartitionHandlerWarning = partitionHandlerWarning };

        /// <summary>
        /// Time to wait for pending requests when a partition is closed.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithWaitClosePartition(TimeSpan waitClosePartition) => 
            this with { WaitClosePartition = waitClosePartition };

        /// <summary>
        /// Allows topic auto-creation when constumer is subscribing or assigning to the topic.
        /// </summary>
        /// <remarks>
        /// When set, and still getting error from broker, consumer will assume that no message was produced yet
        /// </remarks>
        public ConsumerSettings<TKey, TValue> WithAutoCreateTopicsEnabled(bool autoCreateTopicsEnabled) => 
            this with { AutoCreateTopicsEnabled = autoCreateTopicsEnabled };
        
        /// <summary>
        /// If set to a finite duration, the consumer will re-send the last committed offsets periodically for all assigned partitions.
        /// Set it to TimeSpan.Zero to switch it off
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCommitRefreshInterval(TimeSpan commitRefreshInterval) =>
            this with { CommitRefreshInterval = commitRefreshInterval == TimeSpan.Zero ? Timeout.InfiniteTimeSpan : commitRefreshInterval };
        
        /// <summary>
        /// The stage will await outstanding offset commit requests before shutting down,
        /// but if that takes longer than this timeout it will stop forcefully.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithStopTimeout(TimeSpan stopTimeout) => 
            this with { StopTimeout = stopTimeout };
        
        /// <summary>
        ///  Limits the blocking on Kafka consumer position calls.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithPositionTimeout(TimeSpan positionTimeout) => 
            this with { PositionTimeout = positionTimeout };

        /// <summary>
        /// Fully qualified config path which holds the dispatcher configuration to be used by the consuming actor. Some blocking may occur.
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithDispatcher(string dispatcherId) => 
            this with { DispatcherId = dispatcherId };
        
        /// <summary>
        /// Check interval for TransactionalProducer when finishing transaction before shutting down consumer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithDrainingCheckInterval(TimeSpan drainingCheckInterval) => 
            this with { DrainingCheckInterval = drainingCheckInterval };
        
        /// <summary>
        /// Sets key deserializer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> keyDeserializer) => 
            this with { KeyDeserializer = keyDeserializer };
        
        /// <summary>
        /// Sets value deserializer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> valueDeserializer) => 
            this with { ValueDeserializer = valueDeserializer };
        
        public ConsumerSettings<TKey, TValue> WithConsumerFactory(Func<ConsumerSettings<TKey, TValue>, IConsumer<TKey, TValue>> consumerFactory) => 
            this with { ConsumerFactory = consumerFactory };
        
        /// <summary>
        /// Sets the timeout for closing the consumer
        /// </summary>
        public ConsumerSettings<TKey, TValue> WithCloseTimeout(TimeSpan closeTimeout) =>
            this with { StopTimeout = closeTimeout };

        /// <summary>
        /// Assigned consumer group Id, or null
        /// </summary>
        public string? GroupId => Properties.GetValueOrDefault("group.id");

        [Obsolete("Use C# record copy syntax with 'with' expressions instead")]
        private ConsumerSettings<TKey, TValue> Copy(
            IDeserializer<TKey> keyDeserializer = null,
            IDeserializer<TValue> valueDeserializer = null,
            TimeSpan? pollInterval = null,
            TimeSpan? pollTimeout = null,
            TimeSpan? commitTimeout = null,
            TimeSpan? partitionHandlerWarning = null,
            TimeSpan? metadataRequestTimeout = null,
            TimeSpan? drainingCheckInterval = null,
            TimeSpan? commitTimeWarning = null,
            TimeSpan? commitRefreshInterval = null,
            TimeSpan? stopTimeout = null,
            TimeSpan? positionTimeout = null,
            TimeSpan? waitClosePartition = null,
            bool? autoCreateTopicsEnabled = null,
            int? bufferSize = null,
            string dispatcherId = null,
            IImmutableDictionary<string, string> properties = null,
            ConnectionCheckerSettings connectionCheckerSettings = null,
            TimeSpan? closeTimeout = null,
            Func<ConsumerSettings<TKey, TValue>, IConsumer<TKey, TValue>>? consumerFactory = null
            ) =>
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
                metadataRequestTimeout: metadataRequestTimeout ?? this.MetadataRequestTimeout,
                drainingCheckInterval: drainingCheckInterval ?? this.DrainingCheckInterval,
                dispatcherId: dispatcherId ?? this.DispatcherId,
                autoCreateTopicsEnabled: autoCreateTopicsEnabled ?? this.AutoCreateTopicsEnabled,
                properties: properties ?? this.Properties,
                connectionCheckerSettings: connectionCheckerSettings ?? this.ConnectionCheckerSettings,
                consumerFactory: consumerFactory ?? this.ConsumerFactory);

        internal RebalanceListener<TKey, TValue>? RebalanceListener { get; private set; }
        
        /// <summary>
        /// Creates new kafka consumer, using event handlers provided
        /// </summary>
        public IConsumer<TKey, TValue> CreateKafkaConsumer(
            Action<IConsumer<TKey, TValue>, Error> consumeErrorHandler = null,
            Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionAssignedHandler = null,
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionRevokedHandler = null,
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> partitionLostHandler = null,
            Action<IConsumer<TKey, TValue>, string> statisticHandler = null)
        {
            RebalanceListener = new RebalanceListener<TKey, TValue>(
                onPartitionAssigned: partitionAssignedHandler,
                onPartitionRevoked: partitionRevokedHandler,
                onPartitionLost: partitionLostHandler);

            if (this.ConsumerFactory != null)
                return this.ConsumerFactory(this);
            
            return new ConsumerBuilder<TKey, TValue>(this.Properties)
                .SetKeyDeserializer(this.KeyDeserializer)
                .SetValueDeserializer(this.ValueDeserializer)
                .SetErrorHandler((c, e) => consumeErrorHandler?.Invoke(c, e))
                .SetPartitionsAssignedHandler((c, partitions) => partitionAssignedHandler?.Invoke(c, partitions))
                .SetPartitionsRevokedHandler((c, partitions) => partitionRevokedHandler?.Invoke(c, partitions))
                .SetPartitionsLostHandler((c, partitions) => partitionLostHandler?.Invoke(c, partitions))
                .SetStatisticsHandler((c, json) => statisticHandler?.Invoke(c, json))
                .Build();
        }
    }

    internal sealed class RebalanceListener<TKey, TValue>
    {
        public RebalanceListener(
            Action<IConsumer<TKey, TValue>, List<TopicPartition>> onPartitionAssigned, 
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> onPartitionRevoked, 
            Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> onPartitionLost)
        {
            OnPartitionAssigned = onPartitionAssigned;
            OnPartitionRevoked = onPartitionRevoked;
            OnPartitionLost = onPartitionLost;
        }

        public Action<IConsumer<TKey, TValue>, List<TopicPartition>> OnPartitionAssigned { get; } 
        public Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> OnPartitionRevoked { get; }
        public Action<IConsumer<TKey, TValue>, List<TopicPartitionOffset>> OnPartitionLost { get; }
    }
}
