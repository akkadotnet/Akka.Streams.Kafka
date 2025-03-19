// -----------------------------------------------------------------------
//  <copyright file="ProducerSettings.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Streams.Kafka.Internal;
using Akka.Util.Internal;
using Confluent.Kafka;
using Error = Confluent.Kafka.Error;

namespace Akka.Streams.Kafka.Settings;

public sealed record ProducerSettings<TKey, TValue>
{
    public ProducerSettings(ISerializer<TKey>? keySerializer, ISerializer<TValue>? valueSerializer, int parallelism,
        string dispatcherId, TimeSpan flushTimeout, TimeSpan eosCommitInterval,
        IImmutableDictionary<string, string> properties)
    {
        // These properties are guaranteed to be initialized in all constructors
        KeySerializer = keySerializer;
        ValueSerializer = valueSerializer;
        Parallelism = parallelism;
        DispatcherId = dispatcherId ?? throw new ArgumentNullException(nameof(dispatcherId));
        FlushTimeout = flushTimeout;
        EosCommitInterval = eosCommitInterval;
        Properties = properties ?? throw new ArgumentNullException(nameof(properties));
    }

    public ISerializer<TKey>? KeySerializer { get; init; }
    public ISerializer<TValue>? ValueSerializer { get; init; }
    public int Parallelism { get; init; }
    public string DispatcherId { get; init; } = null!;
    public TimeSpan FlushTimeout { get; init; }

    /// <summary>
    /// The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
    /// </summary>
    public TimeSpan EosCommitInterval { get; init; }

    public IImmutableDictionary<string, string> Properties { get; init; } = null!;

    /// <summary>
    /// Gets property value by key
    /// </summary>
    public object? this[string propertyKey]
    {
        get { return Properties.GetValueOrDefault(propertyKey); }
    }

    public string? GetProperty(string key) => Properties.GetValueOrDefault(key);

    public ProducerSettings<TKey, TValue> WithBootstrapServers(string bootstrapServers) =>
        WithProperty("bootstrap.servers", bootstrapServers);

    public ProducerSettings<TKey, TValue> WithProperty(string key, string value) =>
        this with { Properties = Properties.SetItem(key, value) };

    public ProducerSettings<TKey, TValue> WithProducerConfig(ProducerConfig config)
        => WithProperties(config);

    public ProducerSettings<TKey, TValue> WithProperties(IEnumerable<KeyValuePair<string, string>> properties)
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
    /// The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`.
    /// </summary>
    public ProducerSettings<TKey, TValue> WithEosCommitInterval(TimeSpan eosCommitInterval) =>
        this with { EosCommitInterval = eosCommitInterval };

    public ProducerSettings<TKey, TValue> WithParallelism(int parallelism) =>
        this with { Parallelism = parallelism };

    public ProducerSettings<TKey, TValue> WithDispatcher(string dispatcherId) =>
        this with { DispatcherId = dispatcherId };

    [Obsolete("Use C# record copy syntax with 'with' expressions instead")]
    private ProducerSettings<TKey, TValue> Copy(
        ISerializer<TKey>? keySerializer = null,
        ISerializer<TValue>? valueSerializer = null,
        int? parallelism = null,
        string? dispatcherId = null,
        TimeSpan? flushTimeout = null,
        TimeSpan? eosCommitInterval = null,
        IImmutableDictionary<string, string>? properties = null) =>
        new(
            keySerializer ?? KeySerializer,
            valueSerializer ?? ValueSerializer,
            parallelism ?? Parallelism,
            dispatcherId ?? DispatcherId,
            flushTimeout ?? FlushTimeout,
            eosCommitInterval ?? EosCommitInterval,
            properties ?? Properties);

    public static ProducerSettings<TKey, TValue> Create(ActorSystem system, ISerializer<TKey>? keySerializer,
        ISerializer<TValue>? valueSerializer)
    {
        if (system == null) throw new ArgumentNullException(nameof(system));

        var config = system.Settings.Config.GetConfig("akka.kafka.producer");
        return Create(config, keySerializer, valueSerializer);
    }

    public static ProducerSettings<TKey, TValue> Create(Akka.Configuration.Config config,
        ISerializer<TKey>? keySerializer, ISerializer<TValue>? valueSerializer)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config), "Kafka config for Akka.NET producer was not provided");

        var properties = config.GetConfig("kafka-clients").ParseKafkaClientsProperties();

        return new ProducerSettings<TKey, TValue>(
            keySerializer,
            valueSerializer,
            config.GetInt("parallelism", 100),
            config.GetString("use-dispatcher", "akka.kafka.default-dispatcher"),
            config.GetTimeSpan("flush-timeout", TimeSpan.FromSeconds(2)),
            config.GetTimeSpan("eos-commit-interval", TimeSpan.FromMilliseconds(100)),
            properties);
    }

    public IProducer<TKey, TValue> CreateKafkaProducer(
        Action<IProducer<TKey, TValue>, Error>? producerErrorHandler = null)
    {
        var builder = new ProducerBuilder<TKey, TValue>(Properties);

        if (KeySerializer != null)
            builder.SetKeySerializer(KeySerializer);

        if (ValueSerializer != null)
            builder.SetValueSerializer(ValueSerializer);

        return builder
            .SetErrorHandler((p, error) => producerErrorHandler?.Invoke(p, error))
            .Build();
    }
}