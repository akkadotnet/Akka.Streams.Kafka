// -----------------------------------------------------------------------
//  <copyright file="ProducerExtensions.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Streams.Kafka.Messages;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Extensions;

/// <summary>
/// ProducerExtensions
/// </summary>
public static class ProducerExtensions
{
    /// <summary>
    /// Produce <see cref="ProducerRecord{K,V}"/>
    /// </summary>
    public static void Produce<K, V>(this IProducer<K, V> producer, ProducerRecord<K, V> message,
        Action<DeliveryReport<K, V>> reportHandler)
    {
        if (message.Partition.HasValue)
        {
            producer.Produce(new TopicPartition(message.Topic, message.Partition.Value), message.Message,
                reportHandler);
        }
        else
        {
            producer.Produce(message.Topic, message.Message, reportHandler);
        }
    }
}