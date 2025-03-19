// -----------------------------------------------------------------------
//  <copyright file="GroupTopicPartitionOffset.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages;

/// <summary>
/// Offset position for a groupId, topic, partition.
/// </summary>
public record GroupTopicPartitionOffset
{
    /// <summary>
    /// GroupTopicPartitionOffset
    /// </summary>
    public GroupTopicPartitionOffset(string groupId, string topic, int partition, Offset offset)
    {
        GroupId = groupId;
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }

    /// <summary>
    /// GroupTopicPartitionOffset
    /// </summary>
    public GroupTopicPartitionOffset(GroupTopicPartition groupTopicPartition, Offset offset)
        : this(groupTopicPartition.GroupId, groupTopicPartition.Topic, groupTopicPartition.Partition, offset)
    {
    }

    /// <summary>
    /// Consumer's group Id
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// Topic
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Partition
    /// </summary>
    public int Partition { get; }

    /// <summary>
    /// Kafka partition offset value
    /// </summary>
    public Offset Offset { get; }

    /// <summary>
    /// Group topic partition info
    /// </summary>
    public GroupTopicPartition GroupTopicPartition
    {
        get { return new GroupTopicPartition(GroupId, Topic, Partition); }
    }
}

/// <summary>
/// Group, topic and partition info
/// </summary>
public sealed record GroupTopicPartition(string GroupId, string Topic, int Partition)
{
    public TopicPartition TopicPartition { get; } = new(Topic, Partition);
}

public sealed record OffsetAndMetadata(Offset Offset, string Metadata);