// -----------------------------------------------------------------------
//  <copyright file="Committers.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers;

/// <summary>
/// Used by <see cref="CommittableSourceMessageBuilder{K,V}"/> to commit messages by
/// sending <see cref="KafkaConsumerActorMetadata.Internal.Commit"/> to <see cref="KafkaConsumerActor{K,V}"/>
/// </summary>
internal class KafkaAsyncConsumerCommitter : IEquatable<KafkaAsyncConsumerCommitter>
{
    private readonly TimeSpan _commitTimeout;
    private readonly Lazy<IActorRef> _consumerActor;

    public KafkaAsyncConsumerCommitter(Func<IActorRef> consumerActorFactory, TimeSpan commitTimeout)
    {
        _commitTimeout = commitTimeout;
        _consumerActor = new Lazy<IActorRef>(consumerActorFactory);
    }

    public virtual Task CommitOneOfMany(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) =>
        SendWithReply(new KafkaConsumerActorMetadata.Internal.Commit(topicPartition, offsetAndMetadata));

    public virtual Task CommitSingle(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) =>
        SendWithReply(new KafkaConsumerActorMetadata.Internal.CommitSingle(topicPartition, offsetAndMetadata));

    public virtual void TellCommit(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata,
        bool emergency) =>
        _consumerActor.Value.Tell(
            new KafkaConsumerActorMetadata.Internal.CommitWithoutReply(topicPartition, offsetAndMetadata, emergency));

    private Task<Done> SendWithReply(object msg)
    {
        return DoAsk();

        async Task<Done> DoAsk()
        {
            try
            {
                var askOp = await _consumerActor.Value.Ask(msg, _commitTimeout);
                return Done.Instance;
            }
            catch (Exception ex)
            {
                switch (ex)
                {
                    case AskTimeoutException:
                        throw new CommitTimeoutException($"Kafka commit took longer than: {_commitTimeout}");
                    default:
                        throw;
                }
            }
        }
    }

    public static Task Commit(CommittableOffset committableOffset)
    {
        var committer = committableOffset.Committer;
        
        /*
         * https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
         * "The committed offset should always be the offset of the next message that your application will read.
         * Thus, when calling commitSync(offsets) you should add one to the offset of the last message processed."
         */
        return committer.CommitSingle(committableOffset.Offset.GroupTopicPartition.TopicPartition,
            new OffsetAndMetadata(committableOffset.Offset.Offset + 1, committableOffset.Metadata));
    }

    public static Task Commit(CommittableOffsetBatch batch)
    {
        var tasks = ForBatch(batch, (committer, partition, metadata) => committer.CommitOneOfMany(partition, metadata));
        return Task.WhenAll(tasks);
    }

    public static void TellCommit(CommittableOffsetBatch batch, bool emergency) =>
        ForBatch(batch, (committer, partition, metadata) =>
        {
            committer.TellCommit(partition, metadata, emergency);
            return Done.Instance;
        });

    private static IReadOnlyList<T> ForBatch<T>(CommittableOffsetBatch batch,
        Func<KafkaAsyncConsumerCommitter, TopicPartition, OffsetAndMetadata, T> sendMsg)
    {
        var results = batch.OffsetsAndMetadata.Select(c =>
        {
            var (groupTopicPartition, offsetAndMetadata) = c;
            // sends one message per partition; they are aggregated together in the KafkaConsumerActor
            var committer = batch.CommitterFor(groupTopicPartition);
            return sendMsg(committer, groupTopicPartition.TopicPartition, offsetAndMetadata);
        }).ToList(); // ToList to force evaluation - otherwise some offsets won't be sent

        return results;
    }

    /*
     * Have to override equality members for both the commitTimeout and the consumerActor. This comparison is used
     * inside the CommittableOffsetBatch. The comparison is mostly relevant when multiple sources share a consumer
     * actor.
     */

    public bool Equals(KafkaAsyncConsumerCommitter? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return _commitTimeout.Equals(other._commitTimeout) && _consumerActor.Value.Equals(other._consumerActor.Value);
    }

    public override bool Equals(object? obj) =>
        ReferenceEquals(this, obj) || (obj is KafkaAsyncConsumerCommitter other && Equals(other));

    public override int GetHashCode()
    {
        unchecked
        {
            return (_commitTimeout.GetHashCode() * 397) ^ _consumerActor.Value.GetHashCode();
        }
    }

    public static bool operator ==(KafkaAsyncConsumerCommitter? left, KafkaAsyncConsumerCommitter? right) =>
        Equals(left, right);

    public static bool operator !=(KafkaAsyncConsumerCommitter? left, KafkaAsyncConsumerCommitter? right) =>
        !Equals(left, right);
}