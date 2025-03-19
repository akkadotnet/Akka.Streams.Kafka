// -----------------------------------------------------------------------
//  <copyright file="CommittableSubSourceStage.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Akka.Streams.Util;
using Akka.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete;

public class
    CommittableSubSourceStage<K, V> : KafkaSourceStage<K, V, (TopicPartition,
    Source<CommittableMessage<K, V>, NotUsed>)>
{
    private readonly Func<ConsumeResult<K, V>, string> _metadataFromRecord;

    /// <summary>
    /// Consumer settings
    /// </summary>
    public ConsumerSettings<K, V> Settings { get; }

    /// <summary>
    /// Subscription
    /// </summary>
    public IAutoSubscription Subscription { get; }

    public CommittableSubSourceStage(ConsumerSettings<K, V> settings, IAutoSubscription subscription,
        Func<ConsumeResult<K, V>, string>? metadataFromRecord = null)
        : base("CommittableSubSourceStage")
    {
        Settings = settings;
        Subscription = subscription;
        _metadataFromRecord = metadataFromRecord ?? (_ => string.Empty);
        _subSourceStageLogicFactory = new CommittableSubSourceStageLogicFactory(Settings, _metadataFromRecord);
    }

    private readonly SubSourceLogic.ISubSourceStageLogicFactory<K, V, CommittableMessage<K, V>>
        _subSourceStageLogicFactory;

    protected override (GraphStageLogic, IControl) Logic(
        SourceShape<(TopicPartition, Source<CommittableMessage<K, V>, NotUsed>)> shape,
        Attributes inheritedAttributes)
    {
        var logic = new SubSourceLogic<K, V, CommittableMessage<K, V>>(shape, Settings, Subscription,
            Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>>.None,
            _ => { },
            _subSourceStageLogicFactory,
            inheritedAttributes);
        return (logic, logic.Control);
    }

    private class
        CommittableSubSourceStageLogicFactory(
            ConsumerSettings<K, V> settings,
            Func<ConsumeResult<K, V>, string> metadataFromRecord)
        : SubSourceLogic.ISubSourceStageLogicFactory<K, V,
            CommittableMessage<K, V>>
    {
        public SubSourceStageLogic<K, V, CommittableMessage<K, V>> Create(SourceShape<CommittableMessage<K, V>> shape,
            TopicPartition tp, IActorRef consumerActor,
            Action<SubSourceLogic.SubSourceStageLogicControl> subSourceStartedCb,
            Action<(TopicPartition partition, SubSourceLogic.ISubSourceCancellationStrategy cancellationStrategy)>
                subSourceCancelledCb, int actorNumber) =>
            new CommittableSubSourceStageLogic<K, V>(shape, tp, consumerActor, actorNumber,
                GetMessageBuilder(consumerActor, settings, metadataFromRecord),
                subSourceStartedCb, subSourceCancelledCb);

        /// <summary>
        /// Creates message builder for sub-source logic
        /// </summary>
        private CommittableSourceMessageBuilder<K, V> GetMessageBuilder(IActorRef consumerActor,
            ConsumerSettings<K, V> consumerSettings, Func<ConsumeResult<K, V>, string> metadataFromRecord)
        {
            var committer = new KafkaAsyncConsumerCommitter(() => consumerActor, consumerSettings.CommitTimeout);
            return new CommittableSourceMessageBuilder<K, V>(committer, consumerSettings.GroupId, metadataFromRecord);
        }
    }
}

internal sealed class CommittableSubSourceStageLogic<K, V> : SubSourceStageLogic<K, V, CommittableMessage<K, V>>
{
    public CommittableSubSourceStageLogic(
        SourceShape<CommittableMessage<K, V>> shape,
        TopicPartition topicPartition,
        IActorRef consumerActor, int actorNumber,
        IMessageBuilder<K, V, CommittableMessage<K, V>> messageBuilder,
        Action<SubSourceLogic.SubSourceStageLogicControl> subSourceStartedCallback,
        Action<(TopicPartition, SubSourceLogic.ISubSourceCancellationStrategy)> subSourceCancelledCallback)
        : base(shape, topicPartition, consumerActor, actorNumber, messageBuilder, subSourceStartedCallback,
            subSourceCancelledCallback)
    {
    }
}