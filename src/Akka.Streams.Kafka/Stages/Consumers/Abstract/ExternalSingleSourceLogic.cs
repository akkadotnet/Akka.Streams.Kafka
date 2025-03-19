// -----------------------------------------------------------------------
//  <copyright file="ExternalSingleSourceLogic.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract;

/// <summary>
/// Single source logic for externally provided <see cref="KafkaConsumerActor{K,V}"/>
/// </summary>
/// <typeparam name="K">Key type</typeparam>
/// <typeparam name="V">Value type</typeparam>
/// <typeparam name="TMessage">Message type</typeparam>
internal class ExternalSingleSourceLogic<K, V, TMessage> : BaseSingleSourceLogic<K, V, TMessage>
{
    private readonly IActorRef _consumerActor;
    private readonly IManualSubscription _manualSubscription;

    public ExternalSingleSourceLogic(
        SourceShape<TMessage> shape,
        IActorRef consumerActor,
        IManualSubscription subscription,
        Attributes attributes,
        Func<BaseSingleSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory,
        bool autoCreateTopics)
        : base(shape, attributes, messageBuilderFactory, autoCreateTopics, subscription)
    {
        _consumerActor = consumerActor;
        _manualSubscription = subscription;
    }

    protected override IActorRef CreateConsumerActor() => _consumerActor;

    protected override void ConfigureSubscription() => ConfigureManualSubscription(_manualSubscription);

    protected override void PerformShutdown()
    {
        base.PerformShutdown();
        CompleteStage();
    }
}