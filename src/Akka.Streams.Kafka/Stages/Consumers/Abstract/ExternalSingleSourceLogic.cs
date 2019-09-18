using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// Single source logic for externally provided <see cref="KafkaConsumerActor{K,V}"/>
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    /// <typeparam name="TMessage">Message type</typeparam>
    internal class ExternalSingleSourceLogic<K, V, TMessage> : BaseSingleSourceLogic<K, V, TMessage>
    {
        private readonly IActorRef _consumerActor;
        private readonly IManualSubscription _subscription;

        public ExternalSingleSourceLogic(SourceShape<TMessage> shape, IActorRef consumerActor, IManualSubscription subscription,
                                         TaskCompletionSource<NotUsed> completion, Attributes attributes, 
                                         Func<BaseSingleSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory) 
            : base(shape, completion, attributes, messageBuilderFactory)
        {
            _consumerActor = consumerActor;
            _subscription = subscription;
        }

        /// <inheritdoc />
        protected override IActorRef CreateConsumerActor() => _consumerActor;

        /// <inheritdoc />
        protected override void ConfigureSubscription() => ConfigureManualSubscription(_subscription);

        /// <inheritdoc />
        protected override void PerformShutdown() => CompleteStage();
    }
}