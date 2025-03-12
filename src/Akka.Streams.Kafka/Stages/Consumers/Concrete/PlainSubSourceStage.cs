using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Akka.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    /// <summary>
    /// This stage is used for <see cref="KafkaConsumer.PlainPartitionedSource{K,V}"/>
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    public class PlainSubSourceStage<K, V> : KafkaSourceStage<K, V, (TopicPartition, Source<ConsumeResult<K, V>, NotUsed>)>
    {
        /// <summary>
        /// Consumer settings
        /// </summary>
        public ConsumerSettings<K, V> Settings { get; }
        /// <summary>
        /// Subscription
        /// </summary>
        public IAutoSubscription Subscription { get; }
        /// <summary>
        /// Function to get offsets from partitions on paritions assigned event
        /// </summary>
        public Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> GetOffsetsOnAssign { get; }
        /// <summary>
        /// Partitions revoked event handling action
        /// </summary>
        public Action<IImmutableSet<TopicPartition>> OnRevoke { get; }

        private readonly SubSourceLogic.ISubSourceStageLogicFactory<K, V, ConsumeResult<K, V>>
            _subSourceStageLogicFactory;
        
        public PlainSubSourceStage(ConsumerSettings<K, V> settings, IAutoSubscription subscription, 
                                   Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> getOffsetsOnAssign,
                                   Action<IImmutableSet<TopicPartition>> onRevoke) 
            : base("PlainSubSource")
        {
            Settings = settings;
            Subscription = subscription;
            GetOffsetsOnAssign = getOffsetsOnAssign;
            OnRevoke = onRevoke;
            _subSourceStageLogicFactory = new PlainSubSourceStageLogicFactory();
        }

        private class
            PlainSubSourceStageLogicFactory : SubSourceLogic.ISubSourceStageLogicFactory<K, V, ConsumeResult<K, V>>
        {
            public SubSourceStageLogic<K, V, ConsumeResult<K, V>> Create(SourceShape<ConsumeResult<K, V>> shape,
                TopicPartition tp, IActorRef consumerActor,
                Action<SubSourceLogic.SubSourceStageLogicControl> subSourceStartedCb,
                Action<(TopicPartition partition, SubSourceLogic.ISubSourceCancellationStrategy cancellationStrategy)>
                    subSourceCancelledCb, int actorNumber) =>
                new PlainSubSourceStageLogic<K, V>(shape, tp, consumerActor, actorNumber,
                    new PlainMessageBuilder<K, V>(), subSourceStartedCb, subSourceCancelledCb);
        }
        
        protected override (GraphStageLogic, IControl) Logic(SourceShape<(TopicPartition, Source<ConsumeResult<K, V>, NotUsed>)> shape, 
                                                             Attributes inheritedAttributes)
        {
            var logic = new SubSourceLogic<K, V, ConsumeResult<K, V>>(shape, Settings, Subscription, 
                                                                      getOffsetsOnAssign: GetOffsetsOnAssign, 
                                                                      onRevoke: OnRevoke, 
                                                                      _subSourceStageLogicFactory,
                                                                      attributes: inheritedAttributes);

            return (logic, logic.Control);
        }
    }
    
    internal sealed class PlainSubSourceStageLogic<K, V> : SubSourceStageLogic<K, V, ConsumeResult<K, V>>
    {
        public PlainSubSourceStageLogic(
            SourceShape<ConsumeResult<K, V>> shape,
            TopicPartition topicPartition,
            IActorRef consumerActor, int actorNumber,
            IMessageBuilder<K, V, ConsumeResult<K, V>> messageBuilder,
            Action<SubSourceLogic.SubSourceStageLogicControl> subSourceStartedCallback,
            Action<(TopicPartition, SubSourceLogic.ISubSourceCancellationStrategy)> subSourceCancelledCallback)
            : base(shape, topicPartition, consumerActor, actorNumber, messageBuilder, subSourceStartedCallback,
                subSourceCancelledCallback)
        {
        }
    }
}