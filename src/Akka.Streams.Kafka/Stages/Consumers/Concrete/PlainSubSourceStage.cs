using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Akka.Streams.Util;
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

        /// <summary>
        /// PlainSubSourceStage
        /// </summary>
        public PlainSubSourceStage(ConsumerSettings<K, V> settings, IAutoSubscription subscription, 
                                   Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>> getOffsetsOnAssign,
                                   Action<IImmutableSet<TopicPartition>> onRevoke) 
            : base("PlainSubSource")
        {
            Settings = settings;
            Subscription = subscription;
            GetOffsetsOnAssign = getOffsetsOnAssign;
            OnRevoke = onRevoke;
        }

        /// <inheritdoc />
        protected override (GraphStageLogic, IControl) Logic(SourceShape<(TopicPartition, Source<ConsumeResult<K, V>, NotUsed>)> shape, 
                                                             Attributes inheritedAttributes)
        {
            var logic = new SubSourceLogic<K, V, ConsumeResult<K, V>>(shape, Settings, Subscription, 
                                                                      messageBuilderFactory: _ => new PlainMessageBuilder<K, V>(), 
                                                                      getOffsetsOnAssign: GetOffsetsOnAssign, 
                                                                      onRevoke: OnRevoke, 
                                                                      attributes: inheritedAttributes);

            return (logic, logic.Control);
        }
    }
}