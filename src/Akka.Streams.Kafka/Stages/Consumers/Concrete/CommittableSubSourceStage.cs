using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Akka.Streams.Util;
using Akka.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    public class CommittableSubSourceStage<K, V> : KafkaSourceStage<K, V, (TopicPartition, Source<CommittableMessage<K, V>, NotUsed>)>
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

        public CommittableSubSourceStage(ConsumerSettings<K, V> settings, IAutoSubscription subscription, Func<ConsumeResult<K, V>, string> metadataFromRecord = null) 
            : base("CommittableSubSourceStage")
        {
            Settings = settings;
            Subscription = subscription;
            _metadataFromRecord = metadataFromRecord ?? (_ => string.Empty);
        }

        protected override (GraphStageLogic, IControl) Logic(SourceShape<(TopicPartition, Source<CommittableMessage<K, V>, NotUsed>)> shape, Attributes inheritedAttributes)
        {
            var logic = new SubSourceLogic<K, V, CommittableMessage<K, V>>(shape, Settings, Subscription, 
                                                                           messageBuilderFactory: GetMessageBuilder, 
                                                                           getOffsetsOnAssign: Option<Func<IImmutableSet<TopicPartition>, Task<IImmutableSet<TopicPartitionOffset>>>>.None, 
                                                                           onRevoke: _ => { }, 
                                                                           attributes: inheritedAttributes);
            return (logic, logic.Control);
        }
        
        /// <summary>
        /// Creates message builder for sub-source logic
        /// </summary>
        private CommittableSourceMessageBuilder<K, V> GetMessageBuilder(SubSourceLogic<K, V, CommittableMessage<K, V>> logic)
        {
            var committer = new KafkaAsyncConsumerCommitter(() => logic.ConsumerActor, Settings.CommitTimeout);
            return new CommittableSourceMessageBuilder<K, V>(committer, Settings.GroupId, _metadataFromRecord);
        }
    }
}