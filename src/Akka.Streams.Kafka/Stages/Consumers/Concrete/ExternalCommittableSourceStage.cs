using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    /// <summary>
    /// This stage is used for <see cref="KafkaConsumer.CommittableExternalSource{K,V}"/>
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    public class ExternalCommittableSourceStage<K, V> : KafkaSourceStage<K, V, CommittableMessage<K, V>>
    {
        /// <summary>
        /// Externally provided consumer
        /// </summary>
        public IActorRef Consumer { get; }
        /// <summary>
        /// Subscription
        /// </summary>
        public IManualSubscription Subscription { get; }
        /// <summary>
        /// Consumer group Id
        /// </summary>
        public string GroupId { get; }
        /// <summary>
        /// Commit timeout
        /// </summary>
        public TimeSpan CommitTimeout { get; }

        /// <summary>
        /// ExternalCommittableSourceStage
        /// </summary>
        public ExternalCommittableSourceStage(IActorRef consumer, string groupId, TimeSpan commitTimeout, IManualSubscription subscription) 
            : base("ExternalCommittableSource", false)
        {
            Consumer = consumer;
            GroupId = groupId;
            CommitTimeout = commitTimeout;
            Subscription = subscription;
        }

        /// <inheritdoc />
        protected override (GraphStageLogic, IControl) Logic(SourceShape<CommittableMessage<K, V>> shape, Attributes inheritedAttributes)
        {
            var logic = new ExternalSingleSourceLogic<K, V, CommittableMessage<K, V>>(shape, Consumer, Subscription, inheritedAttributes, GetMessageBuilder);

            return (logic, logic.Control);
        }
        
        private CommittableSourceMessageBuilder<K, V> GetMessageBuilder(BaseSingleSourceLogic<K, V, CommittableMessage<K, V>> logic)
        {
            var committer = new KafkaAsyncConsumerCommitter(() => logic.ConsumerActor, CommitTimeout);
            return new CommittableSourceMessageBuilder<K, V>(committer, GroupId, m => string.Empty);
        }
    }
}