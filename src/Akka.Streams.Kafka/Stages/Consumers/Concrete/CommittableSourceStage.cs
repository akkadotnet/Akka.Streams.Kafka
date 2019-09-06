using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    /// <summary>
    /// This stage is used for <see cref="KafkaConsumer.CommittableSource{K,V}"/>
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    internal class CommittableSourceStage<K, V> : KafkaSourceStage<K, V, CommittableMessage<K, V>>
    {
        private readonly Func<ConsumeResult<K, V>, string> _metadataFromMessage;
        
        /// <summary>
        /// Consumer settings
        /// </summary>
        public ConsumerSettings<K, V> Settings { get; }
        /// <summary>
        /// Subscription
        /// </summary>
        public ISubscription Subscription { get; }

        public CommittableSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription, 
                                      Func<ConsumeResult<K, V>, string> metadataFromMessage = null)
            : base("CommittableSource")
        {
            _metadataFromMessage = metadataFromMessage ?? (msg => string.Empty);
            Settings = settings;
            Subscription = subscription;
        }

        /// <summary>
        /// Provides actual stage logic
        /// </summary>
        /// <param name="shape">Shape of the stage</param>
        /// <param name="completion">Used to specify stage task completion</param>
        /// <param name="inheritedAttributes">Stage attributes</param>
        /// <returns>Stage logic</returns>
        protected override GraphStageLogic Logic(SourceShape<CommittableMessage<K, V>> shape, TaskCompletionSource<NotUsed> completion, Attributes inheritedAttributes)
        { 
            return new SingleSourceStageLogic<K, V, CommittableMessage<K, V>>(shape, Settings, Subscription, inheritedAttributes, completion, GetMessageBuilder);
        }

        private CommittableSourceMessageBuilder<K, V> GetMessageBuilder(BaseSingleSourceLogic<K, V, CommittableMessage<K, V>> logic)
        {
            var committer = new KafkaAsyncConsumerCommitter(logic.ConsumerActor, Settings.CommitTimeout);
            return new CommittableSourceMessageBuilder<K, V>(committer, Settings, _metadataFromMessage);
        }
    }
}
