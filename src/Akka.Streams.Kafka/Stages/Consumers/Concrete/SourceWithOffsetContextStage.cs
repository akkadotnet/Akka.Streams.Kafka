using System;
using System.Threading.Tasks;
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
    /// This stage is used for <see cref="KafkaConsumer.SourceWithOffsetContext{K,V}"/>
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    internal class SourceWithOffsetContextStage<K, V> : KafkaSourceStage<K, V, (ConsumeResult<K, V>, ICommittableOffset)>
    {
        /// <summary>
        /// Method for extracting string metadata from consumed record
        /// </summary>
        private readonly Func<ConsumeResult<K, V>, string> _metadataFromMessage;

        /// <summary>
        /// Consumer settings
        /// </summary>
        public ConsumerSettings<K, V> Settings { get; }
        /// <summary>
        /// Subscription
        /// </summary>
        public ISubscription Subscription { get; }

        /// <summary>
        /// CommittableSourceStage
        /// </summary>
        /// <param name="settings">Consumer settings</param>
        /// <param name="subscription">Subscription to be used</param>
        /// <param name="metadataFromMessage">Function to extract string metadata from consumed message</param>
        public SourceWithOffsetContextStage(ConsumerSettings<K, V> settings, ISubscription subscription,
                                            Func<ConsumeResult<K, V>, string> metadataFromMessage = null)
            : base("SourceWithOffsetContext", settings.AutoCreateTopicsEnabled)
        {
            _metadataFromMessage = metadataFromMessage ?? (msg => string.Empty);
            Settings = settings;
            Subscription = subscription;
        }

        /// <inheritdoc />
        protected override (GraphStageLogic, IControl) Logic(
            SourceShape<(ConsumeResult<K, V>, ICommittableOffset)> shape,
            Attributes inheritedAttributes)
        {
            var logic = new SingleSourceStageLogic<K, V, (ConsumeResult<K, V>, ICommittableOffset)>(shape, Settings, Subscription, 
                                                                                                    inheritedAttributes, 
                                                                                                    GetMessageBuilder);
            return (logic, logic.Control);
        }
        
        private OffsetContextBuilder<K, V> GetMessageBuilder(BaseSingleSourceLogic<K, V, (ConsumeResult<K, V>, ICommittableOffset)> logic)
        {
            var committer = new KafkaAsyncConsumerCommitter(() => logic.ConsumerActor, Settings.CommitTimeout);
            return new OffsetContextBuilder<K, V>(committer, Settings, _metadataFromMessage);
        }
    }
}
