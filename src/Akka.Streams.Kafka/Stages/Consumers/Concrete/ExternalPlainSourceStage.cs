using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    /// <summary>
    /// Single source stage for externally provided <see cref="KafkaConsumerActor{K,V}"/>
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    internal class ExternalPlainSourceStage<K, V> : KafkaSourceStage<K, V, ConsumeResult<K, V>>
    {
        /// <summary>
        /// Externally provided consumer
        /// </summary>
        public IActorRef Consumer { get; }
        /// <summary>
        /// Subscription
        /// </summary>
        public IManualSubscription Subscription { get; }
        
        public bool AutoCreateTopics { get; }

        /// <summary>
        /// ExternalPlainSourceStage
        /// </summary>
        /// <param name="consumer">Externally provided consumer</param>
        /// <param name="subscription">Manual subscription</param>
        /// <param name="autoCreateTopics">Flag to mark that the consumer actor uses `auto.create.topics.enable`</param>
        public ExternalPlainSourceStage(IActorRef consumer, IManualSubscription subscription, bool autoCreateTopics) 
            : base("ExternalPlainSubSource")
        {
            Consumer = consumer;
            Subscription = subscription;
            AutoCreateTopics = autoCreateTopics;
        }

        /// <inheritdoc />
        protected override (GraphStageLogic, IControl) Logic(SourceShape<ConsumeResult<K, V>> shape, Attributes inheritedAttributes)
        {
            var logic = new ExternalSingleSourceLogic<K, V, ConsumeResult<K, V>>(
                shape, Consumer, Subscription,
                inheritedAttributes, _ => new PlainMessageBuilder<K, V>(),
                AutoCreateTopics);

            return (logic, logic.Control);
        }
    }
}