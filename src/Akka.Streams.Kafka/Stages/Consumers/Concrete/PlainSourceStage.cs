using System.Threading.Tasks;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    /// <summary>
    /// This stage is used for <see cref="KafkaConsumer.PlainSource{K,V}"/>
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    internal class PlainSourceStage<K, V> : KafkaSourceStage<K, V, ConsumeResult<K, V>>
    {
        /// <summary>
        /// Consumer settings
        /// </summary>
        public ConsumerSettings<K, V> Settings { get; }
        /// <summary>
        /// Subscription
        /// </summary>
        public ISubscription Subscription { get; }

        /// <summary>
        /// PlainSourceStage
        /// </summary>
        /// <param name="settings">Consumer settings</param>
        /// <param name="subscription">Subscription to be used</param>
        public PlainSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription) 
            : base("PlainSource")
        {
            Settings = settings;
            Subscription = subscription;
        }

        /// <summary>
        /// Provides actual stage logic
        /// </summary>
        /// <param name="shape">Shape of the stage</param>
        /// <param name="inheritedAttributes">Stage attributes</param>
        /// <returns>Stage logic</returns>
        protected override (GraphStageLogic, IControl) Logic(SourceShape<ConsumeResult<K, V>> shape, Attributes inheritedAttributes)
        {
            var logic = new SingleSourceStageLogic<K, V, ConsumeResult<K, V>>(shape, Settings, Subscription, inheritedAttributes, 
                                                                              _ => new PlainMessageBuilder<K, V>());
            return (logic, logic.Control);
        }
    }
}
