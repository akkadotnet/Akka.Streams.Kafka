using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Abstract;
using Akka.Streams.Stage;

namespace Akka.Streams.Kafka.Stages.Consumers.Concrete
{
    /// <summary>
    /// This stage is used for <see cref="KafkaConsumer.TransactionalSource{K,V}"/>
    /// </summary>
    /// <typeparam name="K">The key type</typeparam>
    /// <typeparam name="V">The value type</typeparam>
    public class TransactionalSourceStage<K, V> : KafkaSourceStage<K, V, TransactionalMessage<K, V>>
    {
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;

        /// <summary>
        /// TransactionalSourceStage
        /// </summary>
        /// <param name="settings">Consumer settings</param>
        /// <param name="subscription">Subscription</param>
        public TransactionalSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription) 
            : base("TransactionalSource", settings.AutoCreateTopicsEnabled)
        {
            _settings = settings;
            _subscription = subscription;
        }

        /// <inheritdoc />
        protected override (GraphStageLogic, IControl) Logic(SourceShape<TransactionalMessage<K, V>> shape, Attributes inheritedAttributes)
        {
            var transactionalConsumerSettings = TransactionalSourceHelper.PrepareSettings(_settings);
            var logic = new TransactionalSourceLogic<K, V>(
                shape: shape, 
                settings: transactionalConsumerSettings, 
                subscription: _subscription, 
                attributes: InitialAttributes, 
                messageBuilderFactory: stage => new TransactionalMessageBuilder<K, V>(stage));
            
            return (logic, logic.Control);
        }
    }
}