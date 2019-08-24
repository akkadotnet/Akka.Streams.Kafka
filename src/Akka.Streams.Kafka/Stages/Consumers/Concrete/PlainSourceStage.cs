using System.Threading.Tasks;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    internal class PlainSourceStage<K, V> : KafkaSourceStage<K, V, ConsumeResult<K, V>>
    {
        public ConsumerSettings<K, V> Settings { get; }
        public ISubscription Subscription { get; }

        public PlainSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription) 
            : base("PlainSource")
        {
            Settings = settings;
            Subscription = subscription;
        }

        protected override GraphStageLogic Logic(SourceShape<ConsumeResult<K, V>> shape, TaskCompletionSource<NotUsed> completion, Attributes inheritedAttributes)
        {
            return new SingleSourceStageLogic<K, V, ConsumeResult<K, V>>(shape, Settings, Subscription, inheritedAttributes, 
                                                                         completion, new PlainMessageBuilder<K, V>());
        }
    }
}
