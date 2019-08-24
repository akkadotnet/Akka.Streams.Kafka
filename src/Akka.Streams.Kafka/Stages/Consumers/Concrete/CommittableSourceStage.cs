using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    internal class CommittableSourceStage<K, V> : KafkaSourceStage<K, V, CommittableMessage<K, V>>
    {
        private readonly Func<ConsumeResult<K, V>, string> _metadataFromMessage;
        public ConsumerSettings<K, V> Settings { get; }
        public ISubscription Subscription { get; }

        public CommittableSourceStage(ConsumerSettings<K, V> settings, ISubscription subscription, 
                                      Func<ConsumeResult<K, V>, string> metadataFromMessage = null)
            : base("CommittableSource")
        {
            _metadataFromMessage = metadataFromMessage ?? (msg => string.Empty);
            Settings = settings;
            Subscription = subscription;
        }

        protected override GraphStageLogic Logic(SourceShape<CommittableMessage<K, V>> shape, TaskCompletionSource<NotUsed> completion, Attributes inheritedAttributes)
        { 
            return new SingleSourceStageLogic<K, V, CommittableMessage<K, V>>(shape, Settings, Subscription, inheritedAttributes, 
                                                                              completion, new CommittableSourceMessageBuilder<K, V>(Settings, _metadataFromMessage));
        }
    }
}
