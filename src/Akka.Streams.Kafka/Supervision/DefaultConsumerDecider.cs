using System;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Supervision
{
        public class DefaultConsumerDecider<TKey, TValue>
    {
        private readonly bool _autoCreateTopics;

        public DefaultConsumerDecider(ConsumerSettings<TKey, TValue> settings = null)
        {
            _autoCreateTopics = settings?.AutoCreateTopicsEnabled ?? false;
        }
        
        public Directive Decide(Exception e)
        {
            switch (e)
            {
                case ConsumeException ce:
                    if (ce.Error.IsFatal)
                        return Directive.Stop;
                    if (ce.Error.Code == ErrorCode.UnknownTopicOrPart && _autoCreateTopics)
                        return Directive.Resume;
                    if (ce.Error.IsSerializationError())
                        return OnDeserializationError(ce);
                    return OnConsumeException(ce);
                
                case KafkaRetriableException _:
                    return Directive.Resume;
                
                case KafkaException ke:
                    if (ke.Error.IsFatal)
                        return Directive.Stop;
                    return OnKafkaException(ke);
                
                case var exception:
                    return OnException(exception);
            }
        }

        public virtual Directive OnDeserializationError(ConsumeException exception)
            => Directive.Stop;

        public virtual Directive OnConsumeException(ConsumeException exception)
            => Directive.Resume;

        public virtual Directive OnKafkaException(KafkaException exception)
            => Directive.Resume;
        
        public virtual Directive OnException(Exception exception)
            => Directive.Stop;
    }
}