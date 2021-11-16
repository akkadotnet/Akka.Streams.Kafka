using System;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Supervision
{
        public class DefaultConsumerDecider<TKey, TValue>
    {
        private readonly bool _autoCreateTopics;

        public DefaultConsumerDecider(ConsumerSettings<TKey, TValue> settings)
        {
            _autoCreateTopics = settings.AutoCreateTopicsEnabled;
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
                case KafkaException ke:
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
            => Directive.Stop;
        
        public virtual Directive OnException(Exception exception)
            => Directive.Stop;
    }
}