using System;
using System.Reflection;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Supervision
{
    public class DefaultDecider<TKey, TValue>
    {
        private readonly bool _autoCreateTopics;

        public DefaultDecider(ConsumerSettings<TKey, TValue> settings)
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
                case ProduceException<TKey, TValue> pe:
                    if (pe.Error.IsFatal)
                        return Directive.Stop;
                    if (pe.Error.Code == ErrorCode.UnknownTopicOrPart && _autoCreateTopics)
                        return Directive.Resume;
                    if (pe.Error.IsSerializationError())
                        return OnSerializationError(pe);
                    return OnProduceException(pe);
                case KafkaException ke:
                    return OnKafkaException(ke);
                case var exception:
                    return OnException(exception);
            }
        }

        public virtual Directive OnDeserializationError(ConsumeException exception)
            => Directive.Stop;

        public virtual Directive OnSerializationError(ProduceException<TKey, TValue> exception)
            => Directive.Stop;

        public virtual Directive OnConsumeException(ConsumeException exception)
            => Directive.Stop;

        public virtual Directive OnProduceException(ProduceException<TKey, TValue> exception)
            => Directive.Stop;

        public virtual Directive OnKafkaException(KafkaException exception)
            => Directive.Stop;
        
        public virtual Directive OnException(Exception exception)
            => Directive.Stop;
    }
}