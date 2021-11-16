using System;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Supervision
{
    public class DefaultProducerDecider<TKey, TValue>
    {
        public Directive Decide(Exception e)
        {
            switch (e)
            {
                case ProduceException<TKey, TValue> pe:
                    if (pe.Error.IsFatal)
                        return Directive.Stop;
                    if (pe.Error.IsSerializationError())
                        return OnSerializationError(pe);
                    return OnProduceException(pe);
                
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

        public virtual Directive OnSerializationError(ProduceException<TKey, TValue> exception)
            => Directive.Stop;

        public virtual Directive OnProduceException(ProduceException<TKey, TValue> exception)
            => Directive.Stop;

        public virtual Directive OnKafkaException(KafkaException exception)
            => Directive.Stop;
        
        public virtual Directive OnException(Exception exception)
            => Directive.Stop;
    }
}