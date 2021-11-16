using System;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Supervision
{
    /// <summary>
    /// Default Supervision decider used internally inside all producer sink stages.
    ///
    /// You can extend and override the virtual methods in this class to provide your own
    /// custom supervision decider. 
    /// </summary>
    public class DefaultProducerDecider<TKey, TValue>
    {
        /// <summary>
        /// The delegate method that is passed to the ActorAttributes.CreateSupervisionStrategy() method 
        /// </summary>
        /// <param name="exception">The exception thrown</param>
        /// <returns><see cref="Directive"/>></returns>
        public Directive Decide(Exception exception)
        {
            switch (exception)
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
                
                case var ex:
                    return OnException(ex);
            }
        }

        /// <summary>
        /// Decider for all serialization exceptions raised by the underlying Kafka producer
        /// </summary>
        /// <param name="exception"><see cref="ProduceException{TKey,TValue}"/> thrown when producer produced</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnSerializationError(ProduceException<TKey, TValue> exception)
            => Directive.Stop;

        /// <summary>
        /// Decider for all produce exceptions raised by the underlying Kafka producer
        /// </summary>
        /// <param name="exception"><see cref="ProduceException{TKey,TValue}"/> thrown when producer produced</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnProduceException(ProduceException<TKey, TValue> exception)
            => Directive.Stop;

        /// <summary>
        /// Decider for all kafka exceptions raised by the underlying Kafka producer
        /// </summary>
        /// <param name="exception"><see cref="KafkaException"/> thrown when producer produced</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnKafkaException(KafkaException exception)
            => Directive.Stop;
        
        /// <summary>
        /// Decider for all other exceptions raised by the underlying Kafka producer
        /// </summary>
        /// <param name="exception"><see cref="KafkaException"/> thrown when producer produced</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnException(Exception exception)
            => Directive.Stop;
    }
}