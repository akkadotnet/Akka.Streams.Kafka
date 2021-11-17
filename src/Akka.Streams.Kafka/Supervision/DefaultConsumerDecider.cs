using System;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Supervision
{
    /// <summary>
    /// Default Supervision decider used internally inside all consumer source stages.
    ///
    /// You can extend and override the virtual methods in this class to provide your own
    /// custom supervision decider. 
    /// </summary>
    public class DefaultConsumerDecider
    {
        private readonly bool _autoCreateTopics;

        public DefaultConsumerDecider(bool autoCreateTopics)
        {
            _autoCreateTopics = autoCreateTopics;
        }
        
        /// <summary>
        /// The delegate method that is passed to the ActorAttributes.CreateSupervisionStrategy() method 
        /// </summary>
        /// <param name="exception">The exception thrown</param>
        /// <returns><see cref="Directive"/>></returns>
        public Directive Decide(Exception exception)
        {
            switch (exception)
            {
                case ConsumeException ce:
                    if (ce.Error.IsFatal)
                        return Directive.Stop;
                    
                    // Workaround for https://github.com/confluentinc/confluent-kafka-dotnet/issues/1366
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
                
                case var ex:
                    return OnException(ex);
            }
        }

        /// <summary>
        /// Decider for all deserialization exceptions raised by the underlying Kafka consumer
        /// </summary>
        /// <param name="exception"><see cref="ConsumeException"/> thrown when consumer was consumed</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnDeserializationError(ConsumeException exception)
            => Directive.Stop;

        /// <summary>
        /// Decider for consume exceptions raised by the underlying Kafka consumer
        /// </summary>
        /// <param name="exception"><see cref="ConsumeException"/> thrown when consumer was consumed</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnConsumeException(ConsumeException exception)
            => Directive.Resume;

        /// <summary>
        /// Decider for all kafka exceptions raised by the underlying Kafka consumer
        /// </summary>
        /// <param name="exception"><see cref="KafkaException"/> thrown when consumer was consumed</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnKafkaException(KafkaException exception)
            => Directive.Resume;
        
        /// <summary>
        /// Decider for all other exceptions raised by the underlying Kafka consumer
        /// </summary>
        /// <param name="exception"><see cref="Exception"/> thrown when consumer was consumed</param>
        /// <returns><see cref="Directive"/>></returns>
        protected virtual Directive OnException(Exception exception)
            => Directive.Stop;
    }
}