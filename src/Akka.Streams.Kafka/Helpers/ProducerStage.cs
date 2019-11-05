using System;
using System.Threading.Tasks;
using Akka.Annotations;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// INTERNAL API
    ///
    /// Implemented by <see cref="DefaultProducerStage{K,V,P,TIn,TOut}"/> as <see cref="TransactionalProducerStage{K,V,P,TIn,TOut}"/>
    /// </summary>
    [InternalApi]
    public interface IProducerStage<K, V, P, TIn, TOut> where TIn : IEnvelope<K, V, P> where TOut : IResults<K, V, P>
    {
        TimeSpan FlushTimeout { get; }
        bool CloseProducerOnStop { get; }
        Func<Action<IProducer<K, V>, Error>, IProducer<K, V>> ProducerProvider { get; }
        
        Inlet<TIn> In { get; }
        Outlet<Task<TOut>> Out { get; }
        FlowShape<TIn, Task<TOut>> Shape { get; }
    }
    
    /// <summary>
    /// INTERNAL API
    ///
    /// Used to specify custom producer completion events handling
    /// </summary>
    [InternalApi]
    public interface IProducerCompletionState
    {
        /// <summary>
        /// Called when completion succeed
        /// </summary>
        void OnCompletionSuccess();
        /// <summary>
        /// Called on completion failure
        /// </summary>
        void OnCompletionFailure(Exception ex);
    }
    
}