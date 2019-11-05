using System;
using Akka.Annotations;
using Akka.Streams.Dsl;

namespace Akka.Streams.Kafka.Extensions
{
    public static class FlowExtensions
    {
        // TODO: Move this to Akka.Streams core library
        /// <summary>
        /// API MAY CHANGE
        /// 
        /// Turns a Flow into a FlowWithContext which manages a context per element along a stream.
        /// </summary>
        /// <param name="flow">Flow to convert</param>
        /// <param name="collapseContext">Turn each incoming pair of element and context value into an element of passed Flow</param>
        /// <param name="extractContext">Turn each outgoing element of passed Flow into an outgoing context value</param>
        /// <typeparam name="TIn">New flow incoming elements type</typeparam>
        /// <typeparam name="TCtxIn">New flow incoming elements context</typeparam>
        /// <typeparam name="TOut">Out elements type</typeparam>
        /// <typeparam name="TCtxOut">Resulting context type</typeparam>
        /// <typeparam name="TMat">Materialized value type</typeparam>
        /// <typeparam name="TIn2">Type of passed flow elements</typeparam>
        [ApiMayChange]
        public static FlowWithContext<TCtxIn, TIn, TCtxOut, TOut, TMat> AsFlowWithContext<TCtxIn, TIn, TCtxOut, TOut, TMat, TIn2>(
            this Flow<TIn2, TOut, TMat> flow,
            Func<TIn, TCtxIn, TIn2> collapseContext,
            Func<TOut, TCtxOut> extractContext)
        {
            var flowWithTuples = Flow.Create<(TIn, TCtxIn)>()
                .Select(pair => collapseContext(pair.Item1, pair.Item2))
                .ViaMaterialized(flow, Keep.Right)
                .Select(e => (e, extractContext(e)));

            return FlowWithContext.From(flowWithTuples);
        }
    }
}