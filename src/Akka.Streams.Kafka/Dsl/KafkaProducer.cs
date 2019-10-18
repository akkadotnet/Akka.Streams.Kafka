using System;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Dsl
{
    /// <summary>
    /// Akka Stream connector for publishing messages to Kafka topics.
    /// </summary>
    public static class KafkaProducer
    {
        /// <summary>
        /// <para>
        /// Create a sink for publishing records to Kafka topics.
        /// </para>
        /// 
        /// <para>
        /// The <see cref="ProducerRecord{K,V}"/> contains the topic name to which the record is being sent, an optional
        /// partition number, and an optional key and value.
        /// </para>
        /// </summary>
        public static Sink<ProducerRecord<TKey, TValue>, Task> PlainSink<TKey, TValue>(ProducerSettings<TKey, TValue> settings)
        {
            return Flow
                .Create<ProducerRecord<TKey, TValue>>()
                .Select(record => new Message<TKey, TValue, NotUsed>(record, NotUsed.Instance) as IEnvelope<TKey, TValue, NotUsed>)
                .Via(FlexiFlow<TKey, TValue, NotUsed>(settings))
                .ToMaterialized(Sink.Ignore<IResults<TKey, TValue, NotUsed>>(), Keep.Right);
        }

        /// <summary>
        /// <para>
        /// Create a sink for publishing records to Kafka topics.
        /// </para>
        /// 
        /// <para>
        /// The <see cref="ProducerRecord{K,V}"/> contains the topic name to which the record is being sent, an optional
        /// partition number, and an optional key and value.
        /// </para>
        ///
        /// <para>
        /// Supports sharing a Kafka Producer instance.
        /// </para>
        /// </summary>
        public static Sink<ProducerRecord<TKey, TValue>, Task> PlainSink<TKey, TValue>(ProducerSettings<TKey, TValue> settings, IProducer<TKey, TValue> producer)
        {
            return Flow
                .Create<ProducerRecord<TKey, TValue>>()
                .Select(record => new Message<TKey, TValue, NotUsed>(record, NotUsed.Instance) as IEnvelope<TKey, TValue, NotUsed>)
                .Via(FlexiFlow<TKey, TValue, NotUsed>(settings, producer))
                .ToMaterialized(Sink.Ignore<IResults<TKey, TValue, NotUsed>>(), Keep.Right);
        }

        /// <summary>
        /// <para>
        /// Create a flow to conditionally publish records to Kafka topics and then pass it on.
        /// </para>
        ///
        /// <para>
        /// It publishes records to Kafka topics conditionally:
        /// <list type="bullet">
        ///    <item><description>
        ///         <see cref="Message{K,V,TPassThrough}"/> publishes a single message to its topic, and continues in the stream as <see cref="Result{K,V,TPassThrough}"/>
        ///     </description></item>
        ///     <item><description>
        ///         <see cref="MultiMessage{K,V,TPassThrough}"/> publishes all messages in its `records` field, and continues in the stream as <see cref="MultiResult{K,V,TPassThrough}"/>
        ///     </description></item>
        ///     <item><description>
        ///         <see cref="PassThroughMessage{K,V,TPassThrough}"/> does not publish anything, and continues in the stream as <see cref="PassThroughResult{K,V,TPassThrough}"/>
        ///     </description></item>
        /// </list>
        /// </para>
        ///
        /// <para>
        /// The messages support the possibility to pass through arbitrary data, which can for example be a <see cref="CommittableOffset"/>
        /// or <see cref="CommittableOffsetBatch"/> that can be committed later in the flow.
        /// </para>
        /// </summary>
        public static Flow<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>, NotUsed> FlexiFlow<TKey, TValue, TPassThrough>(
            ProducerSettings<TKey, TValue> settings)
        {
            var flow = Flow.FromGraph(new DefaultProducerStage<TKey, TValue, TPassThrough, IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>>(
                    settings,
                    closeProducerOnStop: true))
                .SelectAsync(settings.Parallelism, x => x);

            return string.IsNullOrEmpty(settings.DispatcherId) 
                ? flow
                : flow.WithAttributes(ActorAttributes.CreateDispatcher(settings.DispatcherId));
        }

        /// <summary>
        /// <para>
        /// Create a flow to conditionally publish records to Kafka topics and then pass it on.
        /// </para>
        ///
        /// <para>
        /// It publishes records to Kafka topics conditionally:
        /// <list type="bullet">
        ///    <item><description>
        ///         <see cref="Message{K,V,TPassThrough}"/> publishes a single message to its topic, and continues in the stream as <see cref="Result{K,V,TPassThrough}"/>
        ///     </description></item>
        ///     <item><description>
        ///         <see cref="MultiMessage{K,V,TPassThrough}"/> publishes all messages in its `records` field, and continues in the stream as <see cref="MultiResult{K,V,TPassThrough}"/>
        ///     </description></item>
        ///     <item><description>
        ///         <see cref="PassThroughMessage{K,V,TPassThrough}"/> does not publish anything, and continues in the stream as <see cref="PassThroughResult{K,V,TPassThrough}"/>
        ///     </description></item>
        /// </list>
        /// </para>
        ///
        /// <para>
        /// The messages support the possibility to pass through arbitrary data, which can for example be a <see cref="CommittableOffset"/>
        /// or <see cref="CommittableOffsetBatch"/> that can be committed later in the flow.
        /// </para>
        ///
        /// <para>
        /// Supports sharing a Kafka Producer instance.
        /// </para>
        /// </summary>
        public static Flow<IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>, NotUsed> FlexiFlow<TKey, TValue, TPassThrough>(
            ProducerSettings<TKey, TValue> settings, IProducer<TKey, TValue> producer)
        {
            var flow = Flow.FromGraph(new DefaultProducerStage<TKey, TValue, TPassThrough, IEnvelope<TKey, TValue, TPassThrough>, IResults<TKey, TValue, TPassThrough>>(
                    settings,
                    closeProducerOnStop: false,
                    customProducerProvider: () => producer))
                .SelectAsync(settings.Parallelism, x => x);

            return string.IsNullOrEmpty(settings.DispatcherId) 
                ? flow
                : flow.WithAttributes(ActorAttributes.CreateDispatcher(settings.DispatcherId));
        }

        // TODO
        public static FlowWithContext<IEnvelope<K, V, NotUsed>, C, DeliveryReport<K, V>, C, NotUsed> FlowWithContext<K, V, C>(ProducerSettings<K, V> settings)
        {
            throw new NotImplementedException();
        }
    }
}
