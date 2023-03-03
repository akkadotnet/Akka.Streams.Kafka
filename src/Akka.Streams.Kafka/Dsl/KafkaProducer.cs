using System;
using System.Threading.Tasks;
using Akka.Annotations;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Extensions;
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
        public static Sink<ProducerRecord<TKey, TValue>, Task<Done>> PlainSink<TKey, TValue>(ProducerSettings<TKey, TValue> settings)
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
        public static Sink<ProducerRecord<TKey, TValue>, Task<Done>> PlainSink<TKey, TValue>(ProducerSettings<TKey, TValue> settings, IProducer<TKey, TValue> producer)
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

            return FlowWithDispatcher(settings, flow);
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

            return FlowWithDispatcher(settings, flow);
        }

       
        /// <summary>
        /// <para>
        /// API MAY CHANGE
        /// </para>
        /// <para>
        /// Create a flow to conditionally publish records to Kafka topics and then pass it on.
        /// </para>
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
        /// <para>
        /// This flow is intended to be used with Akka's 
        /// <see cref="KafkaProducer.FlowWithContext{K,V,C}(ProducerSettings{K,V})"/> or
        /// <see cref="KafkaProducer.FlowWithContext{K,V,C}(ProducerSettings{K,V}, IProducer{K,V})"/>
        /// </para>
        /// </summary>
        /// <param name="settings"></param>
        /// <typeparam name="K">Keys type</typeparam>
        /// <typeparam name="V">Values type</typeparam>
        /// <typeparam name="C">Flow context type</typeparam>
        [ApiMayChange]
        public static FlowWithContext<IEnvelope<K, V, NotUsed>, C, IResults<K, V, C>, C, NotUsed> FlowWithContext<K, V, C>(ProducerSettings<K, V> settings)
        {
            return FlexiFlow<K, V, C>(settings).AsFlowWithContext<IEnvelope<K, V, NotUsed>, C, IResults<K, V, C>, C, NotUsed, IEnvelope<K, V, C>>(
                collapseContext: (env, c) => env.WithPassThrough(c), 
                extractContext: res => res.PassThrough);
        }

        /// <summary>
        /// <para>
        /// API MAY CHANGE
        /// </para>
        /// <para>
        /// Create a flow to conditionally publish records to Kafka topics and then pass it on.
        /// </para>
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
        /// <para>
        /// This flow is intended to be used with Akka's 
        /// <see cref="KafkaProducer.FlowWithContext{K,V,C}(ProducerSettings{K,V})"/> or
        /// <see cref="KafkaProducer.FlowWithContext{K,V,C}(ProducerSettings{K,V}, IProducer{K,V})"/>
        /// </para>
        /// </summary>
        /// <param name="settings">Producer settings</param>
        /// <param name="producer">Producer instance to reuse</param>
        /// <typeparam name="K">Keys type</typeparam>
        /// <typeparam name="V">Values type</typeparam>
        /// <typeparam name="C">Flow context type</typeparam>
        [ApiMayChange]
        public static FlowWithContext<IEnvelope<K, V, NotUsed>, C, IResults<K, V, C>, C, NotUsed> FlowWithContext<K, V, C>(ProducerSettings<K, V> settings, IProducer<K, V> producer)
        {
            return FlexiFlow<K, V, C>(settings, producer).AsFlowWithContext<IEnvelope<K, V, NotUsed>, C, IResults<K, V, C>, C, NotUsed, IEnvelope<K, V, C>>(
                collapseContext: (env, c) => env.WithPassThrough(c), 
                extractContext: res => res.PassThrough);
        }

        /// <summary>
        /// API IS FOR INTERNAL USAGE: see https://github.com/akkadotnet/Akka.Streams.Kafka/issues/85
        /// 
        /// Publish records to Kafka topics and then continue the flow. The flow can only be used with a <see cref="KafkaConsumer.TransactionalSource{K,V}"/> that
        /// emits a <see cref="TransactionalMessage{K,V}"/>. The flow requires a unique `transactional.id` across all app
        /// instances.  The flow will override producer properties to enable Kafka exactly-once transactional support.
        /// </summary>
        [InternalApi]
        public static Flow<IEnvelope<K, V, GroupTopicPartitionOffset>, IResults<K, V, GroupTopicPartitionOffset>, NotUsed> TransactionalFlow<K, V>(
                ProducerSettings<K, V> setting,
                string transactionalId)
        {
            if (string.IsNullOrEmpty(transactionalId))
                throw new ArgumentException("You must define a Transactional id");

            var transactionalSettings = setting
                .WithProperty("enable.idempotence", "true")
                .WithProperty("transactional.id", transactionalId)
                .WithProperty("max.in.flight.requests.per.connection", "1");

            var flow = Flow.FromGraph(new TransactionalProducerStage<K, V, GroupTopicPartitionOffset>(closeProducerOnStop: true, settings: transactionalSettings))
                .SelectAsync(transactionalSettings.Parallelism, message => message);
            
            return FlowWithDispatcher(transactionalSettings, flow);
        }

        /// <summary>
        /// API IS FOR INTERNAL USAGE: see https://github.com/akkadotnet/Akka.Streams.Kafka/issues/85
        /// 
        /// Sink that is aware of the <see cref="TransactionalMessage{K,V}.PartitionOffset"/> from a <see cref="KafkaConsumer.TransactionalSource{K,V}"/>.
        /// It will initialize, begin, produce, and commit the consumer offset as part of a transaction.
        /// </summary>
        [InternalApi]
        public static Sink<IEnvelope<K, V, GroupTopicPartitionOffset>, Task<Done>> TransactionalSink<K, V>(
            ProducerSettings<K, V> settings, 
            string transactionalId)
        {
            return TransactionalFlow(settings, transactionalId).ToMaterialized(Sink.Ignore<IResults<K, V, GroupTopicPartitionOffset>>(), Keep.Right);
        }

        private static Flow<IEnvelope<K, V, TPassThrough>, IResults<K, V, TPassThrough>, NotUsed> FlowWithDispatcher<K, V, TPassThrough>(
            ProducerSettings<K, V> settings,
            Flow<IEnvelope<K, V, TPassThrough>, IResults<K, V, TPassThrough>, NotUsed> flow)
        {
            return string.IsNullOrEmpty(settings.DispatcherId) 
                ? flow
                : flow.WithAttributes(ActorAttributes.CreateDispatcher(settings.DispatcherId));
        }
    }
}
