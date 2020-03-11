using System;
using System.Collections.Immutable;
using Akka.Streams.Kafka.Dsl;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Type assepted by "KafkaProducer.FlexiFlow"  with implementations
    /// 
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    /// <typeparam name="TPassThrough"></typeparam>
    public interface IEnvelope<K, V, out TPassThrough>
    {
        TPassThrough PassThrough { get; }
        IEnvelope<K, V, TPassThrough2> WithPassThrough<TPassThrough2>(TPassThrough2 value);
    }

    /// <summary>
    /// Contains methods to generate producer messages
    /// </summary>
    public static class ProducerMessage
    {
        /// <summary>
        /// Create a message containing the `record` and a `passThrough`.
        /// </summary>
        public static IEnvelope<K, V, TPassThrough> Single<K, V, TPassThrough>(ProducerRecord<K, V> record, TPassThrough passThrough) 
            => new Message<K,V,TPassThrough>(record, passThrough);
        
        /// <summary>
        /// Create a message containing the `record`.
        /// </summary>
        public static IEnvelope<K, V, NotUsed> Single<K, V>(ProducerRecord<K, V> record) 
            => new Message<K,V,NotUsed>(record, NotUsed.Instance);
        
        /// <summary>
        /// Create a multi-message containing several `records` and one `passThrough`.
        /// </summary>
        public static IEnvelope<K, V, TPassThrough> Multi<K, V, TPassThrough>(IImmutableSet<ProducerRecord<K, V>> records, TPassThrough passThrough)
            => new MultiMessage<K,V,TPassThrough>(records, passThrough); 
        
        /// <summary>
        /// Create a multi-message containing several `records`
        /// </summary>
        public static IEnvelope<K, V, NotUsed> Multi<K, V>(IImmutableSet<ProducerRecord<K, V>> records)
            => new MultiMessage<K,V,NotUsed>(records, NotUsed.Instance); 
        
        /// <summary>
        /// Create a pass-through message not containing any records.
        /// </summary>
        /// <remarks>
        /// In some cases the type parameters need to be specified explicitly.
        /// </remarks>
        public static IEnvelope<K, V, TPassThrough> PassThrough<K, V, TPassThrough>(TPassThrough passThrough) 
            => new PassThroughMessage<K,V,TPassThrough>(passThrough);
        
        /// <summary>
        /// Create a pass-through message not containing any records for use with `withContext` flows and sinks.
        /// </summary>
        /// <remarks>
        /// In some cases the type parameters need to be specified explicitly.
        /// </remarks>
        public static IEnvelope<K, V, NotUsed> PassThrough<K, V>() 
            => new PassThroughMessage<K,V,NotUsed>(NotUsed.Instance);
    }

    /// <summary>
    /// <para>
    /// <see cref="IEnvelope{K,V,TPassThrough}"/> implementation that produces a single message to a Kafka topic, flows emit
    /// a <see cref="Result{K,V,TPassThrough}"/> for every element processed.
    /// </para>
    /// <para>
    /// The `record` contains a topic name to which the record is being sent, an optional
    /// partition number, and an optional key and value.
    /// </para>
    /// <para>
    /// The `passThrough` field may hold any element that is passed through the `Producer.flow`
    /// and included in the <see cref="Result{K,V,TPassThrough}"/>. That is useful when some context is needed to be passed
    /// on downstream operations. That could be done with unzip/zip, but this is more convenient.
    /// It can for example be a <see cref="CommittableOffset"/> or <see cref="CommittableOffsetBatch"/> that can be committed later in the flow.
    /// </para>
    /// </summary>
    /// <typeparam name="K">The type of keys</typeparam>
    /// <typeparam name="V">The type of values</typeparam>
    /// <typeparam name="TPassThrough">The type of data passed through</typeparam>
    public sealed class Message<K, V, TPassThrough> : IEnvelope<K, V, TPassThrough>
    {
        /// <summary>
        /// Message
        /// </summary>
        public Message(ProducerRecord<K, V> record, TPassThrough passThrough)
        {
            Record = record;
            PassThrough = passThrough;
        }

        /// <summary>
        /// Published record
        /// </summary>
        public ProducerRecord<K, V> Record { get; }

        /// <inheritdoc />
        public TPassThrough PassThrough { get; }

        /// <inheritdoc />
        public IEnvelope<K, V, TPassThrough2> WithPassThrough<TPassThrough2>(TPassThrough2 value)
        {
            return new Message<K, V, TPassThrough2>(Record, value);
        }
    }

    /// <summary>
    /// <para>
    /// <see cref="IEnvelope{K,V,TPassThrough}"/> implementation that produces multiple message to a Kafka topics, flows emit
    /// a <see cref="MultiResult{K,V,TPassThrough}"/> for every element processed.
    /// </para>
    /// <para>
    /// Every element in `records` contains a topic name to which the record is being sent, an optional
    /// partition number, and an optional key and value.
    /// </para>
    /// <para>
    /// The `passThrough` field may hold any element that is passed through the `Producer.flow`
    /// and included in the <see cref="MultiResult{K,V,TPassThrough}"/>. That is useful when some context is needed to be passed
    /// on downstream operations. That could be done with unzip/zip, but this is more convenient.
    /// It can for example be a <see cref="CommittableOffset"/> or <see cref="CommittableOffsetBatch"/> that can be committed later in the flow.
    /// </para>
    /// </summary>
    public sealed class MultiMessage<K, V, TPassThrough> : IEnvelope<K, V, TPassThrough>
    {
        /// <summary>
        /// MultiMessage
        /// </summary>
        public MultiMessage(IImmutableSet<ProducerRecord<K, V>> records, TPassThrough passThrough)
        {
            Records = records;
            PassThrough = passThrough;
        }

        /// <summary>
        /// Records
        /// </summary>
        public IImmutableSet<ProducerRecord<K, V>> Records { get; }
        /// <summary>
        /// PassThrough
        /// </summary>
        public TPassThrough PassThrough { get; }

        /// <inheritdoc />
        public IEnvelope<K, V, TPassThrough2> WithPassThrough<TPassThrough2>(TPassThrough2 value)
        {
            return new MultiMessage<K, V, TPassThrough2>(Records, value);
        }
    }

    /// <summary>
    /// <para>
    /// <see cref="IEnvelope{K,V,TPassThrough}"/> implementation that does not produce anything to Kafka, flows emit
    /// a <see cref="PassThroughResult{K,V,TPassThrough}"/> for every element processed.
    /// </para>
    /// 
    /// <para>
    /// The `passThrough` field may hold any element that is passed through the `Producer.flow`
    /// and included in the <see cref="Result{K,V,TPassThrough}"/>. That is useful when some context is needed to be passed
    /// on downstream operations. That could be done with unzip/zip, but this is more convenient.
    /// It can for example be a <see cref="CommittableOffset"/> or <see cref="CommittableOffsetBatch"/> that can be committed later in the flow.
    /// </para>
    /// </summary>
    public sealed class PassThroughMessage<K, V, TPassThrough> : IEnvelope<K, V, TPassThrough>
    {
        /// <summary>
        /// PassThroughMessage
        /// </summary>
        public PassThroughMessage(TPassThrough passThrough)
        {
            PassThrough = passThrough;
        }

        /// <inheritdoc />
        public TPassThrough PassThrough { get; }

        /// <inheritdoc />
        public IEnvelope<K, V, TPassThrough2> WithPassThrough<TPassThrough2>(TPassThrough2 value)
        {
            return new PassThroughMessage<K, V, TPassThrough2>(value);
        }
    }

    /// <summary>
    /// Output type produced by <see cref="KafkaProducer.FlexiFlow{TKey,TValue,TPassThrough}(Akka.Streams.Kafka.Settings.ProducerSettings{TKey,TValue})"/>
    /// </summary>
    public interface IResults<K, V, out TPassThrough>
    {
        /// <summary>
        /// PassThrough
        /// </summary>
        TPassThrough PassThrough { get; }
    }
    
    /// <summary>
    /// <see cref="IResults{K,V,TPassThrough}"/> implementation emitted when a <see cref="Message"/> has been successfully published.
    ///
    /// Includes the original message, metadata returned from <see cref="Producer{TKey,TValue}"/> and the `offset` of the produced message.
    /// </summary>
    public sealed class Result<K, V, TPassThrough> : IResults<K, V, TPassThrough>
    {
        /// <summary>
        /// Message metadata
        /// </summary>
        public DeliveryReport<K, V> Metadata { get; }
        /// <summary>
        /// Message
        /// </summary>
        public Message<K, V, TPassThrough> Message { get; }
        /// <summary>
        /// Offset
        /// </summary>
        public Offset Offset => Metadata.Offset;
        /// <inheritdoc />
        public TPassThrough PassThrough => Message.PassThrough;

        /// <summary>
        /// Result
        /// </summary>
        public Result(DeliveryReport<K, V> metadata, Message<K, V, TPassThrough> message)
        {
            Metadata = metadata;
            Message = message;
        }
    }

    public sealed class MultiResultPart<K, V>
    {
        public DeliveryReport<K, V> Metadata { get; }
        public ProducerRecord<K, V> Record { get; }

        public MultiResultPart(DeliveryReport<K, V> metadata, ProducerRecord<K, V> record)
        {
            Metadata = metadata;
            Record = record;
        }
    }

    public sealed class MultiResult<K, V, TPassThrough> : IResults<K, V, TPassThrough>
    {
        public MultiResult(IImmutableSet<MultiResultPart<K, V>> parts, TPassThrough passThrough)
        {
            Parts = parts;
            PassThrough = passThrough;
        }

        public IImmutableSet<MultiResultPart<K, V>> Parts { get; }
        public TPassThrough PassThrough { get; }
    }

    /// <summary>
    /// <see cref="IResults{K,V,TPassThrough}"/> implementation emitted when <see cref="PassThroughMessage{K,V,TPassThrough}"/>
    /// has passed through the flow.
    /// </summary>
    public sealed class PassThroughResult<K, V, TPassThrough> : IResults<K, V, TPassThrough>
    {
        public PassThroughResult(TPassThrough passThrough)
        {
            PassThrough = passThrough;
        }

        public TPassThrough PassThrough { get; }
    }
}