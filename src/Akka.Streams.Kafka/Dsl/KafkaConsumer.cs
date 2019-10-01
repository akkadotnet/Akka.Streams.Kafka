using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages;
using Confluent.Kafka;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers;
using Akka.Streams.Kafka.Stages.Consumers.Concrete;

namespace Akka.Streams.Kafka.Dsl
{
    /// <summary>
    /// Akka Stream connector for subscribing to Kafka topics.
    /// </summary>
    public static class KafkaConsumer
    {
        /// <summary>
        /// The <see cref="PlainSource{K,V}"/> emits <see cref="ConsumerRecord"/> elements (as received from the underlying 
        /// <see cref="IConsumer{TKey,TValue}"/>). It has no support for committing offsets to Kafka. It can be used when the
        /// offset is stored externally or with auto-commit (note that auto-commit is by default disabled).
        /// The consumer application doesn't need to use Kafka's built-in offset storage and can store offsets in a store of its own
        /// choosing. The primary use case for this is allowing the application to store both the offset and the results of the
        /// consumption in the same system in a way that both the results and offsets are stored atomically.This is not always
        /// possible, but when it is, it will make the consumption fully atomic and give "exactly once" semantics that are
        /// stronger than the "at-least once" semantics you get with Kafka's offset commit functionality.
        /// </summary>
        public static Source<ConsumeResult<K, V>, Task> PlainSource<K, V>(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            return Source.FromGraph(new PlainSourceStage<K, V>(settings, subscription));
        }

        /// <summary>
        /// Special source that can use an external `KafkaAsyncConsumer`. This is useful when you have
        /// a lot of manually assigned topic-partitions and want to keep only one kafka consumer.
        /// </summary>
        public static Source<ConsumeResult<K, V>, Task> PlainExternalSource<K, V>(IActorRef consumer, IManualSubscription subscription)
        {
            return Source.FromGraph(new ExternalPlainSourceStage<K, V>(consumer, subscription));
        }

        /// <summary>
        /// The <see cref="CommittableSource{K,V}"/> makes it possible to commit offset positions to Kafka.
        /// This is useful when "at-least once delivery" is desired, as each message will likely be
        /// delivered one time but in failure cases could be duplicated.
        /// Compared to auto-commit, this gives exact control over when a message is considered consumed.
        /// If you need to store offsets in anything other than Kafka, <see cref="PlainSource{K,V}"/> should
        /// be used instead of this API.
        /// </summary>
        public static Source<CommittableMessage<K, V>, Task> CommittableSource<K, V>(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            return Source.FromGraph(new CommittableSourceStage<K, V>(settings, subscription));
        }

        /// <summary>
        /// API MAY CHANGE
        ///
        /// This source emits <see cref="ConsumeResult{TKey,TValue}"/> together with the offset position as flow context, thus makes it possible
        /// to commit offset positions to Kafka.
        /// This is useful when "at-least once delivery" is desired, as each message will likely be
        /// delivered one time but in failure cases could be duplicated.
        ///
        /// It is intended to be used with Akka's [flow with context](https://doc.akka.io/docs/akka/current/stream/operators/Flow/asFlowWithContext.html),
        /// <see cref="KafkaProducer.FlowWithContext{K,V,C}"/> and/or <see cref="Committer.SinkWithOffsetContext{E}"/>
        /// </summary>
        [ApiMayChange]
        public static SourceWithContext<ICommittableOffset, ConsumeResult<K, V>, Task> SourceWithOffsetContext<K, V>(
            ConsumerSettings<K, V> settings, ISubscription subscription, Func<ConsumeResult<K, V>, string> metadataFromRecord = null)
        {
            return Source.FromGraph(new SourceWithOffsetContextStage<K, V>(settings, subscription, metadataFromRecord))
                .AsSourceWithContext(m => m.Item2)
                .Select(m => m.Item1);
        }
        
        /// <summary>
        /// The same as <see cref="PlainExternalSource{K,V}"/> but for offset commit support
        /// </summary>
        public static Source<CommittableMessage<K, V>, Task> CommittableExternalSource<K, V>(IActorRef consumer, IManualSubscription subscription, 
                                                                                       string groupId, TimeSpan commitTimeout)
        {
            return Source.FromGraph<CommittableMessage<K, V>, Task>(new ExternalCommittableSourceStage<K, V>(consumer, groupId, commitTimeout, subscription));
        }

        /// <summary>
        /// Convenience for "at-most once delivery" semantics.
        /// The offset of each message is committed to Kafka before being emitted downstream.
        /// </summary>
        public static Source<ConsumeResult<K, V>, Task> AtMostOnceSource<K, V>(ConsumerSettings<K, V> settings, ISubscription subscription)
        {
            return CommittableSource(settings, subscription).SelectAsync(1, async message =>
            {
               await message.CommitableOffset.Commit();
               return message.Record;
            });
        }
    }
}
