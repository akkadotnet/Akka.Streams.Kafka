using System;
using System.Threading.Tasks;
using Akka.Annotations;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// Implements committing flows
    /// </summary>
    public static class Committer
    {
        /// <summary>
        /// Batches offsets and commits them to Kafka, emits <see cref="CommittableOffsetBatch"/> for every committed batch.
        /// </summary>
        public static Flow<ICommittable, ICommittableOffsetBatch, NotUsed> BatchFlow(CommitterSettings settings)
        {
            return Akka.Streams.Dsl.Flow.Create<ICommittable>().GroupedWithin(settings.MaxBatch, settings.MaxInterval)
                .Select(CommittableOffsetBatch.Create)
                .SelectAsync(settings.Parallelism, async batch =>
                {
                   await batch.Commit();
                   return batch;
                });
        }

        /// <summary>
        /// Batches offsets and commits them to Kafka, emits <see cref="Done.Instance"/> for every committed batch.
        /// </summary>
        public static Flow<ICommittable, Done, NotUsed> Flow(CommitterSettings settings)
        {
            return BatchFlow(settings).Select(_ => Done.Instance);
        }

        /// <summary>
        /// API MAY CHANGE
        /// 
        /// Batches offsets from context and commits them to Kafka, emits no useful value,
        /// but keeps the committed <see cref="ICommittableOffsetBatch"/> as context
        /// </summary>
        [ApiMayChange]
        public static FlowWithContext<ICommittableOffset, E, ICommittableOffsetBatch, NotUsed, NotUsed> FlowWithOffsetContext<E>(CommitterSettings settings)
        {
            var value = Akka.Streams.Dsl.Flow.Create<(E, ICommittableOffset)>()
                .Select(m => m.Item2 as ICommittable)
                .Via(BatchFlow(settings))
                .Select(b => (NotUsed.Instance, b));

            return FlowWithContext.From(value);
        }

        /// <summary>
        /// Batches offsets and commits them to Kafka.
        /// </summary>
        public static Sink<ICommittable, Task> Sink(CommitterSettings settings)
        {
            return Flow(settings).ToMaterialized(Streams.Dsl.Sink.Ignore<Done>(), Keep.Right);
        }

        /// <summary>
        /// API MAY CHANGE
        /// 
        /// Batches offsets from context and commits them to Kafka.
        /// </summary>
        [ApiMayChange]
        public static Sink<(E, ICommittableOffset), Task> SinkWithOffsetContext<E>(CommitterSettings settings)
        {
            return Akka.Streams.Dsl.Flow.Create<(E, ICommittableOffset)>()
                .Via(FlowWithOffsetContext<E>(settings))
                .ToMaterialized(Streams.Dsl.Sink.Ignore<(NotUsed, ICommittableOffsetBatch)>(), Keep.Right);
        }
    }
}