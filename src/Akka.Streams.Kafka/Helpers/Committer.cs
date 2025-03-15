using System;
using System.Threading.Tasks;
using Akka.Annotations;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers;

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
            return Akka.Streams.Dsl.Flow.FromGraph(new CommitCollectorStage(settings))
                .SelectAsyncUnordered(settings.Parallelism, async batch =>
                {
                    await ((CommittableOffsetBatch)batch).Commit();
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
        public static FlowWithContext<E, ICommittableOffset, NotUsed, ICommittableOffsetBatch, NotUsed> FlowWithOffsetContext<E>(CommitterSettings settings)
        {
            var value = Akka.Streams.Dsl.Flow.Create<(E, ICommittableOffset)>()
                .Select(ICommittable (m) => m.Item2)
                .Via(BatchFlow(settings))
                .Select(b => (NotUsed.Instance, b));

            return FlowWithContext.From(value);
        }

        /// <summary>
        /// Batches offsets and commits them to Kafka.
        /// </summary>
        public static Sink<ICommittable, Task<Done>> Sink(CommitterSettings settings)
        {
            return Flow(settings).ToMaterialized(Streams.Dsl.Sink.Ignore<Done>(), Keep.Right);
        }

        /// <summary>
        /// API MAY CHANGE
        /// 
        /// Batches offsets from context and commits them to Kafka.
        /// </summary>
        /// <typeparam name="E">Incoming flow elements type</typeparam>
        [ApiMayChange]
        public static Sink<(E, ICommittableOffset), Task<Done>> SinkWithOffsetContext<E>(CommitterSettings settings)
        {
            return Akka.Streams.Dsl.Flow.Create<(E, ICommittableOffset)>()
                .Via(FlowWithOffsetContext<E>(settings))
                .ToMaterialized(Streams.Dsl.Sink.Ignore<(NotUsed, ICommittableOffsetBatch)>(), Keep.Right);
        }
    }
}