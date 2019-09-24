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
            return Flow.Create<ICommittable>().GroupedWithin(settings.MaxBatch, settings.MaxInterval)
                .Select(CommittableOffsetBatch.Create)
                .SelectAsync(settings.Parallelism, async batch =>
                {
                   await batch.Commit();
                   return batch;
                });
        }
    }
}