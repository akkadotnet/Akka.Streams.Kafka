using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;

namespace Akka.Streams.Kafka.Dsl
{
    public static class Producer
    {
        public static Sink<ProduceRecord<TKey, TValue>, Task> PlainSink<TKey, TValue>(ProducerSettings<TKey, TValue> settings)
        {
            return Flow
                .Create<ProduceRecord<TKey, TValue>>()
                .Via(CreateFlow(settings))
                .ToMaterialized(Sink.Ignore<Task<Result<TKey, TValue>>>(), Keep.Right);
        }

        // TODO: work on naming
        public static Flow<ProduceRecord<TKey, TValue>, Task<Result<TKey, TValue>>, NotUsed> CreateFlow<TKey, TValue>(ProducerSettings<TKey, TValue> settings)
        {
            return Flow.FromGraph(new ProducerStage<TKey, TValue>(settings));
        }
    }
}
