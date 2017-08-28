using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    internal sealed class ProducerStage<K, V> : GraphStage<FlowShape<ProduceRecord<K, V>, Task<Result<K, V>>>>
    {
        private readonly ProducerSettings<K, V> _settings;
        private Inlet<ProduceRecord<K, V>> In { get; } = new Inlet<ProduceRecord<K, V>>("messages");
        private Outlet<Task<Result<K, V>>> Out { get; } = new Outlet<Task<Result<K, V>>>("result");

        public ProducerStage(ProducerSettings<K, V> settings)
        {
            _settings = settings;
            Shape = new FlowShape<ProduceRecord<K, V>, Task<Result<K, V>>>(In, Out);
        }

        public override FlowShape<ProduceRecord<K, V>, Task<Result<K, V>>> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new ProducerStageLogic<K, V>(_settings, Shape);
    }

    internal sealed class ProducerStageLogic<K, V> : GraphStageLogic
    {
        private volatile bool _isClosed;
        private readonly Producer<K, V> _producer;
        private readonly TaskCompletionSource<NotUsed> _completionState = new TaskCompletionSource<NotUsed>();

        private Inlet In { get; }
        private Outlet Out { get; }

        public ProducerStageLogic(ProducerSettings<K, V> settings, Shape shape) : base(shape)
        {
            In = shape.Inlets.FirstOrDefault();
            Out = shape.Outlets.FirstOrDefault();
            _producer = new Producer<K, V>(settings.Properties, settings.KeySerializer, settings.ValueSerializer);

            SetHandler(In, 
                onPush: () =>
                {
                    var msg = Grab<ProduceRecord<K, V>>(In);

                    var result = SendToProducer(msg);
                    Push(Out, result);
                },
                onUpstreamFinish: () =>
                {
                    _isClosed = true;
                    _completionState.SetResult(NotUsed.Instance);
                    CheckForCompletion();
                },
                onUpstreamFailure: exception =>
                {
                    _isClosed = true;
                    _completionState.SetException(exception);
                    CheckForCompletion();
                });

            SetHandler(Out, onPull: () =>
            {
                TryPull(In);
            });
        }

        public void CheckForCompletion()
        {
            if (IsClosed(In))
            {
                var completionTask = _completionState.Task;

                if (completionTask.IsFaulted || completionTask.IsCanceled)
                {
                    FailStage(completionTask.Exception);
                }
                else if (completionTask.IsCompleted)
                {
                    CompleteStage();
                }
            }
        }

        private async Task<Result<K, V>> SendToProducer(ProduceRecord<K, V> msg)
        {
            var task = await _producer.ProduceAsync(msg.Topic, msg.Key, msg.Value);
            return new Result<K, V>(task, msg);
        }

        public override void PostStop()
        {
            Log.Debug("Stage completed");

            _producer.Flush(TimeSpan.FromSeconds(2));
            _producer.Dispose();
            Log.Debug("Producer closed");

            base.PostStop();
        }
    }
}
