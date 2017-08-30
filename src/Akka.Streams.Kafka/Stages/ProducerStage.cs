using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    internal sealed class ProducerStage<K, V> : GraphStage<FlowShape<ProduceRecord<K, V>, Task<Message<K, V>>>>
    {
        private readonly ProducerSettings<K, V> _settings;
        private Inlet<ProduceRecord<K, V>> In { get; } = new Inlet<ProduceRecord<K, V>>("messages");
        private Outlet<Task<Message<K, V>>> Out { get; } = new Outlet<Task<Message<K, V>>>("result");

        public ProducerStage(ProducerSettings<K, V> settings)
        {
            _settings = settings;
            Shape = new FlowShape<ProduceRecord<K, V>, Task<Message<K, V>>>(In, Out);
        }

        public override FlowShape<ProduceRecord<K, V>, Task<Message<K, V>>> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new ProducerStageLogic<K, V>(_settings, Shape);
    }

    internal sealed class ProducerStageLogic<K, V> : GraphStageLogic
    {
        private readonly Producer<K, V> _producer;
        private readonly TaskCompletionSource<NotUsed> _completionState = new TaskCompletionSource<NotUsed>();
        private Action<ProduceRecord<K, V>> _sendToProducer;

        private Inlet In { get; }
        private Outlet Out { get; }

        public ProducerStageLogic(ProducerSettings<K, V> settings, Shape shape) : base(shape)
        {
            In = shape.Inlets.FirstOrDefault();
            Out = shape.Outlets.FirstOrDefault();
            _producer = new Producer<K, V>(settings.Properties, settings.KeySerializer, settings.ValueSerializer);
            _producer.OnError += OnProducerError;

            SetHandler(In, 
                onPush: () =>
                {
                    var msg = Grab<ProduceRecord<K, V>>(In);
                    _sendToProducer.Invoke(msg);
                },
                onUpstreamFinish: () =>
                {
                    _completionState.SetResult(NotUsed.Instance);
                    _producer.Flush(TimeSpan.FromSeconds(2));
                    CheckForCompletion();
                },
                onUpstreamFailure: exception =>
                {
                    _completionState.SetException(exception);
                    CheckForCompletion();
                });

            SetHandler(Out, onPull: () =>
            {
                TryPull(In);
            });
        }

        private void OnProducerError(object sender, Error error)
        {
            if (error.Code == ErrorCode.Local_Transport)
            {
                FailStage(new Exception(error.Reason));
            }

            // TODO: what else errors to handle?
            Log.Error(error.Reason);
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

        public override void PreStart()
        {
            base.PreStart();

            _sendToProducer = msg =>
            {
                var task = _producer.ProduceAsync(msg.Topic, msg.Key, msg.Value);
                Push(Out, task);
            };
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
