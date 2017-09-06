using System;
using System.Linq;
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
        private Inlet<ProduceRecord<K, V>> In { get; } = new Inlet<ProduceRecord<K, V>>("kafka.producer.in");
        private Outlet<Task<Message<K, V>>> Out { get; } = new Outlet<Task<Message<K, V>>>("kafka.producer.out");

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
        private Producer<K, V> _producer;
        private readonly TaskCompletionSource<NotUsed> _completionState = new TaskCompletionSource<NotUsed>();
        private Action<ProduceRecord<K, V>> _sendToProducer;
        private readonly ProducerSettings<K, V> _settings;

        private Inlet In { get; }
        private Outlet Out { get; }

        public ProducerStageLogic(ProducerSettings<K, V> settings, Shape shape) : base(shape)
        {
            In = shape.Inlets.FirstOrDefault();
            Out = shape.Outlets.FirstOrDefault();
            _settings = settings;

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

        public override void PreStart()
        {
            base.PreStart();

            _producer = _settings.CreateKafkaProducer();

            Log.Debug($"Producer started: {_producer.Name}");

            _producer.OnError += OnProducerError;

            _sendToProducer = msg =>
            {
                var task = _producer.ProduceAsync(msg.Topic, msg.Key, msg.Value, msg.PartitionId);
                Push(Out, task);
            };
        }

        public override void PostStop()
        {
            _producer.Flush(TimeSpan.FromSeconds(2));
            _producer.Dispose();
            Log.Debug($"Producer stopped: {_producer.Name}");

            base.PostStop();
        }

        private void OnProducerError(object sender, Error error)
        {
            Log.Error(error.Reason);

            if (!KafkaExtensions.IsBrokerErrorRetriable(error) && !KafkaExtensions.IsLocalErrorRetriable(error))
            {
                FailStage(new Exception(error.Reason));
            }
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
    }
}
