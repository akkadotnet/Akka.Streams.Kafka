using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Pattern;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Stage;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Akka.Streams.Kafka
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
        private volatile bool inIsClosed;
        private readonly Producer<K, V> producer;
        private TaskCompletionSource<bool> completionState = new TaskCompletionSource<bool>();

        private Inlet In { get; }
        private Outlet Out { get; }

        public ProducerStageLogic(ProducerSettings<K, V> settings, Shape shape) : base(shape)
        {
            In = shape.Inlets.FirstOrDefault();
            Out = shape.Outlets.FirstOrDefault();
            producer = new Producer<K, V>(settings.Properties, settings.KeySerializer, settings.ValueSerializer);

            SetHandler(In, 
                onPush: () =>
                {
                    var msg = Grab<ProduceRecord<K, V>>(In);

                    var task = producer.ProduceAsync(msg.Topic, msg.Key, msg.Value).GetAwaiter().GetResult();
                    Console.WriteLine($"Partition: {task.Partition}, Offset: {task.Offset}");

                    Push(Out, Task.FromResult(new Result<K, V>(task, msg)));
                },
                onUpstreamFinish: () =>
                {
                    inIsClosed = true;
                    completionState.SetResult(true);
                    CheckForCompletion();
                },
                onUpstreamFailure: exception =>
                {
                    inIsClosed = true;
                    completionState.SetException(exception);
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
                if (!completionState.Task.IsFaulted && completionState.Task.Result)
                {
                    CompleteStage();
                }
                else if (completionState.Task.IsFaulted)
                {
                    FailStage(completionState.Task.Exception);
                }
                else
                {
                    FailStage(new IllegalStateException("Stage completed, but there is no info about status"));
                }
            }
        }

        public override void PostStop()
        {
            Log.Debug("Stage completed");

            producer.Flush(TimeSpan.FromSeconds(2));
            producer.Dispose();
            Log.Debug("Producer closed");

            base.PostStop();
        }
    }
}
