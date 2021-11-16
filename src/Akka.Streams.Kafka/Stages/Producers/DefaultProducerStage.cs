using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Pattern;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Supervision;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages
{
    internal sealed class DefaultProducerStage<K, V, P, TIn, TOut> : GraphStage<FlowShape<TIn, Task<TOut>>>, IProducerStage<K, V, P, TIn, TOut> 
        where TIn: IEnvelope<K, V, P>
        where TOut: IResults<K, V, P>
    {
        public Func<Action<IProducer<K, V>, Error>, IProducer<K, V>> ProducerProvider { get; }
        public ProducerSettings<K, V> Settings { get; }
        public TimeSpan FlushTimeout => Settings.FlushTimeout;
        public bool CloseProducerOnStop { get; }
        public Inlet<TIn> In { get; } = new Inlet<TIn>("kafka.producer.in");
        public Outlet<Task<TOut>> Out { get; } = new Outlet<Task<TOut>>("kafka.producer.out");
        public override FlowShape<TIn, Task<TOut>> Shape { get; }

        /// <inheritdoc />
        protected override Attributes InitialAttributes
            => ActorAttributes.CreateSupervisionStrategy(new DefaultProducerDecider<K, V>().Decide);

        public DefaultProducerStage(
            ProducerSettings<K, V> settings,
            bool closeProducerOnStop,
            Func<IProducer<K, V>> customProducerProvider = null)
        {
            ProducerProvider = errorHandler => customProducerProvider?.Invoke() ?? Settings.CreateKafkaProducer(errorHandler);
            Settings = settings;
            CloseProducerOnStop = closeProducerOnStop;
            
            Shape = new FlowShape<TIn, Task<TOut>>(In, Out);
        }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            return new DefaultProducerStageLogic<K, V, P, TIn, TOut>(this, inheritedAttributes);
        }
    }

    internal class DefaultProducerStageLogic<K, V, P, TIn, TOut> : TimerGraphStageLogic, IProducerCompletionState
        where TIn: IEnvelope<K, V, P>
        where TOut: IResults<K, V, P>
    {
        private readonly IProducerStage<K, V, P, TIn, TOut> _stage;
        private readonly TaskCompletionSource<NotUsed> _completionState = new TaskCompletionSource<NotUsed>();
        private readonly Decider _decider;
        
        protected IProducer<K, V> Producer { get; private set; }
        protected readonly AtomicCounter AwaitingConfirmation = new AtomicCounter(0);
        
        public DefaultProducerStageLogic(IProducerStage<K, V, P, TIn, TOut> stage, Attributes attributes) : base(stage.Shape)
        {
            _stage = stage;

            var supervisionStrategy = attributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
            _decider = supervisionStrategy != null ? supervisionStrategy.Decider : Deciders.StoppingDecider;

            SetHandler(_stage.In, 
                onPush: () =>
                {
                    var msg = Grab(_stage.In) as IEnvelope<K, V, P>;

                    switch (msg)
                    {
                        case Message<K, V, P> message:
                        {
                            var result = new TaskCompletionSource<IResults<K, V, P>>();
                            AwaitingConfirmation.IncrementAndGet();
                            try
                            {
                                var callback = BuildSendCallback(
                                    completion: result, 
                                    onSuccess: report =>
                                    {
                                        result.SetResult(new Result<K, V, P>(report, message));
                                    }, 
                                    onFailure: OnProduceFailure);
                                Producer.Produce(message.Record, GetAsyncCallback(callback));
                                PostSend(msg);
                                Push(stage.Out, result.Task as Task<TOut>);
                            }
                            catch (Exception exception)
                            {
                                OnProduceFailure(exception);
                            }
                            break;
                        }

                        case MultiMessage<K, V, P> multiMessage:
                        {
                            var tasks = multiMessage.Records.Select(record =>
                            {
                                var result = new TaskCompletionSource<MultiResultPart<K, V>>();
                                AwaitingConfirmation.IncrementAndGet();
                                var callback = BuildSendCallback(
                                    completion: result,
                                    onSuccess: report =>
                                    {
                                        result.SetResult(new MultiResultPart<K, V>(report, record));
                                    },
                                    onFailure: OnProduceFailure);
                                try
                                {
                                    Producer.Produce(record, GetAsyncCallback(callback));
                                    return result.Task;
                                }
                                catch (Exception exception)
                                {
                                    OnProduceFailure(exception);
                                    return null;
                                }
                            }).Where(t => t != null).ToArray();
                            if (tasks.Length > 0)
                            {
                                PostSend(msg);
                                var resultTask = Task.WhenAll(tasks).ContinueWith(t => new MultiResult<K, V, P>(t.Result.ToImmutableHashSet(), multiMessage.PassThrough) as IResults<K, V, P>);
                                Push(stage.Out, resultTask as Task<TOut>);
                            }
                            else
                            {
                                TryPull(_stage.In);
                            }
                            break;
                        }

                        case PassThroughMessage<K, V, P> passThroughMessage:
                        {
                            PostSend(msg);
                            var resultTask = Task.FromResult(new PassThroughResult<K, V, P>(passThroughMessage.PassThrough) as IResults<K, V, P>);
                            Push(stage.Out, resultTask as Task<TOut>);
                            break;
                        }
                    }
                },
                onUpstreamFinish: () =>
                {
                    _completionState.SetResult(NotUsed.Instance);
                    CheckForCompletion();
                },
                onUpstreamFailure: exception =>
                {
                    _completionState.SetException(exception);
                    CheckForCompletion();
                });

            SetHandler(_stage.Out, onPull: () =>
            {
                TryPull(_stage.In);
            });
        }

        public override void PreStart()
        {
            base.PreStart();

            Producer = _stage.ProducerProvider(null);
            Log.Debug($"Producer started: {Producer.Name}");
        }

        public override void PostStop()
        {
            Log.Debug("Stage completed");

            if (_stage.CloseProducerOnStop)
            {
                try
                {
                    // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
                    Producer.Flush(_stage.FlushTimeout);
                    // TODO: fix missing deferred close support: `producer.close(stage.closeTimeout.toMillis, TimeUnit.MILLISECONDS)` 
                    Producer.Dispose();
                    Log.Debug($"Producer closed: {Producer.Name}");
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Problem occurred during producer close");
                }
            }

            base.PostStop();
        }
        
        public virtual void OnCompletionSuccess() => CompleteStage();

        public virtual void OnCompletionFailure(Exception ex) => FailStage(ex);
        
        protected virtual void PostSend(IEnvelope<K, V, P> msg) { }

        private void OnProduceFailure(Exception ex)
        {
            switch (_decider(ex))
            {
                case Directive.Stop:
                    CloseAndFailStage(ex);
                    Log.Error(ex, $"Producer.Produce threw an exception: {ex.Message}");
                    break;
                default:
                {
                    Log.Debug($"This exception has been handled by the Supervision Decider: {ex.Message}");
                    if (ex is ProduceException<K, V> pEx)
                    {
                        var error = pEx.Error;
                        if (error.IsFatal)
                        {
                            // if it is a fatal exception, producer needs to be restarted
                            Producer = _stage.ProducerProvider(null);
                            Log.Debug($"Producer restarted: {Producer.Name}");
                        }
                    }
                    TryPull(_stage.In);
                    ConfirmAndCheckForCompletion();
                    break;
                }
            }
        }
        
        private Action<DeliveryReport<K, V>> BuildSendCallback<TResult>(
            TaskCompletionSource<TResult> completion, 
            Action<DeliveryReport<K, V>> onSuccess,
            Action<Exception> onFailure)
        {
            return report =>
            {
                var error = report.Error;
                if (error.IsError)
                {
                    onFailure(new ProduceException<K, V>(error, report));
                }
                else
                {
                    onSuccess(report);
                    ConfirmAndCheckForCompletion();
                }
            };
        }

        private void ConfirmAndCheckForCompletion()
        {
            AwaitingConfirmation.Decrement();
            CheckForCompletion();
        }

        private void CheckForCompletion()
        {
            if (IsClosed(_stage.In) && AwaitingConfirmation.Current == 0)
            {
                Log.Debug("Completing publisher stage");
                var completionTask = _completionState.Task;

                if (completionTask.IsFaulted || completionTask.IsCanceled)
                {
                    OnCompletionFailure(completionTask.Exception);
                }
                else if (completionTask.IsCompleted)
                {
                    OnCompletionSuccess();
                }
                else
                {
                    FailStage(new IllegalStateException("Stage completed, but there is no info about status"));
                }
            }
        }

        private void CloseAndFailStage(Exception ex)
        {
            DisposeProducerImmediately();
            FailStage(ex);
        }

        // Called CloseProducerImmediately in JVM, there is no Producer.Close() in C#, we just dispose.
        private void DisposeProducerImmediately()
        {
            if (Producer != null && _stage.CloseProducerOnStop)
            {
                Producer.Dispose();
            }
        }

        // Called CloseProducer in JVM, there is no Producer.Close() in C#, we just dispose.
        private void DisposeProducer()
        {
            try
            {
                if (Producer != null && _stage.CloseProducerOnStop)
                {
                    Producer.Flush();
                    Producer.Dispose();
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Problem occured during producer close");
            }
        }
        
        /// <inheritdoc />
        protected override void OnTimer(object timerKey) { }
    }
}
