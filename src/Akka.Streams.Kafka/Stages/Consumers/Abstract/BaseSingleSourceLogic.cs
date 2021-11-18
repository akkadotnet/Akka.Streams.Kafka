using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Streams.Kafka.Supervision;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Akka.Streams.Util;
using Confluent.Kafka;
using Decider = Akka.Streams.Supervision.Decider;
using Directive = Akka.Streams.Supervision.Directive;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// Shared GraphStageLogic for <see cref="SingleSourceStageLogic{K,V,TMessage}"/> and <see cref="ExternalSingleSourceLogic{K,V,TMessage}"/>
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    /// <typeparam name="TMessage">Message type</typeparam>
    internal abstract class BaseSingleSourceLogic<K, V, TMessage> : GraphStageLogic
    {
        private readonly SourceShape<TMessage> _shape;
        private readonly IMessageBuilder<K, V, TMessage> _messageBuilder;
        private int _requestId = 0;
        private bool _requested = false;
        private readonly Decider _decider;

        private readonly ConcurrentQueue<ConsumeResult<K, V>> _buffer = new ConcurrentQueue<ConsumeResult<K, V>>();
        protected IImmutableSet<TopicPartition> TopicPartitions { get; set; } = ImmutableHashSet.Create<TopicPartition>();

        protected StageActor SourceActor { get; private set; }
        internal IActorRef ConsumerActor { get; private set; }

        /// <summary>
        /// Implements <see cref="IControl"/> to provide control over executed source
        /// </summary>
        public virtual PromiseControl<TMessage> Control { get; }
        
        protected BaseSingleSourceLogic(
            SourceShape<TMessage> shape,
            Attributes attributes,
            Func<BaseSingleSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory,
            bool autoCreateTopics) 
            : base(shape)
        {
            _shape = shape;
            _messageBuilder = messageBuilderFactory(this);
            Control = new BaseSingleSourceControl(_shape, Complete, SetKeepGoing, GetAsyncCallback, PerformShutdown);
            
            // TODO: Move this to the GraphStage.InitialAttribute when it is fixed (https://github.com/akkadotnet/akka.net/issues/5388)
            var supervisionStrategy = attributes.GetAttribute(new ActorAttributes.SupervisionStrategy(new DefaultConsumerDecider(autoCreateTopics).Decide));
            _decider = supervisionStrategy.Decider;
            
            SetHandler(shape.Outlet, onPull: Pump, onDownstreamFinish: PerformShutdown);
        }

        public override void PreStart()
        {
            base.PreStart();
            
            SourceActor = GetStageActor(MessageHandling);
            ConsumerActor = CreateConsumerActor();
            SourceActor.Watch(ConsumerActor);
            
            ConfigureSubscription();
        }

        public override void PostStop()
        {
            Control.OnShutdown();
            
            base.PostStop();
        }

        /// <summary>
        /// Creates consumer actor
        /// </summary>
        protected abstract IActorRef CreateConsumerActor();

        /// <summary>
        /// This should configure consumer subscription on stage start
        /// </summary>
        protected abstract void ConfigureSubscription();

        /// <summary>
        /// Configures manual subscription
        /// </summary>
        /// <param name="subscription"></param>
        protected void ConfigureManualSubscription(IManualSubscription subscription)
        {
            switch (subscription)
            {
                case Assignment assignment:
                    ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Assign(assignment.TopicPartitions), SourceActor.Ref);
                    TopicPartitions = TopicPartitions.Union(assignment.TopicPartitions);
                    break;
                case AssignmentWithOffset assignmentWithOffset:
                    ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.AssignWithOffset(assignmentWithOffset.TopicPartitions), SourceActor.Ref);
                    TopicPartitions = TopicPartitions.Union(assignmentWithOffset.TopicPartitions.Select(tp => tp.TopicPartition));
                    break;
            }
        }

        protected virtual void MessageHandling((IActorRef, object) args)
        {
            var (sender, message) = args;
            switch (message)
            {
                case KafkaConsumerActorMetadata.Internal.Messages<K, V> msg:
                    if(Log.IsDebugEnabled)
                        Log.Debug("Received {0} messages from {1}", msg.MessagesList.Count, sender);
                    
                    // might be more than one in flight when we assign/revoke tps
                    if (msg.RequestId == _requestId)
                        _requested = false;

                    foreach (var consumerMessage in msg.MessagesList)
                        _buffer.Enqueue(consumerMessage);
                    
                    Pump();
                    break;
                
                case Status.Failure failure:
                    var exception = failure.Cause;
                    var cause = exception.GetCause(); 
                    var directive = _decider(exception); 
                    if (directive == Directive.Stop)
                    {
                        if(Log.IsErrorEnabled)
                            Log.Error(exception, "Source stage failed with exception: [{0}]. Decider directive: {1}", cause, directive);
                        FailStage(failure.Cause);
                        break;
                    }

                    if(Log.IsInfoEnabled)
                        Log.Info(exception, "Source stage failure [{0}] handled with Supervision Directive [{1}]", cause, directive);
                    
                    var isSerializationError = exception is ConsumeException cEx && cEx.Error.IsSerializationError();
                    if (isSerializationError)
                    {
                        if (directive == Directive.Resume)
                            break;
                        
                        // ConsumerActor is still alive, we need to kill it.
                        ConsumerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, SourceActor.Ref);
                    }
                    
                    // Empty the buffer to make sure that messages does not get duplicated
                    while (_buffer.TryDequeue(out _))
                    { }
                    
                    // ConsumerActor are designed to suicide itself on any error except for de/serialization error
                    // to prevent any offset/commit runaway. We will need to restart it.
                    SourceActor.Unwatch(ConsumerActor);
                    ConsumerActor = CreateConsumerActor();
                    SourceActor.Watch(ConsumerActor);
                    ConfigureSubscription();
                    break;
                
                case Terminated terminated:
                    if(Log.IsInfoEnabled)
                        Log.Info("Consumer actor terminated: {0}", terminated.ActorRef.Path);
                    break;
            }
        }

        private void Pump()
        {
            while(IsAvailable(_shape.Outlet) && _buffer.TryDequeue(out var message))
            {
                Push(_shape.Outlet, _messageBuilder.CreateMessage(message));
            }
            
            if (IsAvailable(_shape.Outlet) && !_requested && TopicPartitions.Any())
            {
                RequestMessages();
            }
        }

        protected void RequestMessages()
        {
            _requested = true;
            _requestId += 1;
            if (Log.IsDebugEnabled)
                Log.Debug("Requesting messages, requestId: {0}, partitions: {1}", _requestId, string.Join(", ", TopicPartitions));
            ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.RequestMessages(_requestId, TopicPartitions.ToImmutableHashSet()), SourceActor.Ref);
        }

        protected abstract void PerformShutdown();

        protected class BaseSingleSourceControl : PromiseControl<TMessage>
        {
            private readonly Action _performShutdown;

            public BaseSingleSourceControl(SourceShape<TMessage> shape, Action<Outlet<TMessage>> completeStageOutlet, Action<bool> setStageKeepGoing, 
                                           Func<Action, Action> asyncCallbackFactory, Action performShutdown) 
                : base(shape, completeStageOutlet, setStageKeepGoing, asyncCallbackFactory)
            {
                _performShutdown = performShutdown;
            }

            public override void PerformShutdown() => _performShutdown();
        }
    }
}