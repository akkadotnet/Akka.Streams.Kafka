using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Akka.Streams.Stage;
using Akka.Util;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// Shared GraphStageLogic for <see cref="SingleSourceStageLogic{K,V,TMessage}"/> and <see cref="ExternalSingleSourceLogic"/>
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    /// <typeparam name="TMessage">Message type</typeparam>
    internal abstract class BaseSingleSourceLogic<K, V, TMessage> : GraphStageLogic
    {
        private readonly SourceShape<TMessage> _shape;
        private readonly TaskCompletionSource<NotUsed> _completion;
        private readonly IMessageBuilder<K, V, TMessage> _messageBuilder;
        private int _requestId = 0;
        private bool _requested = false;
        private readonly ConcurrentQueue<ConsumeResult<K, V>> _buffer = new ConcurrentQueue<ConsumeResult<K, V>>();
        protected IImmutableSet<TopicPartition> TopicPartitions { get; set; } = ImmutableHashSet.Create<TopicPartition>();
        
        protected StageActor SourceActor { get; private set; }
        internal IActorRef ConsumerActor { get; private set; }
        internal MessageDispatcher ExecutionContext => Materializer.ExecutionContext;
        
        protected BaseSingleSourceLogic(SourceShape<TMessage> shape, TaskCompletionSource<NotUsed> completion,
                                        Func<BaseSingleSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory) 
            : base(shape)
        {
            _shape = shape;
            _completion = completion;
            _messageBuilder = messageBuilderFactory(this);
            
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
            OnShutdown();
            
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
        /// This is called when stage downstream is finished
        /// </summary>
        protected abstract void PerformShutdown();

        /// <summary>
        /// Makes this logic task finished
        /// </summary>
        protected void OnShutdown()
        {
            _completion.SetResult(NotUsed.Instance);
        }

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

        private void MessageHandling(Tuple<IActorRef, object> args)
        {
            switch (args.Item2)
            {
                case KafkaConsumerActorMetadata.Internal.Messages<K, V> msg:
                    // might be more than one in flight when we assign/revoke tps
                    if (msg.RequestId == _requestId)
                        _requested = false;

                    foreach (var consumerMessage in msg.MessagesList)
                        _buffer.Enqueue(consumerMessage);
                    
                    Pump();
                    
                    break;
                
                case Status.Failure failure:
                    FailStage(failure.Cause);
                    break;
                
                case Terminated terminated:
                    FailStage(new ConsumerFailed());
                    break;
            }
        }

        private void Pump()
        {
            if (IsAvailable(_shape.Outlet))
            {
                if (_buffer.TryDequeue(out var message))
                {
                    Push(_shape.Outlet, _messageBuilder.CreateMessage(message));
                    Pump();
                }
                else if (!_requested && TopicPartitions.Any())
                {
                    RequestMessages();
                }
            }
        }

        protected void RequestMessages()
        {
            _requested = true;
            _requestId += 1;
            Log.Debug($"Requesting messages, requestId: {_requestId}, partitions: {string.Join(", ", TopicPartitions)}");
            ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.RequestMessages(_requestId, TopicPartitions.ToImmutableHashSet()), SourceActor.Ref);
        }
    }
}