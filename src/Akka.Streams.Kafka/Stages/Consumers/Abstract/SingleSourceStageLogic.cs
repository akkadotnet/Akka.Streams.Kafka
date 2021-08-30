using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Abstract
{
    /// <summary>
    /// Base class for any single-source stage logic implementations
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    /// <typeparam name="TMessage"></typeparam>
    internal class SingleSourceStageLogic<K, V, TMessage> : BaseSingleSourceLogic<K, V, TMessage>
    {
        private readonly SourceShape<TMessage> _shape;
        private readonly ConsumerSettings<K, V> _settings;
        private readonly ISubscription _subscription;

        private readonly int _actorNumber = KafkaConsumerActorMetadata.NextNumber();

        public SingleSourceStageLogic(SourceShape<TMessage> shape, ConsumerSettings<K, V> settings, 
                                      ISubscription subscription, Attributes attributes, 
                                      Func<BaseSingleSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory) 
            : base(shape, attributes, messageBuilderFactory)
        {
            _shape = shape;
            _settings = settings;
            _subscription = subscription;
        }

        /// <inheritdoc />
        protected override void ConfigureSubscription()
        {
            switch (_subscription)
            {
                case TopicSubscription topicSubscription:
                    ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.Subscribe(topicSubscription.Topics), SourceActor.Ref);
                    break;
                case TopicSubscriptionPattern topicSubscriptionPattern:
                    ConsumerActor.Tell(new KafkaConsumerActorMetadata.Internal.SubscribePattern(topicSubscriptionPattern.TopicPattern), SourceActor.Ref);
                    break;
                case IManualSubscription manualSubscription:
                    ConfigureManualSubscription(manualSubscription);
                    break;
                default:
                    throw new NotSupportedException();
            }
        }

        /// <inheritdoc />
        protected override IActorRef CreateConsumerActor()
        {
            var partitionsAssignedHandler = GetAsyncCallback<IEnumerable<TopicPartition>>(PartitionsAssigned);
            var partitionsRevokedHandler = GetAsyncCallback<IEnumerable<TopicPartitionOffset>>(PartitionsRevoked);

            IPartitionEventHandler internalHandler = new PartitionEventHandlers.AsyncCallbacks(partitionsAssignedHandler, partitionsRevokedHandler);

            // If custom partition events handler specified - add it to the chain
            var eventHandler = _subscription is IAutoSubscription autoSubscription && autoSubscription.PartitionEventsHandler.HasValue
                ? new PartitionEventHandlers.Chain(autoSubscription.PartitionEventsHandler.Value, internalHandler)
                : internalHandler;

            IStatisticsHandler statisticsHandler = _subscription.StatisticsHandler.HasValue
                ? _subscription.StatisticsHandler.Value
                : new StatisticsHandlers.Empty();

            // This allows to override partition events handling by subclasses
            eventHandler = AddToPartitionAssignmentHandler(eventHandler);
            
            if (!(Materializer is ActorMaterializer actorMaterializer))
                throw new ArgumentException($"Expected {typeof(ActorMaterializer)} but got {Materializer.GetType()}");
            
            var extendedActorSystem = actorMaterializer.System.AsInstanceOf<ExtendedActorSystem>();
            var actor = extendedActorSystem.SystemActorOf(KafkaConsumerActorMetadata.GetProps(SourceActor.Ref, _settings, eventHandler, statisticsHandler),
                                                          $"kafka-consumer-{_actorNumber}");
            return actor;
        }

        public override void PostStop()
        {
            ConsumerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, SourceActor.Ref);

            base.PostStop();
        }

        protected override void PerformShutdown()
        {
            SetKeepGoing(true);
            
            if (!IsClosed(_shape.Outlet))
                Complete(_shape.Outlet);
            
            SourceActor.Become(ShuttingDownReceive);
            StopConsumerActor();
        }

        /// <summary>
        /// Opportunity for subclasses to add their logic to the partition assignment callbacks.
        /// </summary>
        protected virtual IPartitionEventHandler AddToPartitionAssignmentHandler(IPartitionEventHandler handler)
        {
            return handler;
        }

        protected virtual void ShuttingDownReceive((IActorRef, object) args)
        {
            switch (args.Item2)
            {
                case Terminated terminated when terminated.ActorRef.Equals(ConsumerActor):
                    Control.OnShutdown();
                    CompleteStage();
                    break;
                default:
                    // Ignoring any consumed messages, because downstream is already closed
                    return;
            }
        }

        protected virtual void StopConsumerActor()
        {
            Materializer.ScheduleOnce(_settings.StopTimeout, () =>
            {
                ConsumerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, SourceActor.Ref);
            });
        }

        private void PartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            TopicPartitions = TopicPartitions.Union(partitions);
            Log.Debug($"Partitions were assigned: {string.Join(", ", TopicPartitions)}");
            RequestMessages();
        }
        
        private void PartitionsRevoked(IEnumerable<TopicPartitionOffset> partitions)
        {
            TopicPartitions = TopicPartitions.Except(partitions.Select(tpo => tpo.TopicPartition));
            Log.Debug("Partitions were revoked");
        }
    }
}