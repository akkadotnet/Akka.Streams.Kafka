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

        public SingleSourceStageLogic(SourceShape<TMessage> shape, ConsumerSettings<K, V> settings, 
                                      ISubscription subscription, Attributes attributes, 
                                      Func<BaseSingleSourceLogic<K, V, TMessage>, IMessageBuilder<K, V, TMessage>> messageBuilderFactory) 
            : base(shape, attributes, messageBuilderFactory, settings.AutoCreateTopicsEnabled, subscription)
        {
            _shape = shape;
            _settings = settings;
        }

        protected override object LogSource
        {
            get
            {
                var strPart = (_settings.Properties.TryGetValue("client.id", out var clientId)) ?
                    $"client-{_settings.GroupId}-{clientId}" : $"client-{_settings.GroupId}";
                return Akka.Event.LogSource.Create(strPart, GetType());
            }
        }

        /// <inheritdoc />
        protected override IActorRef CreateConsumerActor()
        {
            IStatisticsHandler statisticsHandler = Subscription.StatisticsHandler.HasValue
                ? Subscription.StatisticsHandler.Value
                :  StatisticsHandlers.Empty.Instance;
            
            if (Materializer is not ActorMaterializer actorMaterializer)
                throw new ArgumentException($"Expected {typeof(ActorMaterializer)} but got {Materializer.GetType()}");
            
            var extendedActorSystem = actorMaterializer.System.AsInstanceOf<ExtendedActorSystem>();
            var actor = extendedActorSystem.SystemActorOf(KafkaConsumerActorMetadata.GetProps(SourceActor.Ref, _settings, Decider, statisticsHandler),
                                                          $"kafka-consumer-{KafkaConsumerActorMetadata.NextNumber()}");
            
            return actor;
        }

        protected override void ConfigureSubscription()
        {
            var partitionsAssignedHandler = GetAsyncCallback<IImmutableSet<TopicPartition>>(PartitionsAssigned);
            var partitionsRevokedHandler = GetAsyncCallback<IImmutableSet<TopicPartitionOffset>>(PartitionsRevoked);

            ConfigureSubscription(partitionsAssignedHandler, partitionsRevokedHandler);
        }
        
        

        public override void PostStop()
        {
            ConsumerActor.Tell(KafkaConsumerActorMetadata.Internal.Stop.Instance, SourceActor.Ref);

            base.PostStop();
        }

        protected override void PerformShutdown(Exception? ex)
        {
            if (ex is not null and not SubscriptionWithCancelException.NonFailureCancellation)
                Log.Info(ex, $"{nameof(SingleSourceStageLogic<K, V, TMessage>)} was shutdown due to exception");
            
            SetKeepGoing(true);
            
            if (!IsClosed(_shape.Outlet))
                Complete(_shape.Outlet);
            
            SourceActor.Become(ShuttingDownReceive);
            StopConsumerActor();
        }

        protected virtual void ShuttingDownReceive((IActorRef, object) args)
        {
            switch (args.Item2)
            {
                case Terminated _:
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

        private void PartitionsAssigned(IImmutableSet<TopicPartition> partitions)
        {
            TopicPartitions = TopicPartitions.Union(partitions);
            Log.Debug("[{0}] Partitions were assigned: {1}. All partitions: {2}", ConsumerActor.Path.Name, string.Join(", ", partitions), string.Join(", ", TopicPartitions));
            RequestMessages();
        }
        
        private void PartitionsRevoked(IImmutableSet<TopicPartitionOffset> partitions)
        {
            TopicPartitions = TopicPartitions.Except(partitions.Select(tpo => tpo.TopicPartition));
            Log.Debug("[{0}] Partitions were revoked: {1}. All partitions: {2}", ConsumerActor.Path.Name, string.Join(", ", partitions), string.Join(", ", TopicPartitions));
        }
    }
}