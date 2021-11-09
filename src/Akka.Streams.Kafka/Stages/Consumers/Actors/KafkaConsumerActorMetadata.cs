using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    /// <summary>
    /// Containes metadata for <see cref="KafkaConsumerActor{K,V}"/>.
    /// Generally this should not be used from outside of the library.
    /// </summary>
    [InternalApi]
    public static class KafkaConsumerActorMetadata
    {
        private static volatile int _number = 1;
        /// <summary>
        /// Gets next actor number in thread-safe way
        /// </summary>
        /// <returns></returns>
        public static int NextNumber() => Interlocked.Increment(ref _number);
        
        /// <summary>
        /// Gets actor props
        /// </summary>
        public static Props GetProps<K, V>(ConsumerSettings<K, V> settings) =>
            Props.Create(() => new KafkaConsumerActor<K, V>(ActorRefs.Nobody, settings, new PartitionEventHandlers.Empty(), new StatisticsHandlers.Empty())).WithDispatcher(settings.DispatcherId);
        
        /// <summary>
        /// Gets actor props
        /// </summary>
        internal static Props GetProps<K, V>(ConsumerSettings<K, V> settings, IPartitionEventHandler handler, IStatisticsHandler statisticsHandler) =>
            Props.Create(() => new KafkaConsumerActor<K, V>(ActorRefs.Nobody, settings, handler, statisticsHandler)).WithDispatcher(settings.DispatcherId);
        
        /// <summary>
        /// Gets actor props
        /// </summary>
        internal static Props GetProps<K, V>(IActorRef owner, ConsumerSettings<K, V> settings, IPartitionEventHandler handler, IStatisticsHandler statisticsHandler) =>
            Props.Create(() => new KafkaConsumerActor<K, V>(owner, settings, handler, statisticsHandler)).WithDispatcher(settings.DispatcherId);


        /// <summary>
        /// Contains <see cref="KafkaConsumerActor{K,V}"/>  message definitions.
        /// Generally this should not be used from outside of the library.
        /// </summary>
        [InternalApi]
        public class Internal
        {
            /// <summary>
            /// Messages
            /// </summary>
            public class Messages<K, V>
            {
                /// <summary>
                /// Messages
                /// </summary>
                /// <param name="requestId">Request Id</param>
                /// <param name="messagesList">List of consumed messages</param>
               public Messages(int requestId, ImmutableList<ConsumeResult<K, V>> messagesList)
                {
                    RequestId = requestId;
                    MessagesList = messagesList;
                }

                /// <summary>
                /// Request Id
                /// </summary>
                public int RequestId { get; }
                /// <summary>
                /// List of consumed messages
                /// </summary>
                public ImmutableList<ConsumeResult<K, V>> MessagesList { get; }
            }

            /// <summary>
            /// Used to send commit requests to <see cref="KafkaConsumerActor{K,V}"/>
            /// </summary>
            public class Commit
            {
                /// <summary>
                /// Commit
                /// </summary>
                /// <param name="offsets">List of offsets to commit</param>
                public Commit(IImmutableSet<TopicPartitionOffset> offsets)
                {
                    Offsets = offsets;
                }

                /// <summary>
                /// List of offsets to commit
                /// </summary>
                public IImmutableSet<TopicPartitionOffset> Offsets { get; }
            }

            /// <summary>
            /// Committed
            /// </summary>
            public class Committed
            {
                /// <summary>
                /// Commited message
                /// </summary>
                /// <param name="offsets">Collection of committed offsets</param>
                public Committed(IImmutableSet<TopicPartitionOffset> offsets)
                {
                    Offsets = offsets;
                }

                /// <summary>
                /// Committed offsets
                /// </summary>
                public IImmutableSet<TopicPartitionOffset> Offsets { get; }
            }

            /// <summary>
            /// Used to request for kafka messages
            /// </summary>
            public class RequestMessages
            {
                /// <summary>
                /// RequestMessages
                /// </summary>
                /// <param name="requestId">Request Id</param>
                /// <param name="topics">List of topics to consume</param>
                public RequestMessages(int requestId, ImmutableHashSet<TopicPartition> topics)
                {
                    RequestId = requestId;
                    Topics = topics;
                }

                /// <summary>
                /// Request Id
                /// </summary>
                public int RequestId { get; }
                /// <summary>
                /// List of topics to consume
                /// </summary>
                public ImmutableHashSet<TopicPartition> Topics { get; }
            }

            /// <summary>
            /// Revoked
            /// </summary>
            public class Revoked
            {
                /// <summary>
                /// Revoked
                /// </summary>
                /// <param name="partitions">List of revoked partitions</param>
                public Revoked(IImmutableSet<TopicPartition> partitions)
                {
                    Partitions = partitions;
                }

                /// <summary>
                /// List of revoked partitions
                /// </summary>
                public IImmutableSet<TopicPartition> Partitions { get; }
            }

            /// <summary>
            /// Assign
            /// </summary>
            public class Assign
            {
                /// <summary>
                /// Assign
                /// </summary>
                /// <param name="topicPartitions">Topic partitions</param>
                public Assign(IImmutableSet<TopicPartition> topicPartitions)
                {
                    TopicPartitions = topicPartitions;
                }

                /// <summary>
                /// Topic partitions
                /// </summary>
                public IImmutableSet<TopicPartition> TopicPartitions { get; }
            }
            
            /// <summary>
            /// AssignWithOffset
            /// </summary>
            public class AssignWithOffset
            {
                /// <summary>
                /// AssignWithOffset
                /// </summary>
                /// <param name="topicPartitionOffsets">Topic partitions with offsets</param>
                public AssignWithOffset(IImmutableSet<TopicPartitionOffset> topicPartitionOffsets)
                {
                    TopicPartitionOffsets = topicPartitionOffsets;
                }

                /// <summary>
                /// Topic partitions
                /// </summary>
                public IImmutableSet<TopicPartitionOffset> TopicPartitionOffsets { get; }
            }
            
            /// <summary>
            /// Marker interface for subscription requests
            /// </summary>
            public interface ISubscriptionRequest
            {
            }

            /// <summary>
            /// Subscribe
            /// </summary>
            public class Subscribe : ISubscriptionRequest
            {
                /// <summary>
                /// Subscribe
                /// </summary>
                public Subscribe(IImmutableSet<string> topics)
                {
                    Topics = topics;
                }

                /// <summary>
                /// List of topics to subscribe
                /// </summary>
                public IImmutableSet<string> Topics { get; }
            }
            
            /// <summary>
            /// SubscribePattern
            /// </summary>
            public class SubscribePattern : ISubscriptionRequest
            {
                /// <summary>
                /// SubscribePattern
                /// </summary>
                /// <param name="topicPattern">Topic pattern (regular expression to be matched)</param>
                public SubscribePattern(string topicPattern)
                {
                    TopicPattern = topicPattern;
                }

                /// <summary>
                /// Topic pattern (regular expression to be matched)
                /// </summary>
                public string TopicPattern { get; }
            }

            /// <summary>
            /// Stops consuming actor
            /// </summary>
            public class Stop
            {
                public static readonly Stop Instance = new Stop();

                private Stop() { }
            }

            /// <summary>
            /// Seek
            /// </summary>
            public class Seek
            {
                /// <summary>
                /// Seek
                /// </summary>
                /// <param name="offsets">Offsets to seek</param>
                public Seek(IImmutableSet<TopicPartitionOffset> offsets)
                {
                    Offsets = offsets;
                }

                /// <summary>
                /// Offsets to seek
                /// </summary>
                public IImmutableSet<TopicPartitionOffset> Offsets { get; }
            }
        }
    }
}