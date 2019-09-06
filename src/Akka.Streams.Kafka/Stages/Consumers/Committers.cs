using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Stages.Consumers.Actors;
using Akka.Streams.Kafka.Stages.Consumers.Exceptions;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers
{
    /// <summary>
    /// Interface for implementing committing consumed messages
    /// </summary>
    internal interface IInternalCommitter 
    {
        /// <summary>
        /// Commit all offsets (of different topics) belonging to the same stage
        /// </summary>
        Task Commit(ImmutableList<PartitionOffset> offsets);
    }
    
    internal class KafkaAsyncConsumerCommitter : IInternalCommitter
    {
        private readonly IActorRef _consumerActor;
        private readonly TimeSpan _commitTimeout;

        public KafkaAsyncConsumerCommitter(IActorRef consumerActor, TimeSpan commitTimeout)
        {
            _consumerActor = consumerActor;
            _commitTimeout = commitTimeout;
        }


        public Task Commit(ImmutableList<PartitionOffset> offsets)
        {
            var topicPartitionOffsets = offsets.Select(offset => new TopicPartitionOffset(offset.Topic, offset.Partition, offset.Offset)).ToImmutableHashSet();

            return _consumerActor.Ask(new KafkaConsumerActorMetadata.Internal.Commit(topicPartitionOffsets), _commitTimeout)
                .ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        switch (t.Exception.InnerException)
                        {
                            case AskTimeoutException timeoutException:
                                throw new CommitTimeoutException($"Kafka commit took longer than: {_commitTimeout}");
                            default:
                                throw t.Exception;
                        }
                    }
                });
        }
    }
}