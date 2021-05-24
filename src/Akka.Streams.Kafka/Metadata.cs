using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using Akka.Actor;
using Akka.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka
{
    /// <summary>
    /// Messages for Kafka metadata fetching via [[KafkaConsumerActor]].
    ///
    /// NOTE: Processing of these requests blocks the actor loop. The KafkaConsumerActor is configured to run on its
    /// own dispatcher, so just as the other remote calls to Kafka, the blocking happens within a designated thread pool.
    /// However, calling these during consuming might affect performance and even cause timeouts in extreme cases.
    /// </summary>
    public class Metadata
    {
        public interface IRequest { }
        public interface IResponse { }

        /// <summary>
        /// <see cref="Confluent.Kafka.Metadata.Topics"/>
        /// </summary>
        public sealed class ListTopics : IRequest, INoSerializationVerificationNeeded
        {
            public static readonly ListTopics Instance = new ListTopics();
            private ListTopics() { }
        }

        public sealed class Topics : IResponse, INoSerializationVerificationNeeded
        {
            public readonly Try<List<TopicMetadata>> Response;

            public Topics(Try<List<TopicMetadata>> response)
            {
                Response = response;
            }
        }

    }
}
