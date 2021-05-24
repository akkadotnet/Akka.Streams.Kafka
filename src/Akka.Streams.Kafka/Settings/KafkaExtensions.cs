﻿using Akka.Configuration;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Settings
{
    public static class KafkaExtensions
    {
        public static Config DefaultSettings =>
            ConfigurationFactory.FromResource<CommitterSettings>("Akka.Streams.Kafka.reference.conf");

        public static bool IsBrokerErrorRetriable(Error error)
        {
            switch (error.Code)
            {
                case ErrorCode.InvalidMsg:
                case ErrorCode.UnknownTopicOrPart:
                case ErrorCode.LeaderNotAvailable:
                case ErrorCode.NotLeaderForPartition:
                case ErrorCode.RequestTimedOut:
                case ErrorCode.GroupLoadInProress:
                case ErrorCode.GroupCoordinatorNotAvailable:
                case ErrorCode.NotCoordinatorForGroup:
                case ErrorCode.NotEnoughReplicas:
                case ErrorCode.NotEnoughReplicasAfterAppend:
                    return true;
            }

            return false;
        }

        public static bool IsLocalValueSerializationError(Error error)
        {
            return error.Code == ErrorCode.Local_ValueSerialization || error.Code == ErrorCode.Local_ValueDeserialization;
        }

        public static bool IsLocalErrorRetriable(Error error)
        {
            switch (error.Code)
            {
                case ErrorCode.Local_Transport:
                case ErrorCode.Local_AllBrokersDown:
                    return false;
            }

            return true;
        }
    }
}
