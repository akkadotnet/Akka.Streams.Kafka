using System;
using System.Threading;
using Akka.Event;
using Akka.Util;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Testkit.Internal
{
    public static class Checks
    {
        [Obsolete("Kafka DescribeCluster API is not supported in the .NET driver")]
        public static void WaitUntilCluster(
            TimeSpan timeout,
            TimeSpan sleepInBetween,
            IAdminClient adminClient,
            Func<object, bool> predicate,
            ILoggingAdapter log)
        {
            throw new NotImplementedException("Kafka DescribeCluster API is not supported in the .NET driver");
        }
        
        public static void WaitUntilConsumerGroup(
            string groupId,
            TimeSpan timeout,
            TimeSpan sleepInBetween,
            IAdminClient adminClient,
            Func<GroupInfo, bool> predicate,
            ILoggingAdapter log)
        {
            PeriodicalCheck(
                description: "consumer group state",
                timeout: timeout, 
                sleepInBetween: sleepInBetween,
                data: () => adminClient.ListGroup(groupId, timeout),
                predicate: predicate,
                log: log);
        }
        
        public static void PeriodicalCheck<T>(
            string description,
            TimeSpan timeout,
            TimeSpan sleepInBetween,
            Func<T> data,
            Func<T, bool> predicate,
            ILoggingAdapter log)
        {
            var maxTries = (int) (timeout.TotalMilliseconds / sleepInBetween.TotalMilliseconds);
            var triesLeft = maxTries;

            while (true)
            {
                var result = Try<bool>
                    .From(() => predicate(data()))
                    .RecoverWith(ex =>
                    {
                        log.Debug($"Ignoring [{ex.GetType()}: {ex.Message}] while waiting for desired state");
                        return false;
                    });
                
                if (result.Success == false)
                {
                    if (triesLeft > 0)
                    {
                        Thread.Sleep(sleepInBetween);
                        triesLeft--;
                        continue;
                    }
                    throw new TimeoutException(
                        $"Timeout while waiting for desired {description}. Tried [{maxTries}] times, slept [{sleepInBetween}] in between.");
                }

                if (!result.IsSuccess)
                    throw result.Failure.Value;
                
                if(result.Success == true)
                    break;
            }
        }
    }
}