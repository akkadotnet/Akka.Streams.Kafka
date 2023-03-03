using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Streams.Kafka.Internal;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Internal
{
    public class ConnectionCheckerSpec : Akka.TestKit.Xunit2.TestKit
    {
        private readonly TimeSpan _retryInterval;
        private readonly ConnectionCheckerSettings _config; 

        public ConnectionCheckerSpec()
        {
            _retryInterval = TimeSpan.FromMilliseconds(500);
            _config = new ConnectionCheckerSettings(true, 3, _retryInterval, 2d);
        }

        [Fact(DisplayName = "KafkaConnectionChecker must wait for response and retryInterval before perform new ask")]
        public void WaitForResponseBeforeAsking()
        {
            WithCheckerActorRef(checker =>
            {
                ExpectListTopicsRequest(0);
                Thread.Sleep(_retryInterval);
                checker.Tell(new Metadata.Topics(new Try<List<TopicMetadata>>(new List<TopicMetadata>())));
                ExpectListTopicsRequest(0);
            });
        }

        [Fact(DisplayName =
            "KafkaConnectionChecker must exponentially retry on failure and failed after max retries exceeded")]
        public void MustExponentiallyRetry()
        {
            WithCheckerActorRef(checker =>
            {
                foreach (var retry in Enumerable.Range(1, _config.MaxRetries + 1))
                {
                    ExpectListTopicsRequest(retry);
                    checker.Tell(new Metadata.Topics(new Try<List<TopicMetadata>>(new TimeoutException())));
                }

                Watch(checker);
                ExpectMsg<KafkaConnectionFailed>();
                ExpectTerminated(checker);
            });
        }

        [Fact(DisplayName =
            "KafkaConnectionChecker must return to normal mode if in backoff mode receive Metadata.Topics(success)")]
        public void MustReturnToNormal()
        {
            WithCheckerActorRef(checker =>
            {
                ExpectListTopicsRequest(0);
                checker.Tell(new Metadata.Topics(new Try<List<TopicMetadata>>(new TimeoutException())));
                
                ExpectListTopicsRequest(1);
                checker.Tell(new Metadata.Topics(new Try<List<TopicMetadata>>(new List<TopicMetadata>())));
                
                ExpectListTopicsRequest(0);
            });
        }

        private void ExpectListTopicsRequest(int retry)
        {
            var minimum = retry > 0 ? _retryInterval : _retryInterval * _config.Factor * retry;
            var stopwatch = Stopwatch.StartNew();
            ExpectMsg<Metadata.ListTopics>(TimeSpan.FromSeconds(5));
            stopwatch.Stop();
            stopwatch.Elapsed.Should().BeGreaterThan(minimum);
        }

        private void WithCheckerActorRef(Action<IActorRef> block)
        {
            var checker = ChildActorOf(ConnectionChecker.Props(_config));
            block(checker);
            Sys.Stop(Watch(checker));
            ExpectTerminated(checker);
        }
        
        private T WithCheckerActorRef<T>(Func<IActorRef, T> block)
        {
            var checker = ChildActorOf(ConnectionChecker.Props(_config));
            var res = block(checker);
            Sys.Stop(Watch(checker));
            ExpectTerminated(checker);
            return res;
        }
    }
}