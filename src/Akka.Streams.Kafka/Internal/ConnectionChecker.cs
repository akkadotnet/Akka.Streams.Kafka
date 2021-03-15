using System;
using Akka.Actor;
using Akka.Dispatch.SysMsg;
using Akka.Event;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers.Actors;

namespace Akka.Streams.Kafka.Internal
{
    internal class ConnectionChecker : ReceiveActor, IWithTimers
    {
        public static Props Props(ConnectionCheckerSettings config) =>
            Actor.Props.Create(() => new ConnectionChecker(config));

        private readonly ConnectionCheckerSettings _config;
        private readonly ILoggingAdapter _log;

        private readonly int _maxRetries;
        private readonly TimeSpan _checkInterval;
        private readonly double _factor;

        public ConnectionChecker(ConnectionCheckerSettings config)
        {
            _config = config;
            _log = Context.GetLogger();

            _maxRetries = config.MaxRetries;
            _checkInterval = config.CheckInterval;
            _factor = config.Factor;
        }

        public ITimerScheduler Timers { get; set; }

        private Receive Regular()
            => Behaviour(0, _checkInterval);

        private Receive Backoff(int failedAttempts, TimeSpan backoffCheckInterval)
            => Behaviour(failedAttempts, backoffCheckInterval);

        private Receive Behaviour(int failedAttempts, TimeSpan interval)
        {
            return msg =>
            {
                switch (msg)
                {
                    case KafkaConsumerActorMetadata.Internal.Stop _:
                        Timers.CancelAll();
                        Context.Stop(Self);
                        return true;

                    case Internal.CheckConnection _:
                        Context.Parent.Tell(Metadata.ListTopics.Instance);
                        return true;

                    case Metadata.Topics topics when topics.Response.Failure.Value is TimeoutException:
                        // failedAttempts is a sum of first triggered failure and retries (retries + 1)
                        if (failedAttempts == _maxRetries)
                        {
                            Context.Parent.Tell(new KafkaConnectionFailed(topics.Response.Failure.Value, _maxRetries));
                            Context.Stop(Self);
                        }
                        else
                        {
                            Context.Become(Backoff(failedAttempts + 1, StartBackoffTimer(interval)));
                        }

                        return true;

                    // This is a debug, to check to see if C# Confluent.Kafka behaves differently compared to JVM
                    case Metadata.Topics topics when topics.Response.Failure.HasValue:
                        _log.Warning($"Caught an exception while testing broker connection: {topics.Response.Failure.Value}");
                        return false;

                    case Metadata.Topics topics when topics.Response.IsSuccess:
                        StartTimer();
                        Become(Regular());
                        return true;
                }
                return false;
            };
        }

        protected override void PreStart()
        {
            base.PreStart();
            StartTimer();
        }

        private void StartTimer()
        {
            Timers.StartSingleTimer(Internal.RegularCheck.Instance, Internal.CheckConnection.Instance, _checkInterval);
        }

        private TimeSpan StartBackoffTimer(TimeSpan previousInterval)
        {
            var backoffCheckInterval = new TimeSpan((long)(previousInterval.Ticks * _factor));
            Timers.StartSingleTimer(Internal.BackoffCheck.Instance, Internal.CheckConnection.Instance, backoffCheckInterval);
            return backoffCheckInterval;
        }

        internal static class Internal
        {
            public class RegularCheck
            {
                public static readonly RegularCheck Instance = new RegularCheck();
                private RegularCheck() { }
            }

            public class BackoffCheck
            {
                public static readonly BackoffCheck Instance = new BackoffCheck();
                private BackoffCheck() { }
            }

            public class CheckConnection
            {
                public static readonly CheckConnection Instance = new CheckConnection();
                private CheckConnection() { }
            }
        }
    }

}
