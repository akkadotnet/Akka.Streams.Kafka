using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor.Setup;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Testkit.Internal;
using Akka.Streams.TestKit;
using Akka.Util;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Testkit.Dsl
{
    public abstract class KafkaSpec : KafkaTestKit, IAsyncLifetime
    {
        protected KafkaSpec(string config, string actorSystemName = null, ITestOutputHelper output = null) : base(config, actorSystemName, output)
        {
        }

        protected KafkaSpec(Config config, string actorSystemName = null, ITestOutputHelper output = null) : base(config, actorSystemName, output)
        {
        }

        protected KafkaSpec(ActorSystemSetup config, string actorSystemName = null, ITestOutputHelper output = null) : base(config, actorSystemName, output)
        {
        }
        
        protected IProducer<string, string> TestProducer { get; private set; }


        public virtual Task InitializeAsync()
        {
            TestProducer = ProducerDefaults().CreateKafkaProducer();
            SetUpAdminClient();
            return Task.CompletedTask;
        }

        public virtual Task DisposeAsync()
        {
            TestProducer?.Dispose();
            CleanUpAdminClient();
            Shutdown();
            return Task.CompletedTask;
        }

        protected void Sleep(TimeSpan time, string msg)
        {
            Log.Debug($"Sleeping {time}: {msg}");
            Thread.Sleep(time);
        }

        protected List<T> AwaitMultiple<T>(TimeSpan timeout, IEnumerable<Task<T>> tasks)
        {
            var completedTasks = new List<Task<T>>();
            using (var cts = new CancellationTokenSource(timeout))
            {
                var waitingTasks = tasks.ToList();
                while (waitingTasks.Count > 0)
                {
                    var anyTask = Task.WhenAny(waitingTasks);
                    try
                    {
                        anyTask.Wait(cts.Token);
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"AwaitMultiple failed. Exception: {e.Message}", e);
                    }

                    var completedTask = anyTask.Result;
                    waitingTasks.Remove(completedTask);
                    completedTasks.Add(completedTask);
                }
            }

            return completedTasks.Select(t => t.Result).ToList();
        }
        
        protected TimeSpan SleepAfterProduce => TimeSpan.FromSeconds(4);

        protected void AwaitProduce(IEnumerable<Task<Done>> tasks)
        {
            AwaitMultiple(TimeSpan.FromSeconds(4), tasks);
            Sleep(SleepAfterProduce, "to be sure producing has happened");
        }

        protected readonly Partition Partition0 = new Partition(0);

        // Not implemented
        [Obsolete("Kafka DescribeCluster API isn't supported by the .NET driver")]
        protected void WaitUntilCluster(Func<object, bool> predicate)
            => Checks.WaitUntilCluster(Settings.ClusterTimeout, Settings.CheckInterval, AdminClient, predicate, Log);
        
        protected void WaitUntilConsumerGroup(string groupId, Func<GroupInfo, bool> predicate)
            => Checks.WaitUntilConsumerGroup(
                groupId: groupId,
                timeout: Settings.ConsumerGroupTimeout,
                sleepInBetween: Settings.CheckInterval,
                adminClient: AdminClient,
                predicate: predicate,
                log: Log);

        protected void WaitUntilConsumerSummary(string groupId, Func<List<GroupMemberInfo>, bool> predicate)
            => WaitUntilConsumerGroup(groupId, info =>
            {
                return info.State == "Stable" && Try<bool>.From(() => predicate(info.Members)).OrElse(false).Success.Value;
            });

        protected ImmutableList<string> CreateTopics(IEnumerable<int> topics)
            => CreateTopicsAsync(topics).Result;
        
        protected async Task<ImmutableList<string>> CreateTopicsAsync(IEnumerable<int> topics)
        {
            var topicNames = topics.Select(CreateTopicName).ToImmutableList();
            var configs = new Dictionary<string, string>();
            var newTopics = topicNames.Select(topic =>
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 1,
                    ReplicationFactor = 1,
                    Configs = configs
                });
            await AdminClient.CreateTopicsAsync(
                topics: newTopics,
                options: new CreateTopicsOptions {RequestTimeout = TimeSpan.FromSeconds(10)});
            return topicNames;
        }

        protected void PeriodicalCheck<T>(string description, int maxTries, TimeSpan sleepInBetween, Func<T> data, Func<T, bool> predicate)
            => Checks.PeriodicalCheck(description, new TimeSpan(sleepInBetween.Ticks * maxTries), sleepInBetween, data, predicate, Log);

        /// <summary>
        /// Produce messages to topic using specified range and return a Future so the caller can synchronize consumption.
        /// </summary>
        protected Task Produce(string topic, IEnumerable<int> range, int? partition = null)
            => ProduceString(topic, range.Select(i => i.ToString()), partition);
        
        protected Task ProduceString(string topic, IEnumerable<string> range, int? partition = null)
        {
            partition ??= Partition0;
            return Source.From(range)
                // NOTE: If no partition is specified but a key is present a partition will be chosen
                // using a hash of the key. If neither key nor partition is present a partition
                // will be assigned in a round-robin fashion.
                .Select(n => new ProducerRecord<string, string>(topic, partition, DefaultKey, n))
                .RunWith(KafkaProducer.PlainSink(ProducerDefaults().WithProducer(TestProducer)), Sys.Materializer());
        }

        protected Task ProduceTimestamped(string topic, IEnumerable<(int, long)> timestampedRange)
            => Source.From(timestampedRange)
                .Select( tuple =>
                {
                    var (n, ts) = tuple;
                    return new ProducerRecord<string, string>(topic, Partition0, ts, DefaultKey, n.ToString());
                })
                .RunWith(KafkaProducer.PlainSink(ProducerDefaults().WithProducer(TestProducer)), Sys.Materializer());

        protected (IControl, TestSubscriber.Probe<string>) CreateProbe(
            ConsumerSettings<string, string> consumerSettings,
            string[] topics)
            => KafkaConsumer.PlainSource(consumerSettings, Subscriptions.Topics(topics))
                .Select(s => s.Message.Value)
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Sys.Materializer());
    }
}