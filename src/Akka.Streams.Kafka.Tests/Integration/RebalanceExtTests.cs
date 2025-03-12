using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class RebalanceExtTests : KafkaIntegrationTests
{
    public RebalanceExtTests(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(RebalanceExtTests), output, fixture)
    {
    }

    private const string ConsumerClientId1 = "consumer-1";
    private const string ConsumerClientId2 = "consumer-2";

    private sealed record TopicPartitionMetadata(
        IReadOnlyCollection<string> Topics,
        IReadOnlyCollection<TopicPartition> Partitions,
        IReadOnlyDictionary<string, TaskCompletionSource<Done>> TpFutureMap,
        IReadOnlyCollection<Task<Done>> ProducerTpsAck,
        IReadOnlyDictionary<int, MessageAck> MessageAndStoreAck);

    private sealed record MessageAck(
        string PartitionName,
        AtomicCounter MessageCounter,
        TaskCompletionSource<Done> WaitUntil,
        TaskCompletionSource<Done> AckWaitUntil);

    private sealed class LoggingAssignmentHandler(string clientId, ILoggingAdapter log) : IPartitionEventHandler
    {
        private readonly string _clientId = clientId;
        private readonly ILoggingAdapter _log = log;

        public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions,
            IRestrictedConsumer consumer) =>
            _log.Debug("AssignmentHandler::OnRevoke: clientId {0} tps {1} consumer {2}", _clientId,
                revokedTopicPartitions, consumer);

        public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer) =>
            _log.Debug("AssignmentHandler::OnLost: clientId {0} tps {1} consumer {2}", _clientId,
                revokedTopicPartitions, consumer);

        public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer) =>
            _log.Debug("AssignmentHandler::OnAssign: clientId {0} tps {1} consumer {2}", _clientId,
                assignedTopicPartitions, consumer);

        public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer) =>
            _log.Debug("AssignmentHandler::OnStop: clientId {0} tps {1} consumer {2}", _clientId,
                topicPartitions, consumer);
    }

    private ConsumerSettings<Null, string> CreateConsumerSettings(string groupId) =>
        CreateConsumerSettings<Null, string>(groupId)
            .WithPollInterval(TimeSpan.FromMilliseconds(400));

    private (IControl control, Task<IImmutableList<Task<Done>>> tasks) SubscribeAndConsumeMessages(string clientId,
        int perPartitionMessageCount, TopicPartitionMetadata topicMetaData,
        ConsumerSettings<Null, string> consumerSettings,
        IAutoSubscription subscription, SharedKillSwitch sharedKillSwitch)
    {
        var logPrefix = $"[{clientId}]";
        return KafkaConsumer.CommittablePartitionedSource(consumerSettings
                    .WithClientId(clientId),
                subscription.WithPartitionEventsHandler(new LoggingAssignmentHandler(clientId, Log)))
            .Select(tuple =>
            {
                var (tp, source) = tuple;
                Log.Debug("{0} Consuming partitioned source clientId: {1}, for tp: {2}", logPrefix, clientId, tp);
                var innerStream = source
                    .Via(sharedKillSwitch.Flow<CommittableMessage<Null, string>>())
                    .Via(BusinessFlow(clientId, perPartitionMessageCount, topicMetaData, logPrefix))
                    .Via(Committer.BatchFlow(CommitterSettings.WithMaxBatch(1)));

                return innerStream.RunWith(Sink.Ignore<ICommittableOffsetBatch>(), Materializer);
            })
            .Via(sharedKillSwitch.Flow<Task<Done>>())
            .ToMaterialized(Sink.Seq<Task<Done>>(), Keep.Both)
            .Run(Materializer);
    }

    private Flow<CommittableMessage<Null, string>, ICommittable, NotUsed> BusinessFlow(string clientId,
        int perPartitionMessageCount, TopicPartitionMetadata topicMetadata, string logPrefix)
    {
        return Flow.FromFunction<CommittableMessage<Null, string>, Task<ICommittable>>(ProcessMsg)
            .SelectAsync(1, async t => await t);

        async Task<ICommittable> ProcessMsg(CommittableMessage<Null, string> message)
        {
            var messageVal = int.Parse(message.Record.Message.Value);
            var duplicateCount = topicMetadata.MessageAndStoreAck[messageVal].MessageCounter.IncrementAndGet();
            var partitionName = $"{message.Record.Topic}-{message.Record.Partition}";
            var msg1 =
                $"{logPrefix}::businessFlow:offset={message.CommitableOffset.Offset.Offset} messageId={messageVal} partition={partitionName} consumerId={clientId} duplicateCount={duplicateCount}";
            
            if (duplicateCount > 1)
            {
                Log.Warning("businessFlow:duplicate:{0}", msg1);
            }
            else
            {
                Log.Info("businessFlow:received:{0}", msg1);
            }
            
            // Flow of execution is blocked, but not the thread
            Log.Info("{0}::businessFlow::begin:blockAtMessage={1}", logPrefix, msg1);
            await topicMetadata.MessageAndStoreAck[messageVal].WaitUntil.Task.WaitAsync(RemainingOrDefault);
            var ackWaitUntilPromise = topicMetadata.MessageAndStoreAck[messageVal].AckWaitUntil;
            if (ackWaitUntilPromise.Task.IsCompleted)
            {
                Log.Info("{0}::businessFlow::ignore:blockAtMessage={1}", logPrefix, msg1);
            }
            else
            {
                ackWaitUntilPromise.TrySetResult(Done.Instance);
                Log.Info("{0}::businessFlow::end:blockAtMessage={1}", logPrefix, msg1);
            }

            if (message.CommitableOffset.Offset.Offset == perPartitionMessageCount - 1)
            {
                var lastMessagePromise = topicMetadata.TpFutureMap[message.Record.Topic];
                if(lastMessagePromise.Task.IsCompleted)
                    Log.Warning("{0}::businessFlow::promise:already completed={1}", logPrefix, msg1);
                else
                {
                    Log.Info("{0}::businessFlow::promise:completing={1}", logPrefix, msg1);
                    lastMessagePromise.TrySetResult(Done.Instance);
                    
                }
            }

            return message.CommitableOffset;
        }
    }
}