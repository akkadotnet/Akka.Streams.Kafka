using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class CommittableSourceIntegrationTests : Akka.TestKit.Xunit2.TestKit
    {
        private const string KafkaUrl = "localhost:9092";

        private const string InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it";

        private readonly ActorMaterializer _materializer;

        public CommittableSourceIntegrationTests(ITestOutputHelper output) 
            : base(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"), null, output)
        {
            _materializer = Sys.Materializer();
        }

        private string Uuid { get; } = Guid.NewGuid().ToString();

        private string CreateTopic(int number) => $"topic-{number}-{Uuid}";
        private string CreateGroup(int number) => $"group-{number}-{Uuid}";

        private ProducerSettings<Null, string> ProducerSettings =>
            ProducerSettings<Null, string>.Create(Sys, null, new StringSerializer(Encoding.UTF8))
                .WithBootstrapServers(KafkaUrl);

        private Task Produce(string topic, IEnumerable<string> range, ProducerSettings<Null, string> settings = null)
        {
            settings = settings ?? ProducerSettings;
            var source = Source
                .From(range)
                .Select(elem => new ProduceRecord<Null, string>(topic, null, elem.ToString()))
                .ViaMaterialized(Dsl.Producer.CreateFlow(settings), Keep.Both);

            return source.RunWith(Sink.Ignore<Message<Null, string>>(), _materializer);
        }

        private TestSubscriber.Probe<string> CreateProbe(ConsumerSettings<Null, string> consumerSettings, string topic, ISubscription sub)
        {
            return Dsl.Consumer
                .PlainSource(consumerSettings, sub)
                .Where(c => !c.Value.Equals(InitialMsg))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<string>(), _materializer);
        }

        private async Task GivenInitializedTopic(string topic)
        {
            var producer = ProducerSettings.CreateKafkaProducer();
            await producer.ProduceAsync(topic, null, InitialMsg, 0);
            producer.Dispose();
        }

        private ConsumerSettings<Null, string> CreateConsumerSettings(string group)
        {
            return ConsumerSettings<Null, string>.Create(Sys, null, new StringDeserializer(Encoding.UTF8))
                .WithBootstrapServers(KafkaUrl)
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }

        [Fact]
        public async Task CommittableSource_must_consume_messages_from_Producer_without_commits()
        {
            int elementsCount = 100;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await GivenInitializedTopic(topic1);

            await Source
                .From(Enumerable.Range(1, elementsCount))
                .Select(elem => new ProduceRecord<Null, string>(topic1, null, elem.ToString()))
                .RunWith(Dsl.Producer.PlainSink(ProducerSettings), _materializer);

            var consumerSettings = CreateConsumerSettings(group1);

            var probe = Dsl.Consumer
                .CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .Where(c => !c.Record.Value.Equals(InitialMsg))
                .Select(c => c.Record.Value)
                .RunWith(this.SinkProbe<string>(), _materializer);

            probe.Request(elementsCount);
            foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
                probe.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe.Cancel();
        }

        [Fact]
        public async Task CommittableSource_must_resume_from_commited_offset()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var group2 = CreateGroup(2);

            await GivenInitializedTopic(topic1);

            await Source
                .From(Enumerable.Range(1, 100))
                .Select(elem => new ProduceRecord<Null, string>(topic1, null, elem.ToString()))
                .RunWith(Dsl.Producer.PlainSink(ProducerSettings), _materializer);

            var consumerSettings = CreateConsumerSettings(group1);
            var committedElements = new ConcurrentQueue<string>();

            var (_, probe1) = Dsl.Consumer.CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WhereNot(c => c.Record.Value == InitialMsg)
                .SelectAsync(10, elem =>
                {
                    return elem.CommitableOffset.Commit().ContinueWith(t =>
                    {
                        committedElements.Enqueue(elem.Record.Value);
                        return Done.Instance;
                    });
                })
                .ToMaterialized(this.SinkProbe<Done>(), Keep.Both)
                .Run(_materializer);

            probe1.Request(25);

            foreach (var _ in Enumerable.Range(1, 25))
            {
                probe1.ExpectNext(Done.Instance, TimeSpan.FromSeconds(10));
            }
                
            probe1.Cancel();

            // Await.result(control.isShutdown, remainingOrDefault)

            var probe2 = Dsl.Consumer.CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .Select(_ => _.Record.Value)
                .RunWith(this.SinkProbe<string>(), _materializer);

            // Note that due to buffers and SelectAsync(10) the committed offset is more
            // than 26, and that is not wrong

            // some concurrent publish
            await Source
                .From(Enumerable.Range(101, 100))
                .Select(elem => new ProduceRecord<Null, string>(topic1, null, elem.ToString()))
                .RunWith(Dsl.Producer.PlainSink(ProducerSettings), _materializer);

            probe2.Request(100);
            foreach (var i in Enumerable.Range(committedElements.Count + 1, 100).Select(c => c.ToString()))
                probe2.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe2.Cancel();

            // another consumer should see all
            var probe3 = Dsl.Consumer.CommittableSource(consumerSettings.WithGroupId(group2), Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WhereNot(c => c.Record.Value == InitialMsg)
                .Select(_ => _.Record.Value)
                .RunWith(this.SinkProbe<string>(), _materializer);

            probe3.Request(100);
            foreach (var i in Enumerable.Range(1, 100).Select(c => c.ToString()))
                probe3.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe3.Cancel();
        }

        [Fact]
        public async Task CommittableSource_must_handle_commit_without_demand()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await GivenInitializedTopic(topic1);

            // important to use more messages than the internal buffer sizes
            // to trigger the intended scenario
            Produce(topic1, Enumerable.Range(1, 100).Select(c => c.ToString()))
                .Wait(RemainingOrDefault);

            var consumerSettings = CreateConsumerSettings(group1);

            var (control, probe1) = Dsl.Consumer.CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WhereNot(c => c.Record.Value == InitialMsg)
                .ToMaterialized(this.SinkProbe<CommittableMessage<Null, string>>(), Keep.Both)
                .Run(_materializer);

            // request one, only
            probe1.Request(1);

            var commitableOffset = probe1.ExpectNext().CommitableOffset;

            // enqueue some more
            Produce(topic1, Enumerable.Range(101, 10).Select(c => c.ToString()))
                .Wait(RemainingOrDefault);

            probe1.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

            // then commit, which triggers a new poll while we haven't drained
            // previous buffer
            var done1 = commitableOffset.Commit();
            done1.Wait(RemainingOrDefault);

            probe1.Request(1);
            var done2 = probe1.ExpectNext().CommitableOffset.Commit();
            done2.Wait(RemainingOrDefault);

            probe1.Cancel();
            // TODO: Await.result(control.isShutdown, remainingOrDefault)
        }

        [Fact]
        public async Task CommittableSource_must_consume_and_commit_in_batches()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await GivenInitializedTopic(topic1);

            // important to use more messages than the internal buffer sizes
            // to trigger the intended scenario
            Produce(topic1, Enumerable.Range(1, 100).Select(c => c.ToString()))
                .Wait(RemainingOrDefault);

            var consumerSettings = CreateConsumerSettings(group1);

            Tuple<Task, TestSubscriber.Probe<CommittedOffsets>> ConsumeAndBatchCommit(string topic)
            {
                return Dsl.Consumer.CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic, 0)))
                    .Select(msg => msg.CommitableOffset)
                    .Batch(
                        max: 10,
                        seed: CommittableOffsetBatch.Empty.Updated,
                        aggregate: (batch, elem) => batch.Updated(elem))
                    .SelectAsync(1, c => c.Commit())
                    .ToMaterialized(this.SinkProbe<CommittedOffsets>(), Keep.Both)
                    .Run(_materializer);
            }

            var (control, probe) = ConsumeAndBatchCommit(topic1);

            // Request one batch
            probe.Request(1).ExpectNextN(1);

            probe.Cancel();
            // TODO: Await.result(control.isShutdown, remainingOrDefault)

            // Resume consumption
            var consumerSettings2 = CreateConsumerSettings(group1);
            var probe2 = CreateProbe(consumerSettings2, topic1, Subscriptions.Assignment(new TopicPartition(topic1, 0)));

            var element = probe2.Request(1).ExpectNext();
            int.Parse(element).Should().BeGreaterThan(1, "Should start after first element");
            probe2.Cancel();
        }
    }
}

