using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;
using Config = Akka.Configuration.Config;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class CommittableSourceIntegrationTests : KafkaIntegrationTests
    {
        private const string InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it";
        private readonly KafkaFixture _fixture;
        private readonly ActorMaterializer _materializer;

        public CommittableSourceIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(CommittableSourceIntegrationTests), output)
        {
            _fixture = fixture;
            _materializer = Sys.Materializer();
        }

        private string Uuid { get; } = Guid.NewGuid().ToString();

        private string CreateTopic(int number) => $"topic-{number}-{Uuid}";
        private string CreateGroup(int number) => $"group-{number}-{Uuid}";

        private ProducerSettings<Null, string> ProducerSettings
        {
            get => ProducerSettings<Null, string>.Create(Sys, null, null).WithBootstrapServers(_fixture.KafkaServer);
        }

        private async Task GivenInitializedTopic(string topic)
        {
            using (var producer = ProducerSettings.CreateKafkaProducer())
            {
                await producer.ProduceAsync(topic, new Message<Null, string> { Value = InitialMsg });
            }
        }

        private ConsumerSettings<Null, string> CreateConsumerSettings(string group)
        {
            return ConsumerSettings<Null, string>.Create(Sys, null, null)
                .WithBootstrapServers(_fixture.KafkaServer)
                .WithProperty("auto.offset.reset", "earliest")
                .WithGroupId(group);
        }

        [Fact]
        public async Task CommitableSource_consumes_messages_from_Producer_without_commits()
        {
            int elementsCount = 100;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await GivenInitializedTopic(topic1);

            await Source
                .From(Enumerable.Range(1, elementsCount))
                .Select(elem => new MessageAndMeta<Null, string> { Topic = topic1, Message = new Message<Null, string> { Value = elem.ToString() } })
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), _materializer);

            var consumerSettings = CreateConsumerSettings(group1);

            var probe = KafkaConsumer
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
        public async Task CommitableSource_resume_from_commited_offset()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var group2 = CreateGroup(2);

            await GivenInitializedTopic(topic1);

            await Source
                .From(Enumerable.Range(1, 100))
                .Select(elem => new MessageAndMeta<Null, string> { Topic = topic1, Message = new Message<Null, string> { Value = elem.ToString() } })
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), _materializer);

            var consumerSettings = CreateConsumerSettings(group1);
            var committedElements = new ConcurrentQueue<string>();

            var (task, probe1) = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WhereNot(c => c.Record.Value == InitialMsg)
                .SelectAsync(10, async elem =>
                {
                    await elem.CommitableOffset.Commit();
                    committedElements.Enqueue(elem.Record.Value);
                    return Done.Instance;
                })
                .ToMaterialized(this.SinkProbe<Done>(), Keep.Both)
                .Run(_materializer);

            probe1.Request(25);

            foreach (var _ in Enumerable.Range(1, 25))
            {
                probe1.ExpectNext(Done.Instance, TimeSpan.FromSeconds(10));
            }
                
            probe1.Cancel();

            AwaitCondition(() => task.IsCompletedSuccessfully);

            var probe2 = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .Select(_ => _.Record.Value)
                .RunWith(this.SinkProbe<string>(), _materializer);

            // Note that due to buffers and SelectAsync(10) the committed offset is more
            // than 26, and that is not wrong

            // some concurrent publish
            await Source
                .From(Enumerable.Range(101, 100))
                .Select(elem => new MessageAndMeta<Null, string> { Topic = topic1, Message = new Message<Null, string> { Value = elem.ToString() } })
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), _materializer);

            probe2.Request(100);
            foreach (var i in Enumerable.Range(committedElements.Count + 1, 100).Select(c => c.ToString()))
                probe2.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe2.Cancel();

            // another consumer should see all
            var probe3 = KafkaConsumer.CommittableSource(consumerSettings.WithGroupId(group2), Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WhereNot(c => c.Record.Value == InitialMsg)
                .Select(_ => _.Record.Value)
                .RunWith(this.SinkProbe<string>(), _materializer);

            probe3.Request(100);
            foreach (var i in Enumerable.Range(1, 100).Select(c => c.ToString()))
                probe3.ExpectNext(i, TimeSpan.FromSeconds(10));

            probe3.Cancel();
        }
    }
}
