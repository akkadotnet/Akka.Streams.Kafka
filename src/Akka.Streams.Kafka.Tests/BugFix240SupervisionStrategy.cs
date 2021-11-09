using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Supervision;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Akka.Streams.TestKit;

namespace Akka.Streams.Kafka.Tests
{
    public class BugFix240SupervisionStrategy: KafkaIntegrationTests
    {
        public BugFix240SupervisionStrategy(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(BugFix240SupervisionStrategy), output, fixture)
        {
        }

        [Fact]
        public async Task SupervisionStrategy_Decider_on_Producer_should_work()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var topicPartition = new TopicPartition(topic, 0);
            var callCount = 0;

            // create a custom Decider with a "Restart" directive in the event of DivideByZeroException
            Directive Decider(Exception cause)
            {
                callCount++;
                return Directive.Resume;
            }

            var consumerSettings = CreateConsumerSettings<string>(group);
            var numbers = Source.From(new []{ 9,8,7,6,0,5,4,3,2,1 });
            await numbers
                .Via(Flow.Create<int>().Select(x => $"1/{x} is {1/x} w/ integer division"))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
                .Select(elem => new ProducerRecord<Null, string>(topicPartition, elem))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);
            
            var (_, probe) = KafkaConsumer
                .PlainSource(consumerSettings, Subscriptions.Assignment(topicPartition))
                .Select(c => c.Message.Value)
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Materializer);

            probe.Request(10);
            for (var i = 0; i < 9; i++)
            {
                Log.Info(probe.ExpectNext(TimeSpan.FromSeconds(10)));
            }
            probe.Cancel();
            
            callCount.Should().BeGreaterThan(0);
        }
        
        [Fact]
        public async Task SupervisionStrategy_Decider_on_Consumer_should_work()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var topicPartition = new TopicPartition(topic, 0);
            var callCount = 0;

            // create a custom Decider with a "Restart" directive in the event of DivideByZeroException
            Directive Decider(Exception cause)
            {
                callCount++;
                return Directive.Resume;
            }

            var consumerSettings = CreateConsumerSettings<string>(group);
            var counter = 0;
            
            await Source.From(Enumerable.Range(1, 11))
                .Via(Flow.Create<int>().Select(x => $"1/{x} is {1/x} w/ integer division"))
                .Select(elem => new ProducerRecord<Null, string>(topicPartition, elem))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);
            
            var (_, probe) = KafkaConsumer
                .PlainSource(consumerSettings, Subscriptions.Assignment(topicPartition))
                .Via(Flow.Create<ConsumeResult<Null, string>>().Select(x =>
                {
                    counter++;
                    if (counter % 5 == 0)
                        throw new Exception("BOOM!");
                    return x;
                }))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
                .Select(c => c.Message.Value)
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Materializer);

            probe.Request(10);
            for (var i = 0; i < 9; i++)
            {
                Log.Info(probe.ExpectNext(TimeSpan.FromSeconds(10)));
            }
            probe.Cancel();
            
            callCount.Should().BeGreaterThan(0);
        }
        
        [Fact]
        public async Task SupervisionStrategy_Decider_on_complex_stream_should_work()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var topicPartition = new TopicPartition(topic, 0);
            var callCount = 0;

            // create a custom Decider with a "Restart" directive in the event of DivideByZeroException
            Directive Decider(Exception cause)
            {
                callCount++;
                return Directive.Resume;
            }

            var committerSettings = CommitterSettings.Create(Sys);
            var consumerSettings = CreateConsumerSettings<string>(group);
            var counter = 0;

            await Source.From(Enumerable.Range(1, 11))
                .Via(Flow.Create<int>().Select(x => $"1/{x} is {1/x} w/ integer division"))
                .Select(elem => new ProducerRecord<Null, string>(topicPartition, elem))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);

            var drainingControl = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Assignment(topicPartition))
                .Via(Flow.Create<CommittableMessage<Null, string>>().Select(x =>
                {
                    counter++;
                    if (counter % 5 == 0)
                        throw new Exception("BOOM!");
                    return x;
                }))
                .WithAttributes(Attributes.CreateName("CommitableSource").And(ActorAttributes.CreateSupervisionStrategy(Decider)))
                .Select(c => (c.Record.Topic, c.Record.Message.Value, c.CommitableOffset))
                .SelectAsync(1, async t =>
                {
                    Log.Info($"[{t.Topic}]: {t.Value}");
                    // simulate a request-response call that takes 10ms to complete here
                    await Task.Delay(10);
                    return t;
                })
                .Select(t => ProducerMessage.Single(new ProducerRecord<Null, string>($"{t.Topic}-done", t.Value),
                    t.CommitableOffset))
                .Via(KafkaProducer.FlexiFlow<Null, string, ICommittableOffset>(ProducerSettings)).WithAttributes(Attributes.CreateName("FlexiFlow"))
                .Select(m => (ICommittable)m.PassThrough)
                .AlsoToMaterialized(Committer.Sink(committerSettings), DrainingControl<NotUsed>.Create)
                .To(Flow.Create<ICommittable>()
                    .Async()
                    .GroupedWithin(1000, TimeSpan.FromSeconds(1))
                    .Select(c => c.Count())
                    .Log("MsgCount").AddAttributes(Attributes.CreateLogLevels(LogLevel.InfoLevel))
                    .To(Sink.Ignore<int>()))
                .Run(Sys.Materializer());

            await Task.Delay(TimeSpan.FromSeconds(2));
            await drainingControl.Shutdown();
            callCount.Should().BeGreaterThan(0);
        }
    }
}