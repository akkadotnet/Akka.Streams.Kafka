using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        public async Task SupervisionStrategy_Decider_on_Producer_Upstream_should_work()
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
        public async Task SupervisionStrategy_Decider_on_Consumer_Downstream_should_work()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var topicPartition = new TopicPartition(topic, 0);
            var callCount = 0;

            // create a custom Decider with a "Restart" directive in the event of DivideByZeroException
            Directive Decider(Exception cause)
            {
                callCount++;
                if(cause.Message == "BOOM!")
                    return Directive.Restart;
                return Directive.Stop;
            }

            var consumerSettings = CreateConsumerSettings<string>(group);
            var counter = 0;
            
            await Source.From(Enumerable.Range(1, 11))
                .Select(elem => new ProducerRecord<Null, string>(topicPartition, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);
            
            var (_, probe) = KafkaConsumer
                .PlainSource(consumerSettings, Subscriptions.Assignment(topicPartition))
                .Select(c =>
                {
                    counter++;
                    if (counter % 5 == 0)
                        throw new Exception("BOOM!");
                    return c.Message.Value;
                })
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
                .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
                .Run(Materializer);

            probe.Request(10);
            for (var i = 0; i < 9; i++)
            {
                var message = probe.ExpectNext(TimeSpan.FromSeconds(10)); 
                Log.Info(message);
            }
            probe.Cancel();
            
            callCount.Should().Be(2);
        }
        
        [Fact]
        public async Task Committable_consumer_with_failed_downstream_stage_result_should_be_gapless()
        {
            var topic = CreateTopic(1);
            var group = CreateGroup(1);
            var topicPartition = new TopicPartition(topic, 0);

            var consumerSettings = ConsumerSettings<Null, string>.Create(Sys, null, null)
                .WithBootstrapServers(Fixture.KafkaServer)
                .WithStopTimeout(TimeSpan.FromSeconds(1))
                .WithProperty("auto.offset.reset", "earliest")
                .WithProperty("enable.auto.commit", "false")
                .WithGroupId(group);

            var counter = 0;
            
            await Source.From(Enumerable.Range(1, 11))
                .Select(elem => new ProducerRecord<Null, string>(topicPartition, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);
            
            var probe = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.AssignmentWithOffset(new TopicPartitionOffset(topicPartition, Offset.Unset)))
                .Select(t =>
                {
                    counter++;
                    if (counter % 7 == 0)
                        throw new Exception("BOOM!");
                    return t;
                })
                .SelectAsync(1, async elem =>
                {
                    await elem.CommitableOffset.Commit();
                    return elem.Record.Value;
                })
                .ToMaterialized(this.SinkProbe<string>(), Keep.Right)
                .Run(Materializer);

            var messages = new List<string>();
            probe.Request(11);
            for (var i = 0; i < 6; i++)
            {
                messages.Add(probe.ExpectNext(TimeSpan.FromSeconds(5))); 
            }

            // stream fails at index 7
            var err = probe.ExpectEvent();
            err.Should().BeOfType<TestSubscriber.OnError>();
            var exception = ((TestSubscriber.OnError)err).Cause;
            exception.Message.Should().Be("BOOM!");

            // stream should be dead here
            probe.ExpectNoMsg(TimeSpan.FromSeconds(5));
            probe.Cancel();
            
            // restart dead stream
            probe = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.AssignmentWithOffset(new TopicPartitionOffset(topicPartition, Offset.Unset)))
                .Select(t =>
                {
                    counter++;
                    if (counter % 7 == 0)
                        throw new Exception("BOOM!");
                    return t;
                })
                .SelectAsync(1, async elem =>
                {
                    await elem.CommitableOffset.Commit();
                    return elem.Record.Value;
                })
                .ToMaterialized(this.SinkProbe<string>(), Keep.Right)
                .Run(Materializer);
            
            probe.Request(11);
            for (var i = 0; i < 5; i++)
            {
                messages.Add(probe.ExpectNext(TimeSpan.FromSeconds(5))); 
            }
            probe.Cancel();

            // end result should be gapless
            messages.Select(s => int.Parse(s)).Should().BeEquivalentTo(Enumerable.Range(1, 11));
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
        
        [Fact]
        public async Task SupervisionStrategy_Decider_on_PlainSink_should_work()
        {
            var callCount = 0;
            Directive Decider(Exception cause)
            {
                callCount++;
                switch (cause)
                {
                    case ProduceException<Null, string> ex when ex.Error.IsSerializationError():
                        return Directive.Resume;
                    default:
                        return Directive.Stop;
                }
            }

            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            
            var producerSettings = ProducerSettings<Null, string>
                .Create(Sys, null, new FailingSerializer())
                .WithBootstrapServers(Fixture.KafkaServer);
            
            var sourceTask = Source
                .From(new []{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
                .Select(elem => new ProducerRecord<Null, string>(new TopicPartition(topic1, 0), elem.ToString()))
                .RunWith(
                    KafkaProducer.PlainSink(producerSettings)
                        .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider)), 
                    Materializer);

            await GuardWithTimeoutAsync(sourceTask, TimeSpan.FromSeconds(5));
            
            var settings = CreateConsumerSettings<Null, string>(group1).WithValueDeserializer(new StringDeserializer());
            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            probe.Request(10);
            for (var i = 0; i < 9; i++)
            {
                var message = probe.ExpectNext();
                Log.Info($"> [{i}]: {message}");
            }
            callCount.Should().Be(1);
            probe.Cancel();
        }
        
        [Fact]
        public async Task SupervisionStrategy_Decider_on_PlainSource_should_work()
        {
            var callCount = 0;
            Directive Decider(Exception cause)
            {
                callCount++;
                if(cause is ConsumeException ex && ex.Error.IsSerializationError())
                {
                    return Directive.Resume;
                }
                return Directive.Stop;
            }

            int elementsCount = 10;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            var sourceTask = ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            await GuardWithTimeoutAsync(sourceTask, TimeSpan.FromSeconds(3));
            
            var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<int>(), Materializer);

            probe.Request(elementsCount);
            probe.ExpectNoMsg(TimeSpan.FromSeconds(10));
            callCount.Should().Be(elementsCount);
            probe.Cancel();
        }        
        
        [Fact]
        public async Task SupervisionStrategy_Decider_on_PlainSource_should_stop_on_internal_error()
        {
            int elementsCount = 10;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            var sourceTask = ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            await GuardWithTimeoutAsync(sourceTask, TimeSpan.FromSeconds(3));
            
            var settings = CreateConsumerSettings<Null, string>(group1).WithAutoCreateTopicsEnabled(false);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 5)))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            probe.Request(elementsCount);
            var error = probe.ExpectEvent(TimeSpan.FromSeconds(5));
            error.Should().BeOfType<TestSubscriber.OnError>();
            var exception = ((TestSubscriber.OnError)error).Cause;
            exception.Should().BeOfType<KafkaException>();
            ((KafkaException) exception).Error.Code.Should().Be(ErrorCode.Local_UnknownPartition);
            
            probe.Cancel();
        }
        
        [Fact]
        public async Task PlainSource_should_stop_on_errors()
        {
            int elementsCount = 10;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);

            await ProduceStrings(new TopicPartition(topic1, 0), Enumerable.Range(1, elementsCount), ProducerSettings);

            var settings = CreateConsumerSettings<int>(group1).WithValueDeserializer(Deserializers.Int32);

            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(Deciders.StoppingDecider))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<int>(), Materializer);

            var error = probe.Request(elementsCount).ExpectEvent(TimeSpan.FromSeconds(5));
            error.Should().BeOfType<TestSubscriber.OnError>();
            var exception = ((TestSubscriber.OnError)error).Cause;
            exception.Should().BeOfType<ConsumeException>();
            ((ConsumeException) exception).Error.IsSerializationError().Should().BeTrue();

            probe.ExpectNoMsg(TimeSpan.FromSeconds(5));
            probe.Cancel();
        }

        private static async Task GuardWithTimeoutAsync(Task asyncTask, TimeSpan timeout)
        {
            var cts = new CancellationTokenSource();
            try
            {
                var timeoutTask = Task.Delay(timeout, cts.Token);
                var completedTask = await Task.WhenAny(asyncTask, timeoutTask);
                if (completedTask == timeoutTask)
                    throw new TimeoutException($"Task exceeds timeout duration {timeout}");
                else
                    cts.Cancel();
            }
            finally
            {
                cts.Dispose();
            }
        }
        
        private class FailingSerializer: ISerializer<string>
        {
            public byte[] Serialize(string data, SerializationContext context)
            {
                var i = int.Parse(data);
                if (i == 5)
                    throw new Exception("BOOM");
                return BitConverter.GetBytes(i);
            }
        }
        
        private class StringDeserializer: IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return BitConverter.ToInt32(data).ToString();
            }
        }
    }
}