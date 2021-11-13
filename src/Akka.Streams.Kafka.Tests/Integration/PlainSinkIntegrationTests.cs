using System;
using System.Collections.Generic;
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
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Directive = Akka.Streams.Supervision.Directive;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class PlainSinkIntegrationTests : KafkaIntegrationTests
    {
        public PlainSinkIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(null, output, fixture)
        {
        }

        [Fact]
        public async Task PlainSink_should_publish_100_elements_to_Kafka_producer()
        {
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            var topicPartition1 = new TopicPartition(topic1, 0);

            await GivenInitializedTopic(topicPartition1);

            var consumerSettings = CreateConsumerSettings<string>(group1);
            var consumer = consumerSettings.ConsumerFactory != null 
                ? consumerSettings.ConsumerFactory(consumerSettings) 
                : consumerSettings.CreateKafkaConsumer();
            consumer.Assign(new List<TopicPartition> { topicPartition1 });

            var task = new TaskCompletionSource<NotUsed>();
            int messagesReceived = 0;

            await Source
                .From(Enumerable.Range(1, 100))
                .Select(c => c.ToString())
                .Select(elem => new ProducerRecord<Null, string>(topicPartition1, elem.ToString()))
                .RunWith(KafkaProducer.PlainSink(ProducerSettings), Materializer);

            var dateTimeStart = DateTime.UtcNow;

            bool CheckTimeout(TimeSpan timeout)
            {
                return dateTimeStart.AddSeconds(timeout.TotalSeconds) > DateTime.UtcNow;
            }

            while (!task.Task.IsCompleted && CheckTimeout(TimeSpan.FromMinutes(1)))
            {
                try
                {
                    consumer.Consume(TimeSpan.FromSeconds(1));
                    messagesReceived++;
                    if (messagesReceived == 100)
                        task.SetResult(NotUsed.Instance);
                }
                catch (ConsumeException) { /* Just try again */ }
            }

            messagesReceived.Should().Be(100);
        }

        [Fact]
        public async Task PlainSink_should_fail_stage_if_broker_unavailable()
        {
            var topic1 = CreateTopic(1);

            await GivenInitializedTopic(topic1);

            var config = ProducerSettings<Null, string>.Create(Sys, null, null)
                .WithBootstrapServers("localhost:10092");

            var probe = Source
                .From(Enumerable.Range(1, 100))
                .Select(c => c.ToString())
                .Select(elem => new ProducerRecord<Null, string>(topic1, elem.ToString()))
                .Select(record => new Message<Null, string, NotUsed>(record, NotUsed.Instance) as IEnvelope<Null, string, NotUsed>)
                .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(config))
                .RunWith(this.SinkProbe<IResults<Null, string, NotUsed>>(), Materializer);

            probe.ExpectSubscription();
            probe.OnError(new KafkaException(ErrorCode.Local_Transport));
        }
        
        [Fact]
        public async Task PlainSink_should_resume_on_deserialization_errors()
        {
            var callCount = 0;
            Directive Decider(Exception cause)
            {
                switch (cause)
                {
                    case ProduceException<Null, string> ex when ex.Error.IsSerializationError():
                        callCount++;
                        return Directive.Resume;
                    default:
                        return Directive.Stop;
                }
            }

            var elementsCount = 10;
            var topic1 = CreateTopic(1);
            var group1 = CreateGroup(1);
            
            var producerSettings = ProducerSettings<Null, string>
                .Create(Sys, null, new FailingSerializer())
                .WithBootstrapServers(Fixture.KafkaServer);
            
            var sourceTask = Source
                .From(new []{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
                .Select(elem => new ProducerRecord<Null, string>(new TopicPartition(topic1, 0), elem.ToString()))
                .RunWith(
                    KafkaProducer.PlainSink(producerSettings).WithAttributes(ActorAttributes.CreateSupervisionStrategy(Decider)), 
                    Materializer);
            
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var completeTask = await Task.WhenAny(sourceTask, timeoutTask);
            if (completeTask == timeoutTask)
                throw new Exception("Producer timed out");

            var settings = CreateConsumerSettings<Null, string>(group1).WithValueDeserializer(new StringDeserializer());
            var probe = KafkaConsumer
                .PlainSource(settings, Subscriptions.Assignment(new TopicPartition(topic1, 0)))
                .Select(c => c.Value)
                .RunWith(this.SinkProbe<string>(), Materializer);

            probe.Request(elementsCount);
            for (var i = 0; i < 9; i++)
            {
                Log.Info($">>>>>>>>>>> {i}");
                probe.ExpectNext();
            }
            callCount.Should().Be(1);
            probe.Cancel();
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
