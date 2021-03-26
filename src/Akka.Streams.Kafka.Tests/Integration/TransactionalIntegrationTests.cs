using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration
{
    public class TransactionalIntegrationTests : KafkaIntegrationTests
    {
        public TransactionalIntegrationTests(ITestOutputHelper output, KafkaFixture fixture) 
            : base(nameof(TransactionalIntegrationTests), output, fixture)
        {
        }

        [Fact]
        public async Task Transactional_source_with_sink_Should_work()
        {
            var consumerSettings = CreateConsumerSettings<string>(CreateGroup(1));
            var sourceTopic = CreateTopic(1);
            var targetTopic = CreateTopic(2);
            var transactionalId = Guid.NewGuid().ToString();
            const int totalMessages = 10;

            await ProduceStrings(sourceTopic, Enumerable.Range(1, totalMessages), ProducerSettings);

            var control = KafkaConsumer.TransactionalSource(consumerSettings, Subscriptions.Topics(sourceTopic))
                .Select(message =>
                {
                    return ProducerMessage.Single(
                        new ProducerRecord<Null, string>(targetTopic, message.Record.Message.Key, message.Record.Message.Value),
                        passThrough: message.PartitionOffset);
                })
                .ToMaterialized(KafkaProducer.TransactionalSink(ProducerSettings, transactionalId), Keep.Both)
                .MapMaterializedValue(DrainingControl<NotUsed>.Create)
                .Run(Materializer);

            var consumer = ConsumeStrings(targetTopic, totalMessages, CreateConsumerSettings<Null, string>(CreateGroup(2)));

            AssertTaskCompletesWithin(TimeSpan.FromSeconds(30), consumer.IsShutdown);
            AssertTaskCompletesWithin(TimeSpan.FromSeconds(30), control.DrainAndShutdown());

            consumer.DrainAndShutdown().Result.Should().HaveCount(totalMessages);
        }

        private DrainingControl<IImmutableList<ConsumeResult<Null, string>>> ConsumeStrings(
            string topic, 
            int count, 
            ConsumerSettings<Null, string> settings)
        {
            return KafkaConsumer.PlainSource(settings, Subscriptions.Topics(topic))
                .Take(count)
                .ToMaterialized(Sink.Seq<ConsumeResult<Null, string>>(), Keep.Both)
                .MapMaterializedValue(DrainingControl<IImmutableList<ConsumeResult<Null, string>>>.Create)
                .Run(Materializer);
        }
    }
}