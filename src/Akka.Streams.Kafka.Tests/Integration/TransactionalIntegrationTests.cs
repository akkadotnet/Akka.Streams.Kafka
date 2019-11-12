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

        [Fact(Skip = "Missing producer transactions support, see https://github.com/akkadotnet/Akka.Streams.Kafka/issues/85")]
        public async Task Transactional_source_with_sink_Should_work()
        {
            var settings = CreateConsumerSettings<string>(CreateGroup(1));
            var sourceTopic = CreateTopic(1);
            var targetTopic = CreateTopic(2);
            var transactionalId = Guid.NewGuid().ToString();
            const int totalMessages = 10;
            
            var control = KafkaConsumer.TransactionalSource(settings, Subscriptions.Topics(sourceTopic))
                .Via(Business<TransactionalMessage<Null, string>>())
                .Select(message =>
                {
                    return ProducerMessage.Single(
                        new ProducerRecord<Null, string>(targetTopic, message.Record.Key, message.Record.Value),
                        passThrough: message.PartitionOffset);
                })
                .ToMaterialized(KafkaProducer.TransactionalSink(ProducerSettings, transactionalId), Keep.Both)
                .MapMaterializedValue(DrainingControl<NotUsed>.Create)
                .Run(Materializer);

            var consumer = ConsumeStrings(targetTopic, totalMessages);

            await ProduceStrings(sourceTopic, Enumerable.Range(1, totalMessages), ProducerSettings);

            AssertTaskCompletesWithin(TimeSpan.FromSeconds(totalMessages), consumer.IsShutdown);
            AssertTaskCompletesWithin(TimeSpan.FromSeconds(totalMessages), control.DrainAndShutdown());

            consumer.DrainAndShutdown().Result.Should().HaveCount(totalMessages);
        }

        private Flow<T, T, NotUsed> Business<T>() => Flow.Create<T>();

        private DrainingControl<IImmutableList<ConsumeResult<Null, string>>> ConsumeStrings(string topic, int count)
        {
            return KafkaConsumer.PlainSource(CreateConsumerSettings<string>(CreateGroup(1)), Subscriptions.Topics(topic))
                .Take(count)
                .ToMaterialized(Sink.Seq<ConsumeResult<Null, string>>(), Keep.Both)
                .MapMaterializedValue(DrainingControl<IImmutableList<ConsumeResult<Null, string>>>.Create)
                .Run(Materializer);
        }
    }
}