using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class CommittingSpec : KafkaIntegrationTests
{
    public CommittingSpec(ITestOutputHelper output, KafkaFixture fixture) 
        : base(nameof(CommittingSpec), output, fixture)
    {
    }
    
    private static readonly string[] Numbers = Enumerable.Range(1, 200).Select(c => c.ToString()).ToArray();
    private const int Partition1 = 1;

    [Fact]
    public async Task CommittingMustEnsureUncommittedMessagesAreRedelivered()
    {
        var messages = Numbers.Take(100).ToArray();
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);
        var group2 = CreateGroup(2);

        await ProduceStrings(topic1, messages, ProducerSettings);

        var committedElements = new AtomicCounter(0);
        var consumerSettings = CreateConsumerSettings<string>(group1);

        var (control, probe1) = KafkaConsumer
            .CommittableSource(consumerSettings, Subscriptions.Topics(topic1))
            .SelectAsync(10, async e =>
            {
                await ((CommittableOffset)e.CommitableOffset).Commit();
                var offsetAsInt = (int)e.CommitableOffset.Offset.Offset.Value;
                var curValue = committedElements.Current;
                
                // CAS to ensure that we only commit the highest offset
                // N.B. if this racy, replace with messaging an actor
                committedElements.CompareAndSet(curValue, Math.Max(curValue, offsetAsInt));

                Sys.Log.Info("Committed: {0}", offsetAsInt);
                return e.Record.Message.Value;
            })
            .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
            .Run(Sys);

        await probe1.RequestAsync(25);
        var found = (await probe1.ExpectNextNAsync(25).ToListAsync());
        messages.Take(25).Should().BeEquivalentTo(found);
    }
}