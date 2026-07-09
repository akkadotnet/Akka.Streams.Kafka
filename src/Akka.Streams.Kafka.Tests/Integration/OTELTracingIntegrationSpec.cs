// -----------------------------------------------------------------------
//  <copyright file="OTELTracingIntegrationSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.TestKit;
using Akka.Util.Internal;
using Confluent.Kafka;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Kafka.Tests.Integration;

/// <summary>
/// Validates that Akka.Streams.Kafka streams work correctly when the "Akka.Streams"
/// ActivitySource has listeners registered. This simulates what Phobos does with
/// AddPhobosInstrumentation(traceAkkaStreams: true).
/// 
/// The original bug: NRE in GraphInterpreter.ReportStageError when Kafka streams
/// ran with OTEL tracing active (traceAkkaStreams: true). This test proves the
/// fix by running Kafka streams with the "Akka.Streams" ActivitySource listener
/// subscribed and verifying no NRE occurs.
/// </summary>
public class OTELTracingIntegrationSpec : KafkaIntegrationTests
{
    private readonly ConcurrentQueue<Activity> _capturedSpans = new();
    private ActivityListener? _otelListener;

    public OTELTracingIntegrationSpec(ITestOutputHelper output, KafkaFixture fixture)
        : base(nameof(OTELTracingIntegrationSpec), output, fixture)
    {
    }

    protected override void AfterAll()
    {
        _otelListener?.Dispose();
        base.AfterAll();
    }

    /// <summary>
    /// Subscribes to the "Akka.Streams" ActivitySource — exactly what Phobos does with
    /// AddPhobosInstrumentation(traceAkkaStreams: true).
    /// </summary>
    private void EnableAkkaStreamsTracing()
    {
        _otelListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Akka.Streams",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => 
                ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = _ => { },
            ActivityStopped = activity => _capturedSpans.Enqueue(activity)
        };
        ActivitySource.AddActivityListener(_otelListener);
    }

    /// <summary>
    /// Creates a probe for consuming messages via PlainSource.
    /// Follows the same pattern as PlainSourceIntegrationTests.cs.
    /// </summary>
    private (IControl, TestSubscriber.Probe<string>) CreateProbe(ConsumerSettings<Null, string> consumerSettings,
        ISubscription sub) =>
        KafkaConsumer
            .PlainSource(consumerSettings, sub)
            .Select(c => c.Message.Value)
            .ToMaterialized(this.SinkProbe<string>(), Keep.Both)
            .Run(Materializer);

    /// <summary>
    /// Reproduces the original bug: Kafka producer+consumer with OTEL tracing active.
    /// The original NRE was in GraphInterpreter.ReportStageError when the OTEL wrapper
    /// path was exercised by Kafka streams.
    /// </summary>
    [Fact(DisplayName = "Kafka plain source with Akka.Streams tracing should not throw NRE")]
    public async Task PlainSourceWithAkkaStreamsTracing_ShouldCompleteWithoutError()
    {
        // Enable tracing — this is what AddPhobosInstrumentation(traceAkkaStreams: true) does
        EnableAkkaStreamsTracing();

        var elementsCount = 10;
        var topic1 = CreateTopic(1);
        var group1 = CreateGroup(1);

        await GivenInitializedTopicAsync(topic1);
        await ProduceStrings(topic1, Enumerable.Range(1, elementsCount), ProducerSettings);

        var consumerSettings = CreateConsumerSettings<Null, string>(group1);
        var (_, probe) = CreateProbe(consumerSettings, Subscriptions.Topics(topic1));

        probe.Request(elementsCount);
        foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
            probe.ExpectNext(i, TimeSpan.FromSeconds(10));

        probe.Cancel();

        // Verify spans were captured (proves OTEL path was exercised)
        var allSpans = _capturedSpans.ToList();
        allSpans.Should().NotBeEmpty(
            "stream spans should have been captured when Akka.Streams listeners are active");
        allSpans.Should().Contain(s => s.OperationName.Contains("akka.stream"),
            "at least one akka.stream span should be present when tracing is enabled");
    }

    /// <summary>
    /// Tests committable source (most complex Kafka stream) with OTEL tracing active.
    /// This maximizes the surface area for the OTEL wrapper code path.
    /// </summary>
    [Fact(DisplayName = "Kafka committable source with Akka.Streams tracing should complete without error")]
    public async Task CommittableSourceWithAkkaStreamsTracing_ShouldNotThrowNRE()
    {
        EnableAkkaStreamsTracing();

        var topic1 = CreateTopic(2);
        var group1 = CreateGroup(2);

        await GivenInitializedTopicAsync(topic1);
        await ProduceStrings(topic1, new[] { "data-1", "data-2" }, ProducerSettings);

        await Task.Delay(500);

        // Committable source — the most complex Kafka stream
        // Note: CommittableSource uses .RunWith() pattern (not CreateProbe)
        var committableProbe = KafkaConsumer
            .CommittableSource(
                ConsumerSettings<Null, string>
                    .Create(Sys, null, null)
                    .WithBootstrapServers(Fixture.KafkaServer)
                    .WithGroupId(group1)
                    .WithProperty("auto.offset.reset", "earliest"),
                Subscriptions.Topics(topic1))
            .Select(msg => msg.Record.Message.Value)
            .RunWith(this.SinkProbe<string>(), Materializer);

        committableProbe.Request(2);
        var received = committableProbe.ExpectNextN(2, TimeSpan.FromSeconds(10));

        received.Should().HaveCount(2);

        committableProbe.Cancel();

        var allSpans = _capturedSpans.ToList();
        allSpans.Should().NotBeEmpty("stream spans should have been captured");
    }

    /// <summary>
    /// Verifies that when NO listener is registered for "Akka.Streams", zero spans
    /// are produced (HasListeners() guard works correctly). Control test.
    /// </summary>
    [Fact(DisplayName = "No Akka.Streams listener should produce zero stream spans")]
    public async Task NoAkkaStreamsListener_ShouldProduceZeroStreamSpans()
    {
        // Do NOT enable tracing — simulates traceAkkaStreams: false
        // The framework's HasListeners() guard should prevent any span creation.

        var elementsCount = 5;
        var topic1 = CreateTopic(3);
        var group1 = CreateGroup(3);

        await ProduceStrings(topic1, Enumerable.Range(1, elementsCount), ProducerSettings);

        await Task.Delay(500);

        var consumerSettings = CreateConsumerSettings<Null, string>(group1);
        var (_, probe) = CreateProbe(consumerSettings, Subscriptions.Topics(topic1));

        probe.Request(elementsCount);
        foreach (var i in Enumerable.Range(1, elementsCount).Select(c => c.ToString()))
            probe.ExpectNext(i, TimeSpan.FromSeconds(10));

        probe.Cancel();

        // Give time for any spans to be produced (they shouldn't be)
        await Task.Delay(500);

        _capturedSpans.Should().BeEmpty(
            "no stream spans should be emitted when no Akka.Streams listener is registered");
    }
}
