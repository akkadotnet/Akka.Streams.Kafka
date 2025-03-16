using System;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;

namespace Akka.Streams.Kafka.Tests.TestKit;

/// <summary>
/// Helper factory to create <see cref="IControl"/> instances
/// when testing without a Kafka broker.
/// </summary>
public static class ConsumerControlFactory
{
    public static Source<TIn, IControl> AttachControl<TIn, TMat>(Source<TIn, TMat> source)
    {
        return source.ViaMaterialized(ControlFlow<TIn>(), Keep.Right);
    }

    public static Flow<TIn, TIn, IControl> ControlFlow<TIn>() => Flow.Create<TIn>()
        .ViaMaterialized(KillSwitches.Single<TIn>(), Keep.Right)
        .MapMaterializedValue(Control);

    public static IControl Control(IKillSwitch killSwitch) => new FakeControl(killSwitch);
}

public class FakeControl : IControl
{
    public TaskCompletionSource<Done> ShutdownPromise { get; } = new();
    public IKillSwitch KillSwitch { get; }
    
    public FakeControl(IKillSwitch killSwitch)
    {
        KillSwitch = killSwitch;
    }

    public Task Stop()
    {
        KillSwitch.Shutdown();
        ShutdownPromise.TrySetResult(Done.Instance);
        return ShutdownPromise.Task;
    }

    public Task Shutdown(Exception? ex = null) => Stop();

    public Task IsShutdown => ShutdownPromise.Task;

    public async Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion)
    {
        await Stop();
        return await streamCompletion;
    }
}