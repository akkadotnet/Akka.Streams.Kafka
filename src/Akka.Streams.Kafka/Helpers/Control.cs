// -----------------------------------------------------------------------
//  <copyright file="Control.cs" company="Akka.NET Project">
//      Copyright (C) 2023 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers;

/// <summary>
/// Materialized value of the consumer `Source`.
/// </summary>
public interface IControl
{
    /// <summary>
    /// Stop producing messages from the `Source`. This does not stop the underlying kafka consumer
    /// and does not unsubscribe from any topics/partitions.
    ///
    /// Call <see cref="Shutdown"/> to close consumer.
    /// </summary>
    Task Stop();

    /// <summary>
    /// Shutdown the consumer `Source`. It will wait for outstanding offset
    /// commit requests to finish before shutting down.
    /// </summary>
    Task Shutdown();

    /// <summary>
    /// Shutdown status. The task will be completed when the stage has been shut down
    /// and the underlying <see cref="IConsumer{TKey,TValue}"/> has been closed. Shutdown can be triggered
    /// from downstream cancellation, errors, or <see cref="Shutdown"/>
    /// </summary>
    Task IsShutdown { get; }

    /// <summary>
    /// Stop producing messages from the `Source`, wait for stream completion
    /// and shut down the consumer `Source` so that all consumed messages
    /// reach the end of the stream.
    /// Failures in stream completion will be propagated, the source will be shut down anyway.
    /// </summary>
    Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion);
}

/// <summary>
/// Helper class for creating a <see cref="DrainingControl{T}"/> instances.
/// </summary>
public static class DrainingControl
{
    public static DrainingControl<T> Create<T>(IControl control, Task<T> streamCompletion) =>
        DrainingControl<T>.Create(control, streamCompletion);

    /// <summary>
    /// Stop producing messages from the `Source`, wait for stream completion
    /// and shut down the consumer `Source` so that all consumed messages
    /// reach the end of the stream.
    /// Failures in stream completion will be propagated, the source will be shut down anyway.
    /// </summary>
    internal static async Task<TResult> DrainAndShutdownDefaultAsync<TResult>(this IControl control,
        Task<TResult> streamCompletion)
    {
        TResult result;

        try
        {
            await control.Stop();
        }
        catch
        {
            // suppress stop errors
        }

        try
        {
            result = await streamCompletion;
        }
        catch (Exception)
        {
            try
            {
                await control.Shutdown();
            }
            catch
            {
                // suppress shutdown errors
            }

            // we want to throw the stream termination exception
            throw;
        }

        await control.Shutdown();

        return result;
    }
}

/// <summary>
/// Combine control and a stream completion signal materialized values into
/// one, so that the stream can be stopped in a controlled way without losing
/// commits.
/// </summary>
/// <typeparam name="T">Stream completion result type</typeparam>
public sealed class DrainingControl<T> : IControl
{
    /// <summary>
    /// Combine control and a stream completion signal materialized values into
    /// one, so that the stream can be stopped in a controlled way without losing
    /// commits.
    /// </summary>
    public static DrainingControl<T> Create(IControl control, Task<T> streamCompletion) =>
        new(control, streamCompletion);

    public IControl Control { get; }
    public Task<T> StreamCompletion { get; }

    private DrainingControl(IControl control, Task<T> streamCompletion)
    {
        Control = control;
        StreamCompletion = streamCompletion;
    }

    public Task Stop() => Control.Stop();

    public Task Shutdown() => Control.Shutdown();

    public Task IsShutdown
    {
        get { return Control.IsShutdown; }
    }

    public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) =>
        Control.DrainAndShutdown(streamCompletion);

    /// <summary>
    /// Stop producing messages from the `Source`, wait for stream completion
    /// and shut down the consumer `Source` so that all consumed messages
    /// reach the end of the stream.
    /// </summary>
    public Task<T> DrainAndShutdown() => Control.DrainAndShutdown(StreamCompletion);


    /// <summary>
    /// Combine control and a stream completion signal materialized values into
    /// one, so that the stream can be stopped in a controlled way without losing
    /// commits.
    /// </summary>
    public static DrainingControl<T> Create((IControl, Task<T>) tuple) =>
        new(tuple.Item1, tuple.Item2);

    /// <summary>
    /// Combine control and a stream completion signal materialized values into
    /// one, so that the stream can be stopped in a controlled way without losing
    /// commits.
    /// </summary>
    public static DrainingControl<NotUsed> Create((IControl, Task<Done>) tuple) =>
        new(tuple.Item1,
            tuple.Item2.ContinueWith(t => NotUsed.Instance, TaskContinuationOptions.NotOnFaulted));

    public static DrainingControl<NotUsed> Create(IControl control, Task streamCompletion) =>
        new(control,
            streamCompletion.ContinueWith(t => NotUsed.Instance, TaskContinuationOptions.NotOnFaulted));
}

/// <summary>
/// An implementation of Control to be used as an empty value, all methods return a failed task.
/// </summary>
public sealed class NoopControl : IControl
{
    private static Exception Exception
    {
        get { return new Exception("The correct Consumer.Control has not been assigned, yet."); }
    }

    public Task Stop() => Task.FromException(Exception);

    public Task Shutdown() =>
        Task.FromException(new Exception("The correct Consumer.Control has not been assigned, yet."));

    public Task IsShutdown
    {
        get { return Task.FromException(Exception); }
    }

    public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) =>
        this.DrainAndShutdownDefaultAsync(streamCompletion);
}