using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Extensions;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
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
    /// Combine control and a stream completion signal materialized values into
    /// one, so that the stream can be stopped in a controlled way without losing
    /// commits.
    /// </summary>
    public class DrainingControl<T> : IControl
    {
        public IControl Control { get; }
        public Task<T> StreamCompletion { get; }

        /// <summary>
        /// DrainingControl
        /// </summary>
        /// <param name="control"></param>
        /// <param name="streamCompletion"></param>
        private DrainingControl(IControl control, Task<T> streamCompletion)
        {
            Control = control;
            StreamCompletion = streamCompletion;
        }

        /// <inheritdoc />
        public Task Stop() => Control.Stop();

        /// <inheritdoc />
        public Task Shutdown() => Control.Shutdown();

        /// <inheritdoc />
        public Task IsShutdown => Control.IsShutdown;

        /// <inheritdoc />
        public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) => Control.DrainAndShutdown(streamCompletion);

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
        public static DrainingControl<T> Create(IControl control, Task<T> streamCompletion) => new DrainingControl<T>(control, streamCompletion);
    }

    /// <summary>
    /// An implementation of Control to be used as an empty value, all methods return a failed task.
    /// </summary>
    public class NoopControl : IControl
    {
        private static Exception Exception => new Exception("The correct Consumer.Control has not been assigned, yet.");

        /// <inheritdoc />
        public Task Stop() => Task.FromException(Exception);

        /// <inheritdoc />
        public Task Shutdown() => Task.FromException(Exception);

        /// <inheritdoc />
        public Task IsShutdown => Task.FromException(Exception);

        /// <inheritdoc />
        public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) => this.DrainAndShutdownDefault(streamCompletion);
    }
}