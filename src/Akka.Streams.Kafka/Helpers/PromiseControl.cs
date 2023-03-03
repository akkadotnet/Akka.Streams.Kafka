using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Extensions;
using Akka.Streams.Util;

#nullable enable
namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// Used in source logic classes to provide <see cref="IControl"/> implementation.
    /// </summary>
    /// <typeparam name="TSourceOut"></typeparam>
    internal abstract class PromiseControl<TSourceOut> : IControl
    {
        private readonly SourceShape<TSourceOut> _shape;
        private readonly Action<Outlet<TSourceOut>> _completeStageOutlet;
        private readonly Action<bool> _setStageKeepGoing;

        private readonly TaskCompletionSource<Done> _shutdownTaskSource = new TaskCompletionSource<Done>();
        private readonly TaskCompletionSource<Done> _stopTaskSource = new TaskCompletionSource<Done>();
        private readonly Action _stopCallback;
        private readonly Action<Exception?> _shutdownCallback;

        public PromiseControl(
            SourceShape<TSourceOut> shape, 
            Action<Outlet<TSourceOut>> completeStageOutlet, 
            Action<bool> setStageKeepGoing,  
            Func<Action, Action> asyncCallbackFactory,
            Func<Action<Exception?>, Action<Exception?>> asyncShutdownCallbackFactory)
        {
            _shape = shape;
            _completeStageOutlet = completeStageOutlet;
            _setStageKeepGoing = setStageKeepGoing;

            _stopCallback = asyncCallbackFactory(PerformStop);
            _shutdownCallback = asyncShutdownCallbackFactory(PerformShutdown);
        }

        /// <inheritdoc />
        public Task Stop()
        {
            _stopCallback();
            return _stopTaskSource.Task;
        }

        /// <inheritdoc />
        public Task Shutdown(Exception? ex)
        {
            _shutdownCallback(ex);
            return _shutdownTaskSource.Task;
        }

        /// <inheritdoc />
        public Task IsShutdown => _shutdownTaskSource.Task;

        /// <inheritdoc />
        public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) => this.DrainAndShutdownDefault(streamCompletion);

        /// <summary>
        /// Performs source logic stop
        /// </summary>
        public virtual void PerformStop()
        {
            _setStageKeepGoing(true);
            _completeStageOutlet(_shape.Outlet);
            OnStop();
        }

        /// <summary>
        /// Performs source logic shutdown
        /// </summary>
        public abstract void PerformShutdown(Exception? ex);

        /// <summary>
        /// Executed on source logic stop
        /// </summary>
        public void OnStop()
        {
            _stopTaskSource.TrySetResult(Done.Instance);
        }

        /// <summary>
        /// Executed on source logic shutdown
        /// </summary>
        public void OnShutdown()
        {
            _stopTaskSource.TrySetResult(Done.Instance);
            _shutdownTaskSource.TrySetResult(Done.Instance);
        }
    }
}