using System;
using System.Threading.Tasks;

namespace Akka.Streams.Kafka.Helpers
{
    internal static class PromiseControl
    {
        public interface IControlOperation;

        public sealed class ControlStop : IControlOperation
        {
            public static readonly ControlStop Instance = new();
            private ControlStop(){}
        }

        public sealed class ControlShutdown : IControlOperation
        {
            public static readonly ControlShutdown Instance = new();
            private ControlShutdown(){}
        }
    }
    
    /// <summary>
    /// Used in source logic classes to provide <see cref="IControl"/> implementation.
    /// </summary>
    /// <typeparam name="TSourceOut"></typeparam>
    internal class PromiseControl<TSourceOut> : IControl
    {
        private readonly SourceShape<TSourceOut> _shape;
        private readonly Action<Outlet<TSourceOut>> _completeStageOutlet;
        private readonly Action<bool> _setStageKeepGoing;

        private readonly TaskCompletionSource<Done> _shutdownTaskSource = new();
        private readonly TaskCompletionSource<Done> _stopTaskSource = new();
        private readonly Action<PromiseControl.IControlOperation> _controlCallback;
        private readonly Action _performShutdown;

        public PromiseControl(
            SourceShape<TSourceOut> shape, 
            Action<Outlet<TSourceOut>> completeStageOutlet, 
            Action<bool> setStageKeepGoing,  
            Func<Action<PromiseControl.IControlOperation>, Action<PromiseControl.IControlOperation>> asyncShutdownCallbackFactory,
            Action performShutdown)
        {
            _shape = shape;
            _completeStageOutlet = completeStageOutlet;
            _setStageKeepGoing = setStageKeepGoing;
            _performShutdown = performShutdown;
            _controlCallback = asyncShutdownCallbackFactory(c =>
            {
                switch (c)
                {
                    case PromiseControl.ControlStop:
                        PerformStop();
                        break;
                    case PromiseControl.ControlShutdown:
                        PerformShutdown();
                        break;
                }
            });
        }
        
        public Task Stop()
        {
            if(!_stopTaskSource.Task.IsCompleted)
                _controlCallback(PromiseControl.ControlStop.Instance);
            return _stopTaskSource.Task;
        }
        
        public Task Shutdown()
        {
            if(!_shutdownTaskSource.Task.IsCompleted)
                _controlCallback(PromiseControl.ControlShutdown.Instance);
            return _shutdownTaskSource.Task;
        }
        
        public Task IsShutdown => _shutdownTaskSource.Task;
        
        public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion) => this.DrainAndShutdownDefaultAsync(streamCompletion);

        /// <summary>
        /// Performs source logic stop
        /// </summary>
        protected virtual void PerformStop()
        {
            _setStageKeepGoing(true);
            _completeStageOutlet(_shape.Outlet);
            OnStop();
        }

        /// <summary>
        /// Performs source logic shutdown
        /// </summary>
        protected virtual void PerformShutdown()
        {
            _performShutdown();
        }

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