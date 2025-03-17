using System;
using Akka.Streams.Stage;

namespace Akka.Streams.Kafka.Benchmark.Infrastructure;

public class LogEveryNthElement<T> : GraphStage<FlowShape<T, T>>
{
    private readonly int _n;
    private readonly Func<int,string> _logMessageFn;

    public LogEveryNthElement(int n, Func<int,string> logMessageFn)
    {
        if (n <= 0)
            throw new ArgumentException("N must be a positive integer", nameof(n));
        
        _n = n;
        _logMessageFn = logMessageFn;
        Shape = new FlowShape<T, T>(Inlet, Outlet);
    }

    public Inlet<T> Inlet { get; } = new("LogEveryN.in");
    public Outlet<T> Outlet { get; } = new("LogEveryN.out");

    public override FlowShape<T, T> Shape { get; }

    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) 
        => new Logic(this);

    private sealed class Logic : GraphStageLogic
    {
        private int _countdownToNextLog;

        public Logic(LogEveryNthElement<T> stage) : base(stage.Shape)
        {
            ResetCountdown();

            SetHandler(stage.Inlet, onPush: () =>
            {
                var element = Grab(stage.Inlet);
                _countdownToNextLog++;
                
                if (_countdownToNextLog % stage._n == 0)
                {
                    Log.Info(stage._logMessageFn(_countdownToNextLog));
                }

                Push(stage.Outlet, element);
            });

            SetHandler(stage.Outlet, onPull: () =>
            {
                Pull(stage.Inlet);
            });
        }

        private void ResetCountdown() => _countdownToNextLog =  0;
    }
}