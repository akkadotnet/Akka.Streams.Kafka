using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Helpers;

namespace Akka.Streams.Kafka.Testkit.Dsl
{
    public static class ConsumerControlFactory
    {
        public static Source<TA, IControl> AttachControl<TA, TB>(Source<TA, TB> source)
            => source.ViaMaterialized(ControlFlow<TA>(), Keep.Right);

        public static Flow<TA, TA, IControl> ControlFlow<TA>()
            => Flow.Create<TA>()
                .ViaMaterialized(KillSwitches.Single<TA>(), Keep.Right)
                .MapMaterializedValue(Control);

        public static IControl Control(IKillSwitch killSwitch)
            => new FakeControl(killSwitch);
        
        public class FakeControl : IControl
        {
            private readonly IKillSwitch _killSwitch;
            private readonly TaskCompletionSource<Done> _shutdownPromise;

            public FakeControl(IKillSwitch killSwitch)
            {
                _killSwitch = killSwitch;
                _shutdownPromise = new TaskCompletionSource<Done>();
            }

            public Task Stop()
            {
                _killSwitch.Shutdown();
                _shutdownPromise.SetResult(Done.Instance);
                return _shutdownPromise.Task;
            }

            public Task Shutdown()
            {
                _killSwitch.Shutdown();
                _shutdownPromise.SetResult(Done.Instance);
                return _shutdownPromise.Task;
            }

            public Task IsShutdown => _shutdownPromise.Task;

            public Task<TResult> DrainAndShutdown<TResult>(Task<TResult> streamCompletion)
            {
                _killSwitch.Shutdown();
                _shutdownPromise.SetResult(Done.Instance);
                return Task.FromResult(default(TResult));
            }
        }
    }
}