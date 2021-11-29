using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams.Implementation;
using Akka.Util.Internal;

namespace Akka.Streams.Kafka.Testkit
{
    public static class Extensions
    {
        public static async Task WithTimeout(this Task task, TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource())
            {
                var timeoutTask = Task.Delay(timeout, cts.Token);
                var completed = await Task.WhenAny(task, timeoutTask);
                if (completed == timeoutTask)
                    throw new OperationCanceledException("Operation timed out");
                else
                    cts.Cancel();
            }
        }
        
        public static List<List<T>> Grouped<T>(this IEnumerable<T> messages, int size)
        {
            var groups = new List<List<T>>();
            var list = new List<T>();
            var index = 0;
            foreach (var message in messages)
            {
                list.Add(message);
                if(index != 0 && index % size == 0)
                {
                    groups.Add(list);
                    list = new List<T>();
                }

                index++;
            }
            if(list.Count > 0)
                groups.Add(list);
            return groups;
        }

        public static void AssertAllStagesStopped(this Akka.TestKit.Xunit2.TestKit spec, Action block, IMaterializer materializer)
        {
            AssertAllStagesStopped(spec, () =>
            {
                block();
                return NotUsed.Instance;
            }, materializer);
        }

        public static T AssertAllStagesStopped<T>(this Akka.TestKit.Xunit2.TestKit spec, Func<T> block, IMaterializer materializer)
        {
            if (!(materializer is ActorMaterializerImpl impl))
                return block();

            var probe = spec.CreateTestProbe(impl.System);
            probe.Send(impl.Supervisor, StreamSupervisor.StopChildren.Instance);
            probe.ExpectMsg<StreamSupervisor.StoppedChildren>();
            var result = block();

            probe.Within(TimeSpan.FromSeconds(5), () =>
            {
                IImmutableSet<IActorRef> children = ImmutableHashSet<IActorRef>.Empty;
                try
                {
                    probe.AwaitAssert(() =>
                    {
                        impl.Supervisor.Tell(StreamSupervisor.GetChildren.Instance, probe.Ref);
                        children = probe.ExpectMsg<StreamSupervisor.Children>().Refs;
                        if (children.Count != 0)
                            throw new Exception($"expected no StreamSupervisor children, but got {children.Aggregate("", (s, @ref) => s + @ref + ", ")}");
                    });
                }
                catch 
                {
                    children.ForEach(c=>c.Tell(StreamSupervisor.PrintDebugDump.Instance));
                    throw;
                }
            });

            return result;
        }
    }
}