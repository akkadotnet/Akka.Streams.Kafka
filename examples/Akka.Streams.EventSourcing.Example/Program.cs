using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Streams.Dsl;

namespace Akka.Streams.EventSourcing.Example
{
    public abstract class Request { }

    // Query
    public class GetState : Request { } // Query

    // Command
    public class Increment : Request
    {
        public Increment(int delta)
        {
            Delta = delta;
        }

        public int Delta { get; }
    }

    // Event
    public class Incremented
    {
        public Incremented(int delta)
        {
            Delta = delta;
        }

        public int Delta { get; }
    }

    public class Response
    {
        public Response(int state)
        {
            State = state;
        }

        public int State { get; }
    }

    internal static class ArrayExtensions
    {
        public static Dictionary<T, int> ZipWithIndex<T>(this IEnumerable<T> collection)
        {
            var i = 0;
            var dict = new Dictionary<T, int>();
            foreach (var item in collection)
            {
                dict.Add(item, i);
                i++;
            }
            return dict;
        }
    }

    class Program
    {
        public static IEnumerable<IDurable<T>> Durables<T>(IEnumerable<IEmitted<T>> emitted, int offset = 0)
        {
            return emitted
                .ZipWithIndex()
                .Select(tuple => tuple.Key.Durable(tuple.Value + offset));
        }

        public static Flow<IEmitted<T>, IDelivery<IDurable<T>>, NotUsed> TestEventLog<T>(IEnumerable<IEmitted<T>> emitted = null)
        {
            emitted = emitted ?? new List<IEmitted<T>>();
            return Flow.Create<IEmitted<T>>()
                .ZipWithIndex()
                .Select(tuple => tuple.Item1.Durable(tuple.Item2))
                .Select(c => new Delivered<T>(c.EventType))
                .Prepend(Source.From(Durables(emitted).Select(c => new Delivered<T>(c.EventType))))
                .Select(c => c as IDelivery<IDurable<T>>);
        }

        static void Main(string[] args)
        {
            EventSoursing.RequestHandler<int, Incremented, Request, Response> requestHandler = (state, request) =>
            {
                switch (request)
                {
                    case GetState _:
                        return EventSoursing.Respond<int, Incremented, Response>(new Response(state));
                    case Increment i:
                        return EventSoursing.Emit<int, Incremented, Response>(ImmutableList.Create(new Incremented(i.Delta)), s => new Response(s));
                    default:
                        return null;
                }
            };

            EventSoursing.EventHandler<int, Incremented> eventHandler = (state, evt) 
                => state + evt.Delta;

            var system = ActorSystem.Create("test");
            var materializer = system.Materializer();

            Flow<Request, Response, NotUsed> processor = EventSoursing
                .Create("myEmitterId", 0, requestHandler, eventHandler)
                .Join(TestEventLog<Incremented>());

            // TODO don't work without cast to base type
            var commands = ImmutableList.Create(1, -4, 7).Select(c => new Increment(c) as Request);
            var expected = ImmutableList.Create(1, -3, 4).Select(c => new Response(c));

            var val = Source.From(commands).Via(processor).RunWith(Sink.Seq<Response>(), materializer);
            var b = val.Result;
            Console.WriteLine("Hello World!");
        }
    }
}
