using System;

// ReSharper disable InconsistentNaming

namespace Akka.Streams.EventSourcing
{
    public interface IDelivery<out A> { }

    public sealed class Recovered : IDelivery<NotUsed>
    {
        public static Recovered Instance { get; } = new Recovered();
    }

    public sealed class Delivered<A> : IDelivery<A>
    {
        public Delivered(A eventType)
        {
            EventType = eventType;
        }

        public A EventType { get; }
    }

    public interface IEmitted<out A>
    {
        A EventType { get; }
        string EmitterId { get; }
        string EmissionGuid { get; }
        IDurable<A> Durable(long sequenceNr);
    }

    public sealed class Emitted<A> : IEmitted<A>
    {
        public Emitted(A eventType, string emitterId, string emissionGuid = null)
        {
            EventType = eventType;
            EmitterId = emitterId;
            EmissionGuid = emissionGuid ?? Guid.NewGuid().ToString();
        }

        public A EventType { get; }

        public string EmitterId { get; }

        public string EmissionGuid { get; }

        public IDurable<A> Durable(long sequenceNr) 
            => new Durable<A>(EventType, EmitterId, EmissionGuid, sequenceNr);
    }

    public interface IDurable<out A>
    {
        A EventType { get; }
        string EmitterId { get; }
        string EmissionGuid { get; }
        long SequenceNr { get; }
    }

    public sealed class Durable<A> : IDurable<A>
    {
        public Durable(A eventType, string emitterId, string emissionGuid, long sequenceNr)
        {
            EventType = eventType;
            EmitterId = emitterId;
            EmissionGuid = emissionGuid;
            SequenceNr = sequenceNr;
        }

        public A EventType { get; }

        public string EmitterId { get; }

        public string EmissionGuid { get; }

        public long SequenceNr { get; }
    }
}
