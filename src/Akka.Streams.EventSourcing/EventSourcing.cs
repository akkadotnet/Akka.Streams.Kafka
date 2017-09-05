using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

// ReSharper disable InconsistentNaming

namespace Akka.Streams.EventSourcing
{
    public static class EventSoursing
    {
        public delegate S EventHandler<S, E>(S state, E @event);

        public delegate Emission<S, E, RES> RequestHandler<S, E, REQ, RES>(S state, REQ request);

        public abstract class Emission<S, E, RES> { }

        internal sealed class RespondClass<S, E, RES> : Emission<S, E, RES>
        {
            public RespondClass(RES response)
            {
                Response = response;
            }

            public RES Response { get; }
        }

        internal sealed class EmitClass<S, E, RES> : Emission<S, E, RES>
        {
            public EmitClass(IEnumerable<E> events, Func<S, RES> responseFactory)
            {
                Events = events;
                ResponseFactory = responseFactory;
            }

            public IEnumerable<E> Events { get; }

            public Func<S, RES> ResponseFactory { get; }
        }

        public static Emission<S, E, RES> Respond<S, E, RES>(RES response) 
            => new RespondClass<S, E, RES>(response);

        public static Emission<S, E, RES> Emit<S, E, RES>(IEnumerable<E> events, Func<S, RES> responseFactory)
            => new EmitClass<S, E, RES>(events, responseFactory);

        public static BidiFlow<REQ, IEmitted<E>, IDelivery<IDurable<E>>, RES, NotUsed> Create<S, E, REQ, RES>(
            string emitterId,
            S initial,
            RequestHandler<S, E, REQ, RES> requestHandler,
            EventHandler<S, E> eventHandler)
        {
            return BidiFlow.FromGraph(new EventSoursing<S, E, REQ, RES>(emitterId, initial, _ => requestHandler, _ => eventHandler));
        }

        public static BidiFlow<REQ, IEmitted<E>, IDelivery<IDurable<E>>, RES, NotUsed> Create<S, E, REQ, RES>(
            string emitterId,
            S initial,
            Func<S, RequestHandler<S, E, REQ, RES>> requestHandlerProvider,
            Func<S, EventHandler<S, E>> eventHandlerProvider)
        {
            return BidiFlow.FromGraph(new EventSoursing<S, E, REQ, RES>(emitterId, initial, requestHandlerProvider, eventHandlerProvider));
        }
    }

    internal class EventSoursing<S, E, REQ, RES> 
        : GraphStage<BidiShape<REQ, IEmitted<E>, IDelivery<IDurable<E>>, RES>>
    {
        public EventSoursing(
            string emitterId,
            S initial,
            Func<S, EventSoursing.RequestHandler<S, E, REQ, RES>> requestHandlerProvider,
            Func<S, EventSoursing.EventHandler<S, E>> eventHandlerProvider)
        {
            Shape = new BidiShape<REQ, IEmitted<E>, IDelivery<IDurable<E>>, RES>(Ci, Eo, Ei, Ro);
            EmitterId = emitterId;
            Initial = initial;
            RequestHandlerProvider = requestHandlerProvider;
            EventHandlerProvider = eventHandlerProvider;
        }

        public Inlet<REQ> Ci { get; } = new Inlet<REQ>("EventSourcing.requestIn");
        public Outlet<IEmitted<E>> Eo { get; } = new Outlet<IEmitted<E>>("EventSourcing.eventOut");
        public Inlet<IDelivery<IDurable<E>>> Ei { get; } = new Inlet<IDelivery<IDurable<E>>>("EventSourcing.eventIn");
        public Outlet<RES> Ro { get; } = new Outlet<RES>("EventSourcing.responseOut");

        public override BidiShape<REQ, IEmitted<E>, IDelivery<IDurable<E>>, RES> Shape { get; }

        public string EmitterId { get; }
        public S Initial { get; }
        public Func<S, EventSoursing.RequestHandler<S, E, REQ, RES>> RequestHandlerProvider { get; }
        public Func<S, EventSoursing.EventHandler<S, E>> EventHandlerProvider { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
        {
            return new EventSoursingStageLogic<S, E, REQ, RES>(this);
        }

        internal class Roundtrip<S, RES>
        {
            public Roundtrip(IImmutableSet<string> emissionGuids, Func<S, RES> responseFactory)
            {
                EmissionGuids = emissionGuids;
                ResponseFactory = responseFactory;
            }

            public IImmutableSet<string> EmissionGuids { get; }
            public Func<S, RES> ResponseFactory { get; }

            public Roundtrip<S, RES> Delivered(string emissionGuid)
            {
                return new Roundtrip<S, RES>(EmissionGuids.Remove(emissionGuid), ResponseFactory);
            }
        }

        internal class EventSoursingStageLogic<S, E, REQ, RES> : GraphStageLogic
        {
            private readonly EventSoursing<S, E, REQ, RES> _stage;
            private S _state;
            private bool requestUpstreamFinished = false;
            private Roundtrip<S, RES> roundtrip = null;
            private bool recovered = false;

            public EventSoursingStageLogic(EventSoursing<S, E, REQ, RES> stage) : base(stage.Shape)
            {
                _stage = stage;
                _state = _stage.Initial;

                SetHandler(_stage.Ci, onPush: () =>
                {
                    switch (_stage.RequestHandlerProvider(_state)(_state, Grab(_stage.Ci)))
                    {
                        case EventSoursing.RespondClass<S, E, RES> r:
                            Push(_stage.Ro, r.Response);
                            TryPullCi();
                            break;
                        case EventSoursing.EmitClass<S, E, RES> e:
                            var emitted = e.Events.Select(c => new Emitted<E>(c, _stage.EmitterId)).ToList();
                            roundtrip = new Roundtrip<S, RES>(emitted.Select(c => c.EmissionGuid).ToImmutableHashSet(), e.ResponseFactory);
                            EmitMultiple(_stage.Eo, emitted);
                            break;
                    }

                }, onUpstreamFinish: () =>
                {
                    requestUpstreamFinished = true;
                });

                SetHandler(_stage.Ei, () =>
                {
                    switch (Grab(_stage.Ei))
                    {
                        case Recovered r:
                            recovered = true;
                            TryPullCi();
                            break;
                        case Delivered<IDurable<E>> d:
                            _state = _stage.EventHandlerProvider(_state)(_state, d.EventType.EventType);

                            if (roundtrip != null)
                            {
                                var tempRoundTrip = roundtrip.Delivered(d.EventType.EmissionGuid);
                                if (tempRoundTrip.EmissionGuids.Count == 0)
                                {
                                    Push(_stage.Ro, tempRoundTrip.ResponseFactory(_state));
                                    if (requestUpstreamFinished)
                                        CompleteStage();
                                    else
                                        TryPullCi();
                                    roundtrip = null;
                                }
                                else
                                {
                                    roundtrip = tempRoundTrip;
                                }
                            }

                            break;
                    }
                    TryPullEi();
                });

                SetHandler(_stage.Eo, TryPullCi);

                SetHandler(_stage.Ro, TryPullCi);
            }

            public override void PreStart() => TryPullEi();

            private void TryPullEi()
            {
                if (!requestUpstreamFinished)
                {
                    Pull(_stage.Ei);
                }
            }

            private void TryPullCi()
            {
                if (IsAvailable(_stage.Eo)
                    && IsAvailable(_stage.Ro)
                    && !HasBeenPulled(_stage.Ci)
                    && roundtrip == null
                    && !requestUpstreamFinished)
                {
                    Pull(_stage.Ci);
                }
            }
        }
    }
}
