using System;
using Akka;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Akka.Util.Internal;

namespace Kafka.Partitioned.Consumer.Actors
{
    public class KafkaConsumerSupervisor<TKey, TValue>: ReceiveActor
    {
        private static readonly AtomicCounter WorkerId = new AtomicCounter();
        public static Props Props(ConsumerSettings<TKey, TValue> settings, ISubscription subscription, int partitions)
            => Akka.Actor.Props.Create(() => new KafkaConsumerSupervisor<TKey, TValue>(settings, subscription, partitions));
    
        private readonly ConsumerSettings<TKey, TValue> _settings;
        private readonly ISubscription _subscription;
        private readonly int _partitions;

        public KafkaConsumerSupervisor(ConsumerSettings<TKey, TValue> settings, ISubscription subscription, int partitions)
        {
            _settings = settings;
            _subscription = subscription;
            _partitions = partitions;
        }

        protected override SupervisorStrategy SupervisorStrategy()
            => new OneForOneStrategy(ex =>
            {
                return ex switch
                {
                    Exception { Message: "BOOM!" } => Directive.Restart,
                    _ => Directive.Escalate
                };
            });

        protected override void PreStart()
        {
            base.PreStart();
            for (var i = 0; i < _partitions; i++)
            {
                var id = WorkerId.IncrementAndGet();
                Context.ActorOf(ConsumerWorkerActor<TKey, TValue>.Props(_settings, _subscription), $"worker-{id}");
            }
        }
    }    
}
