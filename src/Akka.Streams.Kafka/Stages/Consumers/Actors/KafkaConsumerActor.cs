using Akka.Actor;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    internal class KafkaConsumerActor<K, V> : ActorBase
    {
        private readonly IActorRef _owner;
        private readonly ConsumerSettings<K, V> _settings;
        private readonly IConsumerEventHandler _consumerEventHandler;

        public KafkaConsumerActor(IActorRef owner, ConsumerSettings<K, V> settings, IConsumerEventHandler consumerEventHandler)
        {
            _owner = owner;
            _settings = settings;
            _consumerEventHandler = consumerEventHandler;
        }

        protected override bool Receive(object message) => throw new System.NotImplementedException();
    }
}