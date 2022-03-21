using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;

namespace Kafka.Partitioned.Consumer.Actors;

public class ConsumerWorkerActor<TKey, TValue>: ReceiveActor
{
    public static Props Props(ConsumerSettings<TKey, TValue> settings, ISubscription subscription)
        => Akka.Actor.Props.Create(() => new ConsumerWorkerActor<TKey, TValue>(settings, subscription));
    
    private readonly ILoggingAdapter _log;
    private readonly ConsumerSettings<TKey, TValue> _settings;
    private readonly ISubscription _subscription;
    private DrainingControl<NotUsed>? _control;
    private readonly Random _rnd = new Random();
    
    public ConsumerWorkerActor(ConsumerSettings<TKey, TValue> settings, ISubscription subscription)
    {
        _settings = settings;
        _subscription = subscription;
        _log = Context.GetLogger();

        Receive<CommittableMessage<TKey, TValue>>(msg =>
        {
            if(_rnd.Next(0, 100) == 0)
                throw new Exception("BOOM!");
            
            var record = msg.Record;
            _log.Info($"{record.Topic}[{record.Partition}][{record.Offset}]: {record.Message.Value}");
            Sender.Tell(msg.CommitableOffset);
        });
    }

    protected override void PostRestart(Exception reason)
    {
        base.PostRestart(reason);
        _log.Info("Worker restarted");
    }

    protected override void PreStart()
    {
        base.PreStart();
        
        var committerDefaults = CommitterSettings.Create(Context.System)
            .WithMaxInterval(TimeSpan.FromMilliseconds(500));
        
        _control = KafkaConsumer.CommittableSource(_settings, _subscription)
            .Ask<ICommittable>(Self, TimeSpan.FromSeconds(1), 1)
            .ToMaterialized(Committer.Sink(committerDefaults), DrainingControl<NotUsed>.Create)
            .Run(Context.System.Materializer());
        _log.Info("Worker started");
    }

    protected override void PostStop()
    {
        base.PostStop();
        _control?.Shutdown().Wait();
        _log.Info("Worker stopped");
    }
}