# Akka Streams Kafka

Akka Streams Kafka is an Akka Streams connector for Apache Kafka. This is a port of the Alpakka Kafka project (https://github.com/akka/alpakka-kafka).

Library is based on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) driver, and implements Sources, Sinks and Flows to handle Kafka message streams.
All stages are build with Akka.Streams advantages in mind:
- There is no constant Kafka topics pooling: messages are consumed on demand, and with back-pressure support
- There is no internal buffering: consumed messages are passed to the downstream in realtime, and producer stages publish messages to Kafka as soon as get them from upstream
- Each stage can make use of it's own `IConsumer` or `IProducer` instance, or can share them (can be used for optimization)
- All Kafka failures can be handled with usual stream error handling strategies

## Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

### Settings

When creating a producer stream you need to pass in `ProducerSettings` that defines things like:

- bootstrap servers of the Kafka cluster
- serializers for the keys and values
- tuning parameters

```C#
var producerSettings = ProducerSettings<Null, string>.Create(system, null, null)
    .WithBootstrapServers("localhost:9092");

// OR you can use Config instance
var config = system.Settings.Config.GetConfig("akka.kafka.producer");
var producerSettings = ProducerSettings<Null, string>.Create(config, null, null)
    .WithBootstrapServers("localhost:9092");
```

> __NOTE:__ 
>
> Specifying `null` as a key/value serializer uses default serializer for key/value type. Built-in serializers are available in `Confluent.Kafka.Serializers` class.

By default when creating `ProducerSettings` with the ActorSystem parameter it uses the config section `akka.kafka.producer`.

#### Defining Kafka Properties Directly Inside HOCON
You can embed Kafka properties directly inside the HOCON configuration by declaring them inside the `kafka-clients` section:
```HOCON
akka.kafka.producer.kafka-clients {
    bootstrap.servers = "localhost:9092"
    client.id = client-1
    enable.idempotence = true
}
```

#### Importing Confluent.Kafka.ProducerConfig Directly Into ProducerSettings

Working with `ProducerConfig` is a lot more convenient than having to use the `ProducerSettings.WithProperty` method because you don't have to memorize all of Kafka property names.
You can import `ProducerConfig` directly into `Akka.Streams.Kafka.ProducerSettings` by using the convenience method `ProducerSettings.WithProducerConfig` to import all of the defined Kafka properties.

```c#
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "client1", 
    EnableIdempotence = true
};
var settings = ProducerSettings<string, string>.Create(system, null, null)
    .WithProducerConfig(config);
```

### Default Producer HOCON Settings

```HOCON
akka.kafka.producer {
  # Tuning parameter of how many sends that can run in parallel.
  parallelism = 100

  # How long to wait for `Producer.Flush`
   flush-timeout = 10s
   
   # Fully qualified config path which holds the dispatcher configuration
   # to be used by the producer stages. Some blocking may occur.
   # When this value is empty, the dispatcher configured for the stream
   # will be used.
   use-dispatcher = "akka.kafka.default-dispatcher"
   
   # Properties defined by Confluent.Kafka.ProducerConfig
   # can be defined in this configuration section.
   kafka-clients {
   }
}
```

### PlainSink

`KafkaProducer.PlainSink` is the easiest way to publish messages. The sink consumes `ProducerRecord` elements which contains a topic name to which the record is being sent, an optional partition number, and an optional key, and a value.

```C#
Source
    .From(Enumerable.Range(1, 100))
    .Select(c => c.ToString())
    .Select(elem => new ProducerRecord<TKey, string>(topic, elem.ToString()))
    .RunWith(KafkaProducer.PlainSink(producerSettings), materializer);
```
The materialized value of the sink is a `Task` which is completed with result when the stream completes or with exception if an error occurs.

### Producer as a Flow

Sometimes there is a need for publishing messages in the middle of the stream processing, not as the last step, and then you can use `KafkaProducer.FlexiFlow`.

```C#
Source
    .Cycle(() => Enumerable.Range(1, 100).GetEnumerator())
    .Select(c => c.ToString())
    .Select(elem => ProducerMessage.Single(new ProducerRecord<Null, string>("akka100", elem)))
    .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(producerSettings))
    .Select(result =>
    {
        var response = result as Result<Null, string, NotUsed>;
        Console.WriteLine($"Producer: {response.Metadata.Topic}/{response.Metadata.Partition} {response.Metadata.Offset}: {response.Metadata.Value}");
        return result;
    })
    .RunWith(Sink.Ignore<IResults<Null, string, NotUsed>>(), materializer);
```

This flow accepts implementations of `Akka.Streams.Kafka.Messages.IEnvelope` and return `Akka.Streams.Kafka.Messages.IResults` elements. 
`IEnvelope` elements contain an extra field to pass through data, the so called `passThrough`. 
Its value is passed through the flow and becomes available in the `ProducerMessage.Results`'s `PassThrough`. 
It can for example hold a `Akka.Streams.Kafka.Messages.CommittableOffset` or `Akka.Streams.Kafka.Messages.CommittableOffsetBatch` (from a `KafkaConsumer.CommittableSource`) 
that can be committed after publishing to Kafka:

```c#
DrainingControl<NotUsed> control = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics(topic1))
    .Select(message =>
    {
        return ProducerMessage.Single(
            new ProducerRecord<Null, string>(topic1, message.Record.Key, message.Record.Value),
            message.CommitableOffset as ICommittable // the passThrough
        );
    })
    .Via(KafkaProducer.FlexiFlow<Null, string, ICommittable>(ProducerSettings))
    .Select(m => m.PassThrough)
    .ToMaterialized(Committer.Sink(CommitterSettings), Keep.Both)
    .MapMaterializedValue(DrainingControl<NotUsed>.Create)
    .Run(Materializer);
```

#### Produce a single message to Kafka

To create one message to a Kafka topic, use the `Akka.Streams.Kafka.Messages.Message` implementation of `IEnvelop`.

It can be created with `ProducerMessage.Single` helper:

```c#
IEnvelope<TKey, TValue, TPassThrough> single = ProducerMessage.Single(
    new ProducerRecord<Null, string>("topic", key, value),
    passThrough)
```

The flow with `ProducerMessage.Message` will continue as `ProducerMessage.Result` elements containing:
- the original input Message
- the record metadata, and
- access to `PassThrough` within the message

#### Let one stream element produce multiple messages to Kafka

The `ProducerMessage.MultiMessage` implementation of `IEnvelope` contains a list of `ProducerRecord`s to produce multiple messages to Kafka topics:

```c#
var multiMessage = ProducerMessage.Multi(new[]
{
    new ProducerRecord<string, string>(topic2, record.Key, record.Value),
    new ProducerRecord<string, string>(topic3, record.Key, record.Value)
}.ToImmutableSet(), passThrough);
```

The flow with `ProducerMessage.MultiMessage` will continue as `ProducerMessage.MultiResult` elements containing:
- a list of `MultiResultPart` with
    - the original input message
    - the record metadata
- the `PassThrough` data

#### Let a stream element pass through, without producing a message to Kafka

The `ProducerMessage.PassThroughMessage` allows to let an element pass through a Kafka flow without producing a new message to a Kafka topic. 
This is primarily useful with Kafka commit offsets and transactions, so that these can be committed without producing new messages.

```c#
var passThroughMessage = ProducerMessage.PassThrough<string, string>(passThrough);
```

For flows the `ProducerMessage.PassThroughMessage`s continue as `ProducerMessage.PassThroughResult` elements containing the `passThrough` data.

### Sharing the `IProducer` instance

Sometimes you may need to make use of already existing `Confluent.Kafka.IProducer` instance (i.e. for integration with existing code). 

Each of the `KafkaProducer` methods has an overload accepting `IProducer` as a parameter. 

## Consumer

A consumer subscribes to Kafka topics and passes the messages into an Akka Stream.

### Settings 
When creating a consumer stream you need to pass in `ConsumerSettings` that define things like:

- de-serializers for the keys and values
- bootstrap servers of the Kafka cluster
- group id for the consumer, note that offsets are always committed for a given consumer group
- tuning parameters

```C#
var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, Serializers.Int32)
    .WithBootstrapServers("localhost:9092")
    .WithGroupId("group1"); // Specifying GroupId is required before starting stream - otherwise you will get an exception at runtime

// OR you can use Config instance
var config = system.Settings.Config.GetConfig("akka.kafka.consumer");
var consumerSettings = ConsumerSettings<Null, string>.Create(config, null, Serializers.Int32)
    .WithBootstrapServers("localhost:9092")
    .WithGroupId("group1"); // Specifying GroupId is required before starting stream - otherwise you will get an exception at runtime
```

As with producer settings, they are loaded from `akka.kafka.consumer` of configuration file (or custom `Config` instance provided).

#### Defining Kafka Properties Directly Inside HOCON
You can embed Kafka properties directly inside the HOCON configuration by declaring them inside the `kafka-clients` section:
```HOCON
akka.kafka.consumer.kafka-clients {
    bootstrap.servers = "localhost:9092"
    client.id = client-1
    group.id = group-1
}
```

#### Importing Confluent.Kafka.ConsumerConfig Directly Into ConsumerSettings

Working with `ConsumerConfig` is a lot more convenient than having to use the `ConsumerSettings.WithProperty` method because you don't have to memorize all of Kafka property names.
You can import `ConsumerConfig` directly into `Akka.Streams.Kafka.ConsumerSettings` by using the convenience method `ConsumerSettings.WithConsumerConfig` to import all of the defined Kafka properties.

```c#
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Latest,
    EnableAutoCommit = true,
    GroupId = "group1",
    ClientId = "client1"
};

var settings = ConsumerSettings<string, string>.Create(actorSystem, null, null)
    .WithConsumerConfig(config);
```

### Default Consumer HOCON Settings

See [`reference.conf`](https://github.com/akkadotnet/Akka.Streams.Kafka/blob/dev/src/Akka.Streams.Kafka/reference.conf) for the latest on settings.

### PlainSource

To consume messages without committing them you can use `KafkaConsumer.PlainSource` method. This will emit consumed messages of `ConsumeResult` type.

_Note_: When using this source, you need to store consumer offset externally - it does not have support of committing offsets to Kafka. 

```C#
var subscription = Subscriptions.Assignment(new TopicPartition("akka", 0));

KafkaConsumer.PlainSource(consumerSettings, subscription)
    .RunForeach(result =>
    {
        Console.WriteLine($"Consumer: {result.Topic}/{result.Partition} {result.Offset}: {result.Value}");
    }, materializer);
```

### PlainExternalSource

Special source that can use an external `KafkaConsumerActor`. This is useful when you have
a lot of manually assigned topic-partitions and want to keep only one kafka consumer.


You can create reusable consumer actor reference like this:

```c#
var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(consumerSettings));
```

### CommittableSource

The `KafkaConsumer.CommittableSource` makes it possible to commit offset positions to Kafka.

If you need to store offsets in anything other than Kafka, `PlainSource` should be used instead of this API.

This is useful when "at-least once delivery" is desired, as each message will likely be delivered one time but in failure cases could be duplicated.

The recommended way to handle commits is to use the built-in `Committer` facilities, which provide proper batching and error handling:

```C#
// Recommended pattern - using Committer.Sink for safe batched commits
var control = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics("topic1"))
    .ToMaterialized(Committer.Sink(CommitterSettings.Create(system)), Keep.Both)
    .MapMaterializedValue(DrainingControl<Done>.Create)
    .Run(materializer);

// For more complex scenarios, you can process messages before committing
var control = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics("topic1"))
    .SelectAsync(parallelism: 10, async message =>
    {
        await ProcessMessage(message.Record); // Your message processing logic
        return message.CommitableOffset;
    })
    .ToMaterialized(Committer.Sink(CommitterSettings.Create(system)), Keep.Both)
    .MapMaterializedValue(DrainingControl<Done>.Create)
    .Run(materializer);

// When you need to produce messages to Kafka before committing
DrainingControl<Done> control = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics("topic1"))
    .Select(msg => 
        ProducerMessage.Single(
            new ProducerRecord<Null, string>("topic2", msg.Record.Message.Key, msg.Record.Message.Value),
            msg.CommitableOffset))
    .Via(KafkaProducer.FlexiFlow<Null, string, ICommittableOffset>(producerSettings))
    .Select(ICommittable (result) => result.PassThrough)
    .ToMaterialized(Committer.Sink(CommitterSettings.Create(system)), Keep.Both)
    .MapMaterializedValue<DrainingControl<Done>>(tuple => DrainingControl.Create(tuple.Item1, tuple.Item2))
    .Run(system);
```

The `Committer` facilities handle batching automatically based on your `CommitterSettings`. You can configure batch size, parallelism, and other parameters:

```C#
var committerSettings = CommitterSettings.Create(system)
    .WithMaxBatch(100) // Maximum number of offsets in one commit
    .WithParallelism(5) // Number of commits that can be in progress at the same time
    .WithMaxInterval(TimeSpan.FromSeconds(3)); // Maximum interval between commits
```

> **WARNING**: Avoid calling `CommittableOffset.Commit()` or `CommittableOffsetBatch.Commit()` directly. Always use the `Committer` facilities to ensure proper batching and error handling. Direct commits can lead to reduced performance and potential data loss in failure scenarios.

When using manual partition assignment or when you need more control over the commit process:

```C#
var subscription = Subscriptions.Assignment(new TopicPartition("topic1", 0));

DrainingControl<Done> control = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Assignment(topicPartition1))
    .Select(ICommittable (c) => c.CommitableOffset)
    .ToMaterialized(
        Committer.Sink(CommitterSettings.Create(system)
            .WithMaxBatch(100)
            .WithParallelism(5)), 
        Keep.Both)
    .MapMaterializedValue<DrainingControl<Done>>(tuple => DrainingControl.Create(tuple.Item1, tuple.Item2))
    .Run(system);
```

### PlainPartitionedSource

The `PlainPartitionedSource` is a way to track automatic partition assignment from Kafka.
When a topic-partition is assigned to a consumer, this source will emit tuples with the assigned topic-partition and a corresponding source of `ConsumerRecord`s.
When a topic-partition is revoked, the corresponding source completes. As of version 1.5.39, the source automatically filters out any messages from recently revoked partitions, providing better consistency during rebalancing operations.

```c#
var control = KafkaConsumer.PlainPartitionedSource(consumerSettings, Subscriptions.Topics(topic))
    .GroupBy(3, tuple => tuple.Item1)
    .SelectAsync(8, async tuple =>
    {
        var (topicPartition, source) = tuple;
        Log.Info($"Sub-source for {topicPartition}");
        var sourceMessages = await source
            .Scan(0, (i, message) => i + 1)
            .Select(i => LogReceivedMessages(topicPartition, i))
            .RunWith(Sink.Last<long>(), Materializer);

        Log.Info($"{topicPartition}: Received {sourceMessages} messages in total");
        return sourceMessages;
    })
    .MergeSubstreams()
    .As<Source<long, IControl>>()
    .Scan(0L, (i, subValue) => i + subValue)
    .ToMaterialized(Sink.Last<long>(), Keep.Both)
    .MapMaterializedValue(DrainingControl<long>.Create)
    .Run(Materializer);
```

### CommitWithMetadataSource

The `CommitWithMetadataSource` makes it possible to add additional metadata (in the form of a string)
 when an offset is committed based on the record. This can be useful (for example) to store information about which
 node made the commit, what time the commit was made, the timestamp of the record etc.

 ```c#
string MetadataFromMessage<K, V>(ConsumeResult<K, V> message) => message.Offset.ToString();

KafkaConsumer.CommitWithMetadataSource(settings, Subscriptions.Topics("topic"), MetadataFromMessage)
    .ToMaterialized(Sink.Ignore<CommittableMessage<Null, string>>(), Keep.Both)
    .Run(Materializer);
```

### SourceWithOffsetContext

This source emits <see cref="ConsumeResult{TKey,TValue}"/> together with the offset position as flow context, thus makes it possible to commit offset positions to Kafka.
This is useful when "at-least once delivery" is desired, as each message will likely be delivered one time but in failure cases could be duplicated.
It is intended to be used with `KafkaProducer.FlowWithContext` and/or `Committer.SinkWithOffsetContext`. As of version 1.5.39, this source includes improved partition handling with automatic filtering of messages from revoked partitions.

```c#
var control = KafkaConsumer.SourceWithOffsetContext(consumerSettings, Subscriptions.Topics("topic1"))
    // Having committable offset as a context now, and passing plain record to the downstream
    .Select(record =>
    {
        IEnvelope<string, string, NotUsed> output = ProducerMessage.Single(new ProducerRecord<string, string>("topic2", record.Key, record.Value));
        return output;
    })
    // Producing message with maintaining the context
    .Via(KafkaProducer.FlowWithContext<string, string, ICommittableOffset>(producerSettings))
    .AsSource()
    // Using Committer.SinkWithOffsetContext to commit messages using offset stored in flow context
    .ToMaterialized(Committer.SinkWithOffsetContext<IResults<string, string, ICommittableOffset>>(committerSettings), Keep.Both)
    .MapMaterializedValue(tuple => DrainingControl<NotUsed>.Create(tuple.Item1, tuple.Item2.ContinueWith(t => NotUsed.Instance)))
    .Run(Materializer);
```

### CommittableExternalSource

Like `PlainExternalSource`, allows to use external `KafkaConsumerActor` (see documentation above).

### CommittablePartitionedSource

Same as `PlainPartitionedSource` but with committable offset support.

### AtMostOnceSource

Convenience for "at-most once delivery" semantics. The offset of each message is committed to Kafka before being emitted downstream.

### CommitWithMetadataPartitionedSource

The same as `PlainPartitionedSource` but with offset commit with metadata support.

### PlainPartitionedManualOffsetSource

The `PlainPartitionedManualOffsetSource` is similar to `PlainPartitionedSource`
but allows the use of an offset store outside of Kafka, while retaining the automatic partition assignment.
When a topic-partition is assigned to a consumer, the `getOffsetsOnAssign`
function will be called to retrieve the offset, followed by a seek to the correct spot in the partition.

As of version 1.5.39, this source uses `IncrementalAssign` internally to prevent offset resets during partition reassignment, making it more reliable for scenarios where you're managing offsets externally - in other words: the stage now remembers any previous assignments you've made.

The `onRevoke` function gives the consumer a chance to store any uncommitted offsets, and do any other cleanup
that is required. The source also automatically filters out any messages from recently revoked partitions to maintain consistency during rebalancing.

```c#
var source = KafkaConsumer.PlainPartitionedManualOffsetSource(consumerSettings, Subscriptions.Topics(topic),
    assignedPartitions =>
    {
        // Handle assigned partitions - retrieve offsets from your external store
        return Task.FromResult(assignedPartitions.ToDictionary(
            p => p, 
            _ => new TopicPartitionOffset(_.Topic, _.Partition, Offset.Stored)));
    },
    revokedPartitions =>
    {
        // Handle partitions that are revoked - store current offsets externally
        return Task.CompletedTask;
    })
    // Pass message values down to the stream
    .Select(m => m.Value);
```

### Transactional Producers and Consumers

Are not implemented yet. Waiting for issue https://github.com/akkadotnet/Akka.Streams.Kafka/issues/85 to be resolved.

### Partition Events handling

Sometimes you may need to add custom handling for partition events, like assigning partition to consumer. To do that, you will need:

1. To write custom implementation of `IPartitionEventHandler` interface:
```c#
class CustomEventsHandler : IPartitionEventHandler
{
	/// <inheritdoc />
	public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
	{
		// Your code here
	}

	/// <inheritdoc />
	public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
	{
		// Your code here
	}

	/// <inheritdoc />
	public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer)
	{
		// Your code here
	}
}
```

Here `IRestrictedConsumer` is an object providing access to some limited API of internal consumer kafka client.

2. Use `WithPartitionEventsHandler` of `Topic` / `TopicPartition` subscriptions, like this:
```c#
var customHandler = new CustomEventsHandler();
KafkaConsumer.PlainSource(settings, Subscriptions.Topics(yourTopic).WithPartitionEventsHandler(customHandler));
```

> Note: Your handler callbacks will be invoked in the same thread where kafka consumer is handling all events and getting messages, so be careful when using it.

## Error handling
Akka.Streams.Kafka stages utilizes stream supervision deciders to dictate what happens when a failure or 
exception is thrown from inside the stream stage. These deciders are basically delegate functions that
returns an `Akka.Streams.Supervision.Directive` enumeration to tell the stage how to behave when a
specific exception occured during the stream lifetime.

You can read more about stream supervision strategies in the [Akka documentation](https://getakka.net/articles/streams/error-handling.html#supervision-strategies)

> __NOTE:__
> 
> A decider applied to a stream using 
> `.WithAttributes(ActorAttributes.CreateSupervisionStrategy(decider))` will be used for the whole 
> stream, any exception that happened in any of the stream stages will use the same decider 
> to determine their fault behavior.

### Producer error handling
The Akka.Streams.Kafka producers are using a default convenience error handling class called 
`Akka.Streams.Kafka.Supervision.DefaultProducerDecider`. This supervision decider uses these strategies
by default:
- Any `ProduceException` with its `IsFatal` flag set will use a hard wired `Directive.Stop`.
- Any `ProduceException` that is classified as a serialization error will use a `Directive.Stop`. This behavior can be overriden.
- Any other `ProduceException` will use a `Directive.Stop`. This behavior can be overriden.
- Any `KafkaRetriableException` will use a hard wired `Directive.Resume`. This behavior assumes that this exception is a transient exception.
- Any `KafkaException` will use a `Directive.Stop`. This behavior can be overriden.
- Any `Exception` will use a `Directive.Stop`. This behavior can be overriden.

To create a custom decider, you will need to extend the `DefaultProducerDecider`:
```C#
private class CustomProducerDecider<K, V> : DefaultProducerDecider<K, V>
{
    protected override Directive OnSerializationError(ProduceException<K, V> exception)
    {
        // custom logic can go here
        return Directive.Resume;
    }
    
    protected override Directive OnProduceException(ProduceException<TKey, TValue> exception)
    {
        // custom logic can go here
        return Directive.Resume;
    }
    
    protected virtual Directive OnKafkaException(KafkaException exception)
    {
        // custom logic can go here
        return Directive.Stop;
    }
    
    protected virtual Directive OnException(Exception exception)
    {
        // custom logic can go here
        return Directive.Stop;
    }
}
```

You then register this new decider on to the stream using a stream attribute:
```c#
var decider = new CustomProducerDecider<Null, int>();
var topicPartition = new TopicPartition("my-topic", 0);
await Source.From(new []{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    .Select(elem => new ProducerRecord<Null, string>(topicPartition, elem))
    .RunWith(
        KafkaProducer.PlainSink(ProducerSettings)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(decider.Decide)), 
        System.Materializer());
```
In this case, the decider is applied only to the `PlainSink` stream, it is not propagated to the stream
using it.

### Consumer error handling
The Akka.Streams.Kafka consumers are using a default convenience error handling class called
`Akka.Streams.Kafka.Supervision.DefaultConsumerDecider`. This supervision decider uses these strategies
by default:
- Any `ConsumeException` with its `IsFatal` flag set will use a hard wired `Directive.Stop`. 
  
  As of the writing of this document, there are no fatal `ConsumeException`. Fatal exceptions are only
  thrown by producers that requires idempotence guarantee or requires transactions.
- Any `ConsumeException` that returns an error code of `ErrorCode.UnknownTopicOrPart` inside a kafka 
  stream with `auto.create.topics.enable` enabled will use a `Directive.Resume`. 
  
  This behavior assumes that the consumer client started before the producer was running and the broker 
  did not have the topic or partition created yet.
- Any `ConsumeException` that is classified as a deserialization error will use a `Directive.Stop`. This behavior can be overriden.
- Any other `ConsumeException` will use a `Directive.Resume`. This behavior can be overriden.
- Any `KafkaRetriableException` will use a hard wired `Directive.Resume`. This behavior assumes that this exception is a transient exception.
- Any `KafkaException` will use a `Directive.Resume`. This behavior can be overriden.
- Any `Exception` will use a `Directive.Stop`. This behavior can be overriden.

A `Directive.Resume` is chosen as default because a fatal and data compromising error very rarely
happened during a `Consumer.Consume`. The most common exceptions that are thrown during a consume are
kafka configuration errors. 

To create a custom decider, you will need to extend the `DefaultConsumerDecider`:
```C#
private class CustomConsumerDecider : DefaultConsumerDecider
{
    protected override Directive OnDeserializationError(ConsumeException exception)
    {
        // custom logic can go here
        return Directive.Resume;
    }
    
    protected override Directive OnConsumeException(ConsumeException exception)
    {
        // custom logic can go here
        return Directive.Resume;
    }
    
    protected virtual Directive OnKafkaException(KafkaException exception)
    {
        // custom logic can go here
        return Directive.Resume;
    }
    
    protected virtual Directive OnException(Exception exception)
    {
        // custom logic can go here
        return Directive.Stop;
    }
}
```

You then register this new decider on to the stream using a stream attribute:
```c#
var decider = new CustomConsumerDecider();
var topicPartition = new TopicPartition("my-topic", 0);
var publisher = KafkaConsumer
    .PlainSource(settings, Subscriptions.Assignment(topicPartition))
    .WithAttributes(ActorAttributes.CreateSupervisionStrategy(decider.Decide))
    .Select(c => c.Value)
    .RunWith(Sink.Publisher<int>(), System.Materializer());
```

### Handling de/serialization exceptions
In the producer side, any serialization errors will be routed to the 
`OnSerializationError(ProduceException<K, V> exception)` callback function. The original message will be 
embedded inside the `ProduceException.DeliveryResult.Message` property if analysis were needed to 
determine the cause of the serialization failure. A key serialization failure will have an error code
of `ErrorCode.Local_KeySerialization`, while a value serialization failure will have an error code
of `ErrorCode.Local_ValueSerialization`.

In the consumer side, any deserialization errors will be routed to the
`OnDeserializationError(ConsumeException exception)` callback function. The consumed message will be
embedded inside the `ConsumeException.ConsumerRecord` property as a `ConsumeResult<byte[], byte[]>` 
instance. You can inspect the raw byte arrays to determine the cause of the failure. A key 
deserialization failure will have an error code of `ErrorCode.Local_KeyDeserialization`, while a value 
deserialization failure will have an error code of `ErrorCode.Local_ValueDeserialization`.

## Local development

There are some helpers to simplify local development

### Tests: File logging

Sometimes it is useful to have all logs written to a file in addition to console. 

There is a built-in file logger, that will be added to default Akka.NET loggers if you will set `AKKA_STREAMS_KAFKA_TEST_FILE_LOGGING` environment variable on your local system to any value.

When set, all logs will be written to `logs` subfolder near to your test assembly, one file per test. Here is how log file name is generated:

```C#
public readonly string LogPath = $"logs\\{DateTime.Now:yyyy-MM-dd_HH-mm-ss}_{Guid.NewGuid():N}.txt";
```
### Tests: Kafka container reuse

By default, tests are configured to be friendly to CI - that is, before starting tests docker Kafka images will be downloaded (if not yet exist) and containers started, and after all tests finish full cleanup will be performed (except the fact that downloaded docker images will not be removed).

While this might be useful when running tests locally, there are situations when you would like to save startup/shutdown tests time by using some pre-existing container, that will be used for all test runs and will not be stopped/started each time.

To achieve that, set `AKKA_STREAMS_KAFKA_TEST_CONTAINER_REUSE` environment variable on your local machine to any value. This will force using existing Kafka container, listening on port `29092` . Use `docker-compose up` console command in the root of project folder to get this container up and running.

