# Akka Streams Kafka

Akka Streams Kafka is an Akka Streams connector for Apache Kafka. This is a port of the Alpakka Kafka project (https://github.com/akka/alpakka-kafka).

Library is based on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) driver, and implements Sources, Sinks and Flows to handle Kafka message streams.
All stages are build with Akka.Streams advantages in mind:
- There is no constant Kafka topics pooling: messages are consumed on demand, and with back-pressure support
- There is no internal buffering: consumed messages are passed to the downstream in realtime, and producer stages publish messages to Kafka as soon as get them from upstream
- Each stage can make use of it's own `IConsumer` or `IProducer` instance, or can share them (can be used for optimization)
- All Kafka failures can be handled with usual stream error handling strategies

## Builds
[![Build status](https://ci.appveyor.com/api/projects/status/0glh2fi8uic17vl4/branch/dev?svg=true)](https://ci.appveyor.com/project/akkadotnet-contrib/akka-streams-kafka/branch/dev)

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

_Note_: Specifying `null` as a key/value serializer uses default serializer for key/value type. Built-in serializers are available in `Confluent.Kafka.Serializers` class.

By default when creating `ProducerSettings` with the ActorSystem parameter it uses the config section `akka.kafka.producer`.

```
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
Its value is passed through the flow and becomes available in the `ProducerMessage.Results`’s `PassThrough`. 
It can for example hold a `Akka.Streams.Kafka.Messages.CommittableOffset` or `Akka.Streams.Kafka.Messages.CommittableOffsetBatch` (from a `KafkaConsumer.CommittableSource`) 
that can be committed after publishing to Kafka:

```csharp
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

```csharp
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

```csharp
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

```csharp
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
Here is how configuration looks like:

```
# Properties for akka.kafka.ConsumerSettings can be
# defined in this section or a configuration section with
# the same layout.
akka.kafka.consumer {
  # Tuning property of scheduled polls.
  # Controls the interval from one scheduled poll to the next.
  poll-interval = 50ms

  # Tuning property of the `KafkaConsumer.poll` parameter.
  # Note that non-zero value means that the thread that
  # is executing the stage will be blocked. See also the `wakup-timeout` setting below.
  poll-timeout = 50ms

  # The stage will delay stopping the internal actor to allow processing of
  # messages already in the stream (required for successful committing).
  # This can be set to 0 for streams using `DrainingControl`.
  stop-timeout = 30s

  # Duration to wait for `KafkaConsumer.close` to finish.
  close-timeout = 20s

  # If offset commit requests are not completed within this timeout
  # the returned Future is completed `CommitTimeoutException`.
  # The `Transactional.source` waits this ammount of time for the producer to mark messages as not
  # being in flight anymore as well as waiting for messages to drain, when rebalance is triggered.
  commit-timeout = 15s

  # If commits take longer than this time a warning is logged
  commit-time-warning = 1s

  # Not relevant for Kafka after version 2.1.0.
  # If set to a finite duration, the consumer will re-send the last committed offsets periodically
  # for all assigned partitions. See https://issues.apache.org/jira/browse/KAFKA-4682.
  commit-refresh-interval = infinite

  # Fully qualified config path which holds the dispatcher configuration
  # to be used by the KafkaConsumerActor. Some blocking may occur.
  use-dispatcher = "akka.kafka.default-dispatcher"

  # Time to wait for pending requests when a partition is closed
  wait-close-partition = 500ms

  # Limits the query to Kafka for a topic's position
  position-timeout = 5s

  # Issue warnings when a call to a partition assignment handler method takes
  # longer than this.
  partition-handler-warning = 5s
}
```

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

```csharp
var consumer = Sys.ActorOf(KafkaConsumerActorMetadata.GetProps(consumerSettings));
```

### CommittableSource

The `KafkaConsumer.CommittableSource` makes it possible to commit offset positions to Kafka.

If you need to store offsets in anything other than Kafka, `PlainSource` should be used instead of this API.

This is useful when “at-least once delivery” is desired, as each message will likely be delivered one time but in failure cases could be duplicated:

```C#
KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics("topic1"))
    .SelectAsync(1, async elem => 
    {
        await elem.CommitableOffset.Commit();
        return Done.Instance;
    })
    .RunWith(Sink.Ignore<Done>(), _materializer);
```
The above example uses separate `SelectAsync` stages for processing and committing. This guarantees that for parallelism higher than 1 we will keep correct ordering of messages sent for commit.

Committing the offset for each message as illustrated above is rather slow. 
It is recommended to batch the commits for better throughput, with the trade-off that more messages may be re-delivered in case of failures.

### PlainPartitionedSource

The `PlainPartitionedSource` is a way to track automatic partition assignment from kafka.
When a topic-partition is assigned to a consumer, this source will emit tuples with the assigned topic-partition and a corresponding source of `ConsumerRecord`s.
When a topic-partition is revoked, the corresponding source completes.

```csharp
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
It is intended to be used with `KafkaProducer.FlowWithContext` and/or `Committer.SinkWithOffsetContext`

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

The `onRevoke` function gives the consumer a chance to store any uncommitted offsets, and do any other cleanup
that is required. 

```c#
var source = KafkaConsumer.PlainPartitionedManualOffsetSource(consumerSettings, Subscriptions.Topics(topic),
    assignedPartitions =>
    {
        // Handle assigned partitions
    },
    revokedPartitions =>
    {
        // Handle partitions that are revoked
    })
    // Pass message values down to the stream
    .Select(m => m.Value);
```

### Transactional Producers and Consumers

Are not implemented yet. Waiting for issue https://github.com/akkadotnet/Akka.Streams.Kafka/issues/85 to be resolved.

## Local development

There are some helpers to simplify local development

### Tests: File logging

Sometimes it is useful to have all logs written to a file in addition to console. 

There is a built-in file logger, that will be added to default Akka.NET loggers if you will set `AKKA_STREAMS_KAFKA_TEST_FILE_LOGGING` environment variable on your local system to any value.

When set, all logs will be written to `logs` subfolder near to your test assembly, one file per test. Here is how log file name is generated:

```C#
public readonly string LogPath = $"logs\\{DateTime.Now:yyyy-MM-dd_HH-mm-ss}_{Guid.NewGuid():N}.txt";
```

### Tests: kafka container reuse

By default, tests are configured to be friendly to CI - that is, before starting tests docker kafka images will be downloaded (if not yet exist) and containers started, and after all tests finish full cleanup will be performed (except the fact that downloaded docker images will not be removed).

While this might be useful when running tests locally, there are situations when you would like to save startup/shutdown tests time by using some pre-existing container, that will be used for all test runs and will not be stopped/started each time.

To achieve that, set `AKKA_STREAMS_KAFKA_TEST_CONTAINER_REUSE` environment variable on your local machine to any value. This will force using existing kafka container, listening on port `29092` . Use `docker-compose up` console command in the root of project folder to get this container up and running.