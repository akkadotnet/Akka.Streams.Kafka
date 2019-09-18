# Akka Streams Kafka

Akka Streams Kafka is an Akka Streams connector for Apache Kafka.

## Builds
[![Build status](https://ci.appveyor.com/api/projects/status/0glh2fi8uic17vl4/branch/dev?svg=true)](https://ci.appveyor.com/project/akkadotnet-contrib/akka-streams-kafka/branch/dev)

## Producer

A producer publishes messages to Kafka topics. The message itself contains information about what topic and partition to publish to so you can publish to different topics with the same producer.

### Settings

When creating a consumer stream you need to pass in `ProducerSettings` that define things like:

- bootstrap servers of the Kafka cluster
- serializers for the keys and values
- tuning parameters

```C#
var producerSettings = new ProducerSettings<Null, string>(system, null, new StringSerializer(Encoding.UTF8))
    .WithBootstrapServers("localhost:9092");
```

In addition to programmatic construction of the ProducerSettings it can also be created from configuration (application.conf). By default when creating ProducerSettings with the ActorSystem parameter it uses the config section akka.kafka.producer.

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

### Producer as a Sink
`KafkaProducer.PlainSink` is the easiest way to publish messages. The sink consumes `MessageAndMeta` elements which contains a topic name to which the record is being sent, an optional partition number, and an optional key and value.

```C#
Source
    .From(Enumerable.Range(1, 100))
    .Select(c => c.ToString())
    .Select(elem => new MessageAndMeta<Null, string> { Topic = "topic1", Message = new Message<Null, string> { Value = elem } })
    .RunWith(KafkaProducer.PlainSink(producerSettings), materializer);
```
The materialized value of the sink is a `Task` which is completed with result when the stream completes or with exception if an error occurs.

### Producer as a Flow
Sometimes there is a need for publishing messages in the middle of the stream processing, not as the last step, and then you can use `KafkaProducer.PlainFlow`

```C#
Source
    .From(Enumerable.Range(1, 100))
    .Select(c => c.ToString())
    .Select(elem => new MessageAndMeta<Null, string> { Topic = "topic1", Message = new Message<Null, string> { Value = elem } })
    .Via(KafkaProducer.PlainFlow(producerSettings))
    .Select(record =>
    {
        Console.WriteLine($"Producer: {record.Topic}/{record.Partition} {record.Offset}: {record.Value}");
        return record;
    })
    .RunWith(Sink.Ignore<DeliveryReport<Null, string>>(), materializer);
```

## Consumer

A consumer is used for subscribing to Kafka topics.

### Settings 
When creating a consumer stream you need to pass in `ConsumerSettings` that define things like:

- bootstrap servers of the Kafka cluster
- group id for the consumer, note that offsets are always committed for a given consumer group
- serializers for the keys and values
- tuning parameters

```C#
var consumerSettings = ConsumerSettings<Null, string>.Create(system, null, new StringDeserializer(Encoding.UTF8))
    .WithBootstrapServers("localhost:9092")
    .WithGroupId("group1");
```

### Plain Consumer
```C#
var subscription = Subscriptions.Assignment(new TopicPartition("akka", 0));

KafkaConsumer.PlainSource(consumerSettings, subscription)
    .RunForeach(result =>
    {
        Console.WriteLine($"Consumer: {result.Topic}/{result.Partition} {result.Offset}: {result.Value}");
    }, materializer);
```

### Committable Consumer
The `KafkaConsumer.CommittableSource` makes it possible to commit offset positions to Kafka.

Compared to auto-commit this gives exact control of when a message is considered consumed.

If you need to store offsets in anything other than Kafka, `PlainSource` should be used instead of this API.

This is useful when “at-least once delivery” is desired, as each message will likely be delivered one time but in failure cases could be duplicated.

```C#
KafkaConsumer.CommitableSource(consumerSettings, Subscriptions.Topics("topic1"))
    .SelectAsync(1, elem =>
    {
        return elem.CommitableOffset.Commit();
    })
    .RunWith(Sink.Ignore<CommittedOffsets>(), _materializer);
```
The above example uses separate mapAsync stages for processing and committing. This guarantees that for parallelism higher than 1 we will keep correct ordering of messages sent for commit.

Committing the offset for each message as illustrated above is rather slow. It is recommended to batch the commits for better throughput, with the trade-off that more messages may be re-delivered in case of failures.

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