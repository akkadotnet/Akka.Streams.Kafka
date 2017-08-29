# Akka Streams Kafka

Akka Streams Kafka is an Akka Streams connector for Apache Kafka.

## Builds
[![Build status](https://ci.appveyor.com/api/projects/status/uveg350ptdydkes9/branch/dev?svg=true)](https://ci.appveyor.com/project/ravengerUA/akka-streams-kafka/branch/dev)

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

### Producer as a Sink
`Producer.PlainSink` is the easiest way to publish messages. The sink consumes `ProducerRecord` elements which contains a topic name to which the record is being sent.

```C#
Source
    .From(Enumerable.Range(500, 601))
    .Select(c => c.ToString())
    .Select(elem => new ProduceRecord<Null, string>("topic1", null, elem))
    .RunWith(Producer.PlainSink(producerSettings), materializer);
```
The materialized value of the sink is a `Task` which is completed with result when the stream completes or with exception if an error occurs.

### Producer as a Flow
Sometimes there is a need for publishing messages in the middle of the stream processing, not as the last step, and then you can use `Producer.CreateFlow`

```C#
Source
    .From(Enumerable.Range(1, 100))
    .Select(c => c.ToString())
    .Select(elem => new ProduceRecord<Null, string>("topic1", null, elem))
    .Via(Producer.CreateFlow(producerSettings))
    .Select(record =>
    {
        Console.WriteLine($"Producer: {record.Topic}/{record.Partition} {record.Offset}: {record.Value}");
        return record;
    })
    .RunWith(Sink.Ignore<Message<Null, string>>(), materializer);
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

Consumer.PlainSource(consumerSettings, subscription)
    .RunForeach(result =>
    {
        Console.WriteLine($"Consumer: {result.Topic}/{result.Partition} {result.Offset}: {result.Value}");
    }, materializer);
```