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
Sometimes there is a need for publishing messages in the middle of the stream processing, not as the last step, and then you can use Producer.flow

```C#
Source
    .From(Enumerable.Range(1, 100))
    .Select(c => c.ToString())
    .Select(elem => new ProduceRecord<Null, string>("topic1", null, elem))
    .Via(Producer.CreateFlow(producerSettings))
    .Select(result =>
    {
        var record = result.Result.Metadata;
        Console.WriteLine($"{record.Topic}/{record.Partition} {result.Result.Offset}: {record.Value}");
        return result;
    })
    .RunWith(Sink.Ignore<Task<Result<Null, string>>>(), materializer);
```