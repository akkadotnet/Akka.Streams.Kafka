#### 1.1.4 September 08 2021 ####

* [Upgrade to Akka.NET 1.4.25](https://github.com/akkadotnet/akka.net/releases/tag/1.4.25)
* [Optimize Consumer polling](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/217)
* [Fix topic partition assignment and revocation bug](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/221)
* [Fix failing Kafka seek causing plain partitioned source to fail](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/225)

__Kafka consumer client polling optimization__

These are the before and after benchmark comparison of the consumer stream throughput.
`KafkaClientThroughput` uses the native client as the baseline number for comparison.

__Before:__
``` ini
BenchmarkDotNet=v0.13.1, OS=Windows 10.0.19041.1165 (2004/May2020Update/20H1)
AMD Ryzen 9 3900X, 1 CPU, 24 logical and 12 physical cores
.NET SDK=5.0.201
  [Host]     : .NET Core 3.1.13 (CoreCLR 4.700.21.11102, CoreFX 4.700.21.11602), X64 RyuJIT
  Job-WTNALI : .NET Core 3.1.13 (CoreCLR 4.700.21.11102, CoreFX 4.700.21.11602), X64 RyuJIT

InvocationCount=2000  IterationCount=100  MinWarmupIterationCount=10  
UnrollFactor=1  
```

|                Method |     Mean |     Error |    StdDev |
|---------------------- |---------:|----------:|----------:|
|   PlainSinkThroughput | 892.8 μs | 225.73 μs | 590.69 μs |
| KafkaClientThroughput | 120.4 μs |  17.29 μs |  45.24 μs |


__After:__
``` ini
BenchmarkDotNet=v0.13.1, OS=Windows 10.0.19041.1165 (2004/May2020Update/20H1)
AMD Ryzen 9 3900X, 1 CPU, 24 logical and 12 physical cores
.NET SDK=5.0.201
  [Host]     : .NET Core 3.1.13 (CoreCLR 4.700.21.11102, CoreFX 4.700.21.11602), X64 RyuJIT
  Job-WTNALI : .NET Core 3.1.13 (CoreCLR 4.700.21.11102, CoreFX 4.700.21.11602), X64 RyuJIT

InvocationCount=20000  IterationCount=20  MinWarmupIterationCount=5  
UnrollFactor=1  
```

|                Method |     Mean |   Error |  StdDev |
|---------------------- |---------:|--------:|--------:|
|   PlainSinkThroughput | 134.0 μs | 0.97 μs | 1.12 μs |
| KafkaClientThroughput | 135.2 μs | 0.21 μs | 0.24 μs |


#### 1.1.3 May 24 2021 ####

* [Add ConnectionChecker feature](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/190)
* Upgrade to Confluent.Kafka 1.7.0
* Upgrade to Akka.NET 1.4.20
* [Added support for custom statistics handler](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/204)

#### 1.1.2 March 09 2021 ####

* Upgraded to [Akka.NET v1.4.16](https://github.com/akkadotnet/akka.net/releases/tag/1.4.16)
* Upgrade to Confluent.Kafka 1.6.2
* [Use Decider for choosing producer background error handling](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/185)

#### 1.1.1 December 16 2020 ####

* Upgraded to [Akka.NET v1.4.13](https://github.com/akkadotnet/akka.net/releases/tag/1.4.13)
* Upgrade to Confluent.Kafka 1.5.3
* [Make Akka.Streams.Kafka client compatible with Azure Event Hub Kafka API](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/167)
