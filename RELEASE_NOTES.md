#### 1.5.67 April 28th 2026 ####

* [Upgraded to Akka.NET v1.5.67](https://github.com/akkadotnet/akka.net/releases/tag/1.5.67) - see [PR #521](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/521)

#### 1.5.59 January 26th 2026 ####

* [Upgraded to Akka.NET v1.5.59](https://github.com/akkadotnet/akka.net/releases/tag/1.5.59)

#### 1.5.55 October 26th 2025 ####

* [Upgraded to Akka.NET v1.5.55](https://github.com/akkadotnet/akka.net/releases/tag/1.5.55) - see [PR #505](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/505)

#### 1.5.46 August 12th 2025 ####

* [Upgraded to Akka.NET v1.5.46](https://github.com/akkadotnet/akka.net/releases/tag/1.5.46)
* [Fix race condition in PlainPartitionedManualOffsetSource when seeking custom offsets](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/498)
* [Fix race condition in SubSourceLogic when consumer is aborted](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/500)

#### 1.5.40.1 March 24th 2025 ####

* Fixed another major N+1 bug when committing offsets to Kafka: [Committing: fix N+1 issues in `CommittableOffsetBatch`](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/487)


#### 1.5.40 March 24th 2025 ####

* [Major bug fix: Committers: fixed n+1 errors with offset committing](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/484)
* [Upgraded to Akka.NET v1.5.40](https://github.com/akkadotnet/akka.net/releases/tag/1.5.40)


#### 1.5.39 March 19th 2025 ####

Akka.Streams.Kafka 1.5.39 represents a major improvement in stability and performance for Kafka stream processing,
particularly for applications using manual partition assignment and rebalancing scenarios. This release includes
critical fixes for partition management and introduces new performance tuning capabilities that give users more control
over their Kafka consumer behavior.

**Key Improvements**

* Improved stability during partition rebalancing operations
* Enhanced manual partition assignment behavior
* Significantly more reliable and improved committing performance
* New performance tuning capabilities for consumer polling
* Better handling of partition revocation scenarios
* Improved memory efficiency through C# record types

**Breaking Changes**

* Manual partition assignment now uses `IncrementalAssign` instead of `Assign` - this prevents offset resets for users
  running `ManualSubscription`s. If you're using manual partition assignment, you'll need to verify your offset
  management logic is compatible with incremental assignment behavior.
* Several internal types have been converted from classes to C# `record`s for better performance and nullability
  support. This change should be transparent for most users as records are fully compatible with standard class usage
  patterns. The only potential impact would be if you're using inheritance on these types (which is not a recommended
  pattern for Akka.Streams.Kafka types).
* We removed some extension methods that should have never been made `public` in the first place.
* We made some changes to `ICommittable` interface and others.

**Major Bug Fixes and Improvements**

* [Fixed critical issue: Exception inside SelectAsync with null cancellation cause](https://github.com/akkadotnet/Akka.Streams.Kafka/issues/426)
* [Resolved: System.ArgumentException during rebalance operations](https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415)
* [Added performance tuning capability through ConsumerSettings.MaxPollRecords](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/453)
* [Improved partition management: filtering messages from revoked partitions](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/452)
* [Enhanced stability: filtering out buffered records from recently revoked partitions](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/450)

**Performance Data**

For the `PlainSource`:

```                                                                                  
                                                                                     
BenchmarkDotNet v0.14.0, Pop!_OS 22.04 LTS                                           
13th Gen Intel Core i7-1360P, 1 CPU, 16 logical and 12 physical cores                
.NET SDK 9.0.100                                                                     
  [Host]  : .NET 9.0.0 (9.0.24.52809), X64 RyuJIT AVX2                               
  LongRun : .NET 9.0.0 (9.0.24.52809), X64 RyuJIT AVX2                               
                                                                                     
Job=LongRun  EvaluateOverhead=False  Concurrent=True                                 
Server=True  InvocationCount=1  IterationCount=10                                    
LaunchCount=3  RunStrategy=Monitoring  UnrollFactor=1                                
WarmupCount=3  Categories=MacroBenchmark,Consumer,Plain                              
                                                                                     
```                                                                                  

| Method              | PollBatchSize |     Mean |    Error |   StdDev |   msg/sec | 
|---------------------|---------------|---------:|---------:|---------:|----------:| 
| ConsumeMessageAsync | 500           | 41.29 μs | 1.960 μs | 2.933 μs | 24,217.30 | 

This is a ~2.5x improvement over what v1.5.38 was able to achieve.

**Dependencies**

* [Upgraded to Akka.NET v1.5.39](https://github.com/akkadotnet/akka.net/releases/tag/1.5.39)

#### 1.5.39-beta2 March 14th 2025 ####

* [Upgraded to Akka.NET v1.5.39](https://github.com/akkadotnet/akka.net/releases/tag/1.5.39)
* [Resolved: Kafka Producer - Exception occured inside SelectAsync - Cancellation cause must not be null](https://github.com/akkadotnet/Akka.Streams.Kafka/issues/426)

#### 1.5.39-beta1 March 13th 2025 ####

*v1.5.39 is a major update for Akka.Streams.Kafka*

* [Resolved: System.ArgumentException: Unexpected records polled potentially thrown during a rebalance](https://github.com/akkadotnet/Akka.Streams.Kafka/issues/415)
* [Expose `ConsumerSettings.MaxPollRecords`](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/453) available so
  users can performance-tune how many records to fetch during polling.
* [Change `Assign` and `AssignWithOffsets` to use
  `IncrementalAssign`](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/455) - prevents `Offset` resets for users
  running `ManualSubscription`s
* [Refactor
  `SubSourceStageLogic`; filter messages from revoked partitions in partitioned stream sources](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/452)
* [Filter out buffered records from recently revoked partitions](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/450)
* [Enable nullability](https://github.com/akkadotnet/Akka.Streams.Kafka/pull/449)